import sqlite3
from pathlib import Path
from unittest.mock import Mock, call

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction_script
from mlb_airflow_data_pipeline.db_utils import create_connection
from mlb_airflow_data_pipeline.statsapi_extraction_script import DataExtractor
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    SEASON_YEAR,
    expected_output_columns,
)


def _player_stats_response(player_id: int, type: str) -> str:
    if player_id == 202:
        raise TypeError("No player stats")
    columns = set(expected_output_columns()) - {"playername", "team_id", "date"}
    return "\n".join(
        f"{column}: {player_id if column == 'hits' else 0}"
        for column in sorted(columns)
    )


@pytest.fixture
def league_api(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock, Mock, Mock]:
    """StatsAPI responses for three teams, including one inactive player."""
    standings_data = Mock(
        return_value={
            200: {"teams": [{"team_id": 147, "name": "Yankees"}]},
            201: {"teams": [{"team_id": 111, "name": "Red Sox"}]},
            202: {"teams": [{"team_id": 133, "name": "Athletics"}]},
        }
    )
    rosters = {
        147: [
            {"person": {"id": 101, "fullName": "Player One"}},
            {"person": {"id": 202, "fullName": "Player Two"}},
        ],
        111: [{"person": {"id": 303, "fullName": "Player Three"}}],
        133: [{"person": {"id": 404, "fullName": "Player Four"}}],
    }
    get = Mock(
        side_effect=lambda endpoint, params: {"roster": rosters[params["teamId"]]}
    )

    player_stats = Mock(side_effect=_player_stats_response)
    lookup_player = Mock(side_effect=AssertionError("Unexpected name lookup"))
    monkeypatch.setattr(statsapi, "standings_data", standings_data)
    monkeypatch.setattr(statsapi, "get", get)
    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    monkeypatch.setattr(statsapi, "lookup_player", lookup_player)

    return standings_data, get, player_stats, lookup_player


def test_roster_player_stats_reach_database(
    tmp_path: Path, league_api: tuple[Mock, Mock, Mock, Mock]
) -> None:
    database_path = tmp_path / "mlb_data.db"
    execution_date = "2026-09-26"
    standings_data, get, player_stats, lookup_player = league_api
    extraction_script.run_extraction(
        str(database_path), "american_league", execution_date
    )

    with sqlite3.connect(database_path) as conn:
        rows = conn.execute(
            """SELECT playername, team_id, hits, date, league_name
               FROM player_stats ORDER BY team_id"""
        ).fetchall()
        standings_count = conn.execute(
            "SELECT COUNT(*) FROM league_standings"
        ).fetchone()[0]

    assert rows == [
        ("Player Three", 111, "303", execution_date, "american_league"),
        ("Player Four", 133, "404", execution_date, "american_league"),
        ("Player One", 147, "101", execution_date, "american_league"),
    ]
    assert standings_count == 3
    standings_data.assert_called_once_with(103, season=SEASON_YEAR)
    assert get.call_args_list == [
        call(
            "team_roster",
            {
                "teamId": team_id,
                "season": SEASON_YEAR,
                "rosterType": "active",
            },
        )
        for team_id in (147, 111, 133)
    ]
    assert player_stats.call_args_list == [
        call(player_id, type="season") for player_id in (101, 202, 303, 404)
    ]
    lookup_player.assert_not_called()


def test_failed_team_preserves_previous_snapshot(
    tmp_path: Path, league_api: tuple[Mock, Mock, Mock, Mock]
) -> None:
    database_path = str(tmp_path / "runs.db")
    extraction_script.run_extraction(database_path, "american_league", "2026-09-26")
    with create_connection(database_path) as conn:
        before = list(conn.iterdump())

    def fail_red_sox_player(player_id: int, type: str) -> str:
        if player_id == 303:
            raise ConnectionError("Stats unavailable")
        return _player_stats_response(player_id, type)

    standings_data, _, player_stats, _ = league_api
    standings_data.return_value[200]["teams"][0]["name"] = "Changed"
    player_stats.side_effect = fail_red_sox_player
    with pytest.raises(RuntimeError, match="Red Sox"):
        extraction_script.run_extraction(database_path, "american_league", "2026-09-26")

    with create_connection(database_path) as conn:
        assert list(conn.iterdump()) == before


def test_data_extractor_creates_valid_dataframes() -> None:
    """Test that DataExtractor creates DataFrames compatible with database storage."""
    data_extractor = DataExtractor(league_name="american_league")

    data_extractor.set_league_division_standings()

    standings_df = data_extractor.league_standings
    assert isinstance(standings_df, pd.DataFrame)
    assert len(standings_df) > 0
    assert "team_id" in standings_df.columns
    assert "name" in standings_df.columns
