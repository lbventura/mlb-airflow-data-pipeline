import sqlite3
from pathlib import Path
from unittest.mock import Mock, call

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction_script
from mlb_airflow_data_pipeline.db_utils import create_connection, save_extraction_run
from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    DataExtractor,
    TeamStats,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    PITCHING_STATS,
    SEASON_YEAR,
)
from mlb_airflow_data_pipeline.statsapi_treatment_script import treat_player_stats


def _player_stats_response(player_id: int) -> dict:
    if player_id == 202:
        return {"people": [{"id": player_id}]}
    sections = [
        {
            "group": "hitting",
            "stats": {
                **dict.fromkeys(BATTING_STATS, 0),
                "hits": player_id,
                "atBats": 1000,
                "plateAppearances": 1100,
            },
        }
    ]
    if player_id == 101:
        sections.extend(
            [
                {
                    "group": "pitching",
                    "stats": {
                        **dict.fromkeys(PITCHING_STATS, 0),
                        "gamesPlayed": 5,
                        "inningsPitched": "20.0",
                        "outs": 60,
                    },
                },
                {
                    "group": "fielding",
                    "stats": {"position": {"abbreviation": "C"}, "gamesPlayed": 0},
                },
            ]
        )
    if player_id == 505:
        sections = [
            {
                "group": "pitching",
                "stats": {
                    **dict.fromkeys(PITCHING_STATS, 0),
                    "gamesPlayed": 5,
                    "inningsPitched": "30.0",
                    "outs": 90,
                },
            }
        ]
    return {
        "people": [
            {
                "id": player_id,
                "stats": [
                    {
                        "group": {"displayName": section["group"]},
                        "splits": [{"stat": section["stats"]}],
                    }
                    for section in sections
                ],
            }
        ]
    }


@pytest.mark.parametrize("stats_type", ["season", "career"])
def test_collision_survives_database_write_with_one_stats_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stats_type: str
) -> None:
    group_stats = {
        "hitting": {**dict.fromkeys(BATTING_STATS, 0), "hits": 0, "stolenBases": 4},
        "pitching": {**dict.fromkeys(PITCHING_STATS, 0), "hits": 43},
        "fielding": {
            "position": {"abbreviation": "C"},
            "gamesPlayed": 120,
            "stolenBases": 50,
        },
    }
    person = {
        "id": 101,
        "stats": [
            {
                "type": {"displayName": stats_type},
                "group": {"displayName": group},
                "splits": [{"stat": stats}],
            }
            for group, stats in group_stats.items()
        ],
    }
    get = Mock(return_value={"people": [person]})
    monkeypatch.setattr(statsapi, "get", get)
    monkeypatch.setattr(extraction_script, "_get_stats_type", lambda: stats_type)

    rows, active, inactive = TeamStats({101: "Player One"}).get_team_stats()
    assert active == {101: "Player One"}
    assert inactive == {}
    rows["playername"] = "Player One"
    rows["team_id"] = 147
    database_path = tmp_path / "mlb_data.db"
    with sqlite3.connect(database_path) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-10-01",
            pd.DataFrame({"team_id": [147]}),
            rows,
        )
        saved = conn.execute(
            """SELECT hitting_hits, pitching_hits, hitting_stolenBases,
                      fielding_C_stolenBases FROM player_stats"""
        ).fetchone()

    assert saved == ("0", "43", "4", "50")
    season_parameter = f",season={SEASON_YEAR}" if stats_type == "season" else ""
    get.assert_called_once_with(
        "person",
        {
            "personId": 101,
            "hydrate": f"stats(group=[hitting,pitching,fielding],type={stats_type}{season_parameter},sportId=1),currentTeam",
        },
    )


@pytest.fixture
def league_api(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock, Mock, Mock]:
    """Three teams, with interleaved hitter/pitcher groups and an inactive player."""
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
            {"person": {"id": 505, "fullName": "Player Five"}},
            {"person": {"id": 606, "fullName": "Player Six"}},
        ],
        111: [{"person": {"id": 303, "fullName": "Player Three"}}],
        133: [{"person": {"id": 404, "fullName": "Player Four"}}],
    }
    player_stats = Mock(side_effect=_player_stats_response)
    get = Mock(
        side_effect=lambda endpoint, params: (
            {"roster": rosters[params["teamId"]]}
            if endpoint == "team_roster"
            else player_stats(params["personId"])
        )
    )
    lookup_player = Mock(side_effect=AssertionError("Unexpected name lookup"))
    monkeypatch.setattr(statsapi, "standings_data", standings_data)
    monkeypatch.setattr(statsapi, "get", get)
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
            """SELECT player_id, playername, team_id, hitting_hits, pitching_outs,
                      date, league_name
               FROM player_stats ORDER BY team_id, player_id"""
        ).fetchall()
        standings_count = conn.execute(
            "SELECT COUNT(*) FROM league_standings"
        ).fetchone()[0]

    assert rows == [
        (303, "Player Three", 111, "303", None, execution_date, "american_league"),
        (404, "Player Four", 133, "404", None, execution_date, "american_league"),
        (101, "Player One", 147, "101", "60", execution_date, "american_league"),
        (505, "Player Five", 147, None, "90", execution_date, "american_league"),
        (606, "Player Six", 147, "606", None, execution_date, "american_league"),
    ]
    assert standings_count == 3
    standings_data.assert_called_once_with(103, season=SEASON_YEAR)
    assert get.call_args_list[:3] == [
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
        call(player_id) for player_id in (101, 202, 505, 606, 303, 404)
    ]
    lookup_player.assert_not_called()

    treat_player_stats(str(database_path), "american_league", execution_date)
    outputs = {
        role: read_output(
            database_path, role, "american_league", execution_date
        ).set_index("playername")
        for role in ("batter", "pitcher", "defender")
    }
    assert outputs["batter"]["hits"].to_dict() == {
        "Player One": 101,
        "Player Three": 303,
        "Player Four": 404,
        "Player Six": 606,
    }
    assert outputs["pitcher"]["outs"].to_dict() == {
        "Player One": 60,
        "Player Five": 90,
    }
    assert outputs["defender"]["position"].to_dict() == {"Player One": "C"}


@pytest.mark.parametrize("error_type", [ConnectionError, TypeError])
@pytest.mark.parametrize("failed_player", [101, 303])
def test_failed_team_preserves_previous_snapshot(
    tmp_path: Path,
    league_api: tuple[Mock, Mock, Mock, Mock],
    error_type: type[Exception],
    failed_player: int,
) -> None:
    database_path = str(tmp_path / "runs.db")
    extraction_script.run_extraction(database_path, "american_league", "2026-09-26")
    with create_connection(database_path) as conn:
        before = list(conn.iterdump())

    original_error = error_type("Stats unavailable")

    def fail_player(player_id: int) -> dict:
        if player_id == failed_player:
            raise original_error
        return _player_stats_response(player_id)

    standings_data, _, player_stats, _ = league_api
    standings_data.return_value[200]["teams"][0]["name"] = "Changed"
    player_stats.side_effect = fail_player
    with pytest.raises(error_type, match="Stats unavailable") as error:
        extraction_script.run_extraction(database_path, "american_league", "2026-09-26")
    assert error.value is original_error

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


def read_output(
    database_path: Path, category: str, league: str, date: str
) -> pd.DataFrame:
    from mlb_airflow_data_pipeline.db_utils import read_treated_stats

    with sqlite3.connect(database_path) as conn:
        return read_treated_stats(conn, category, league, date)
