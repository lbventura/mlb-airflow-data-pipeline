import sqlite3
import tempfile
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction_script
from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    insert_dataframe,
    read_table,
)
from mlb_airflow_data_pipeline.statsapi_extraction_script import DataExtractor
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    SEASON_YEAR,
    expected_output_columns,
)


@pytest.fixture
def temp_db_file() -> Iterator[str]:
    """Create a temporary database file for extraction testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_file = tmp_file.name
    yield db_file
    Path(db_file).unlink()


@pytest.fixture
def db_connection(temp_db_file: str) -> Iterator[sqlite3.Connection]:
    """Create a database connection for extraction testing."""
    with create_connection(temp_db_file) as conn:
        yield conn


@pytest.fixture
def sample_standings_data() -> pd.DataFrame:
    """Create sample league standings data for testing."""
    return pd.DataFrame(
        {
            "team_id": [147, 121, 110],
            "name": ["Yankees", "Mets", "Orioles"],
            "w": [99, 101, 83],
            "l": [63, 61, 79],
        }
    )


@pytest.fixture
def sample_player_data() -> pd.DataFrame:
    """Create sample player statistics data for testing."""
    return pd.DataFrame(
        {
            "playername": ["Aaron Judge", "Pete Alonso", "Ryan Mountcastle"],
            "team_id": [147, 121, 110],
            "hits": [162, 146, 138],
            "runs": [133, 113, 70],
            "homeRuns": [62, 40, 22],
        }
    )


def test_extraction_script_database_integration(
    temp_db_file: str,
    sample_standings_data: pd.DataFrame,
    sample_player_data: pd.DataFrame,
) -> None:
    """Test that the extraction script works with database storage."""
    with patch(
        "mlb_airflow_data_pipeline.statsapi_extraction_script.get_database_path"
    ) as mock_db_path:
        mock_db_path.return_value = temp_db_file

        with create_connection(temp_db_file) as conn:
            insert_dataframe(conn, "league_standings", sample_standings_data)
            insert_dataframe(conn, "player_stats", sample_player_data)

            retrieved_standings = read_table(conn, "league_standings")
            retrieved_player_stats = read_table(conn, "player_stats")

            assert len(retrieved_standings) == 3
            assert len(retrieved_player_stats) == 3
            assert "team_id" in retrieved_standings.columns
            assert "playername" in retrieved_player_stats.columns
            assert retrieved_standings["name"].tolist() == [
                "Yankees",
                "Mets",
                "Orioles",
            ]
            assert retrieved_player_stats["playername"].tolist() == [
                "Aaron Judge",
                "Pete Alonso",
                "Ryan Mountcastle",
            ]


def test_roster_player_stats_reach_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "mlb_data.db"
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

    def player_stats_response(player_id: int, type: str) -> str:
        if player_id == 202:
            raise TypeError("No player stats")
        columns = set(expected_output_columns()) - {"playername", "team_id", "date"}
        return "\n".join(
            f"{column}: {player_id if column == 'hits' else 0}"
            for column in sorted(columns)
        )

    player_stats = Mock(side_effect=player_stats_response)
    lookup_player = Mock(side_effect=AssertionError("Unexpected name lookup"))
    monkeypatch.setattr(extraction_script, "LEAGUE_NAME", "american_league")
    monkeypatch.setattr(
        extraction_script, "get_database_path", lambda: str(database_path)
    )
    monkeypatch.setattr(statsapi, "standings_data", standings_data)
    monkeypatch.setattr(statsapi, "get", get)
    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    monkeypatch.setattr(statsapi, "lookup_player", lookup_player)

    extraction_script.run_extraction()

    with sqlite3.connect(database_path) as conn:
        rows = conn.execute(
            """SELECT playername, team_id, hits, date, league_name
               FROM player_stats ORDER BY team_id"""
        ).fetchall()
        standings_count = conn.execute(
            "SELECT COUNT(*) FROM league_standings"
        ).fetchone()[0]

    execution_date = extraction_script.DATE_TIME_EXECUTION
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


def test_data_extractor_creates_valid_dataframes() -> None:
    """Test that DataExtractor creates DataFrames compatible with database storage."""
    data_extractor = DataExtractor(league_name="american_league")

    data_extractor.set_league_division_standings()

    standings_df = data_extractor.league_standings
    assert isinstance(standings_df, pd.DataFrame)
    assert len(standings_df) > 0
    assert "team_id" in standings_df.columns
    assert "name" in standings_df.columns


def test_data_extractor_database_compatibility(temp_db_file: str) -> None:
    """Test that DataExtractor output is compatible with database storage."""
    data_extractor = DataExtractor(league_name="american_league")
    data_extractor.set_league_division_standings()

    with create_connection(temp_db_file) as conn:
        insert_dataframe(conn, "test_standings", data_extractor.league_standings)

        retrieved_df = read_table(conn, "test_standings")
        assert len(retrieved_df) == len(data_extractor.league_standings)
        assert list(retrieved_df.columns) == list(
            data_extractor.league_standings.columns
        )


def test_database_operations_with_mock_data(
    db_connection: sqlite3.Connection,
    sample_standings_data: pd.DataFrame,
    sample_player_data: pd.DataFrame,
) -> None:
    """Test database operations with mock extraction data."""
    insert_dataframe(db_connection, "league_standings", sample_standings_data)
    insert_dataframe(db_connection, "player_stats", sample_player_data)

    standings_result = read_table(db_connection, "league_standings")
    player_result = read_table(db_connection, "player_stats")

    assert len(standings_result) == 3
    assert len(player_result) == 3
    assert standings_result["team_id"].tolist() == [147, 121, 110]
    assert player_result["hits"].tolist() == [162, 146, 138]


def test_extraction_data_types_compatibility(db_connection: sqlite3.Connection) -> None:
    """Test that extraction script data types are compatible with database storage."""
    test_data = pd.DataFrame(
        {
            "team_id": [147, 121],  # integers
            "name": ["Yankees", "Mets"],  # strings
            "avg": [0.285, 0.301],  # floats
            "active": [True, False],  # booleans
        }
    )

    insert_dataframe(db_connection, "type_test", test_data)
    result = read_table(db_connection, "type_test")

    assert len(result) == 2
    assert result["team_id"].dtype in ["int64", "int32"]
    assert result.name.dtype.name == "str"
    assert result["avg"].dtype in ["float64", "float32"]


def test_large_extraction_dataset_simulation(db_connection: sqlite3.Connection) -> None:
    """Test database operations with simulated large extraction dataset."""
    large_player_stats = pd.DataFrame(
        {
            "playername": [f"Player_{i}" for i in range(500)],
            "team_id": [i % 30 + 100 for i in range(500)],  # 30 teams
            "hits": [150 + (i % 50) for i in range(500)],
            "runs": [80 + (i % 60) for i in range(500)],
            "avg": [0.250 + (i % 100) * 0.001 for i in range(500)],
        }
    )

    insert_dataframe(db_connection, "large_player_stats", large_player_stats)
    result = read_table(db_connection, "large_player_stats")

    assert len(result) == 500
    assert result["playername"].iloc[0] == "Player_0"
    assert result["team_id"].max() == 129
    assert result["hits"].min() >= 150
