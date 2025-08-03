import sqlite3
import tempfile
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    create_table,
    insert_dataframe,
    read_table,
)


@pytest.fixture
def temp_db_file() -> Iterator[str]:
    """Create a temporary database file for integration testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp_file:
        db_file = tmp_file.name
    yield db_file
    Path(db_file).unlink()


@pytest.fixture
def db_connection(temp_db_file: str) -> Iterator[sqlite3.Connection]:
    """Create a database connection for integration testing."""
    with create_connection(temp_db_file) as conn:
        yield conn


@pytest.fixture
def player_stats_table_sql() -> str:
    """SQL for creating a player stats table."""
    return """
        CREATE TABLE player_stats (
            player_id INTEGER,
            player_name TEXT,
            team_name TEXT,
            hits INTEGER,
            runs INTEGER,
            avg REAL
        );
    """


@pytest.fixture
def sample_player_stats() -> pd.DataFrame:
    """Create sample player statistics DataFrame."""
    return pd.DataFrame(
        {
            "player_id": [1, 2, 3],
            "player_name": ["Aaron Judge", "Mookie Betts", "Vladimir Guerrero Jr."],
            "team_name": ["Yankees", "Dodgers", "Blue Jays"],
            "hits": [162, 158, 175],
            "runs": [133, 117, 97],
            "avg": [0.311, 0.269, 0.274],
        }
    )


@pytest.fixture
def sample_league_standings() -> pd.DataFrame:
    """Create sample league standings DataFrame."""
    return pd.DataFrame(
        {
            "team_name": ["Yankees", "Dodgers", "Blue Jays"],
            "wins": [99, 100, 92],
            "losses": [63, 62, 70],
            "division": ["AL East", "NL West", "AL East"],
        }
    )


@pytest.fixture
def updated_player_stats() -> pd.DataFrame:
    """Create updated player statistics DataFrame."""
    return pd.DataFrame(
        {
            "player_id": [1, 2, 3, 4],
            "player_name": [
                "Aaron Judge",
                "Mookie Betts",
                "Vladimir Guerrero Jr.",
                "Mike Trout",
            ],
            "team_name": ["Yankees", "Dodgers", "Blue Jays", "Angels"],
            "hits": [162, 158, 175, 140],
            "runs": [133, 117, 97, 95],
            "avg": [0.311, 0.269, 0.274, 0.283],
        }
    )


def test_full_database_workflow(
    db_connection: sqlite3.Connection,
    player_stats_table_sql: str,
    sample_player_stats: pd.DataFrame,
    updated_player_stats: pd.DataFrame,
) -> None:
    """Test the complete workflow of database operations."""
    create_table(db_connection, player_stats_table_sql)

    insert_dataframe(db_connection, "player_stats", sample_player_stats)

    retrieved_df = read_table(db_connection, "player_stats")

    assert len(retrieved_df) == 3
    assert list(retrieved_df.columns) == [
        "player_id",
        "player_name",
        "team_name",
        "hits",
        "runs",
        "avg",
    ]
    assert retrieved_df["player_name"].tolist() == [
        "Aaron Judge",
        "Mookie Betts",
        "Vladimir Guerrero Jr.",
    ]
    assert retrieved_df["hits"].tolist() == [162, 158, 175]

    insert_dataframe(db_connection, "player_stats", updated_player_stats)

    final_df = read_table(db_connection, "player_stats")

    assert len(final_df) == 4
    assert "Mike Trout" in final_df["player_name"].tolist()


def test_multiple_tables_workflow(
    db_connection: sqlite3.Connection,
    sample_league_standings: pd.DataFrame,
    sample_player_stats: pd.DataFrame,
) -> None:
    """Test working with multiple tables in the same database."""
    insert_dataframe(db_connection, "league_standings", sample_league_standings)
    insert_dataframe(db_connection, "player_stats", sample_player_stats)

    standings_result = read_table(db_connection, "league_standings")
    player_result = read_table(db_connection, "player_stats")

    assert len(standings_result) == 3
    assert len(player_result) == 3
    assert "wins" in standings_result.columns
    assert "hits" in player_result.columns


def test_error_handling_workflow(db_connection: sqlite3.Connection) -> None:
    """Test error handling in integrated database workflow."""
    with pytest.raises(Exception, match="Failed to read table"):
        read_table(db_connection, "nonexistent_table")

    with pytest.raises(sqlite3.Error, match="Failed to create table"):
        create_table(db_connection, "INVALID SQL")


def test_empty_dataframe_integration(db_connection: sqlite3.Connection) -> None:
    """Test integration with empty DataFrames."""
    empty_df = pd.DataFrame({"id": [], "name": []})

    insert_dataframe(db_connection, "empty_test", empty_df)
    result_df = read_table(db_connection, "empty_test")

    assert len(result_df) == 0
    assert list(result_df.columns) == ["id", "name"]


def test_large_dataset_integration(db_connection: sqlite3.Connection) -> None:
    """Test integration with larger datasets."""
    large_df = pd.DataFrame(
        {
            "id": range(1000),
            "value": [f"item_{i}" for i in range(1000)],
            "score": [i * 0.1 for i in range(1000)],
        }
    )

    insert_dataframe(db_connection, "large_table", large_df)
    result_df = read_table(db_connection, "large_table")

    assert len(result_df) == 1000
    assert result_df["id"].max() == 999
    assert result_df["value"].iloc[0] == "item_0"
    assert result_df["score"].iloc[999] == 99.9
