import sqlite3
import tempfile
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    read_player_stats,
)


@pytest.fixture
def temp_db_file() -> Iterator[str]:
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_file = tmp_file.name
    yield db_file
    Path(db_file).unlink()


@pytest.fixture
def db_connection(temp_db_file: str) -> Iterator[sqlite3.Connection]:
    """Create a database connection for testing."""
    with create_connection(temp_db_file) as conn:
        yield conn


def test_create_connection_success(temp_db_file: str) -> None:
    """Test successful database connection creation."""
    with create_connection(temp_db_file) as conn:
        assert isinstance(conn, sqlite3.Connection)


def test_create_connection_invalid_path() -> None:
    """Test connection creation with invalid path raises error."""
    with pytest.raises(sqlite3.Error, match="Failed to create database connection"):
        with create_connection("/invalid/path/to/database.db"):
            pass  # This code should not be reached


def test_read_player_stats_scopes_to_one_league_run(
    db_connection: sqlite3.Connection,
) -> None:
    player_stats = pd.DataFrame(
        {
            "playername": ["AL player", "NL player", "Earlier AL player"],
            "league_name": [
                "american_league",
                "national_league",
                "american_league",
            ],
            "date": ["2026-01-01", "2026-01-01", "2025-12-31"],
        }
    )
    player_stats.to_sql("player_stats", db_connection, index=False)

    result = read_player_stats(db_connection, "american_league", "2026-01-01")

    assert result["playername"].tolist() == ["AL player"]
