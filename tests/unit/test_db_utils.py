import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    create_table,
    get_database_path,
    insert_dataframe,
    read_table,
)


@pytest.fixture
def temp_db_file():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_file = tmp_file.name
    yield db_file
    Path(db_file).unlink()


@pytest.fixture
def db_connection(temp_db_file):
    """Create a database connection for testing."""
    conn = create_connection(temp_db_file)
    yield conn
    conn.close()


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})


@pytest.fixture
def empty_dataframe():
    """Create an empty DataFrame for testing."""
    return pd.DataFrame({"id": [], "name": []})


@pytest.fixture
def test_table_sql():
    """SQL for creating a test table."""
    return """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    """


def test_create_connection_success(temp_db_file):
    """Test successful database connection creation."""
    conn = create_connection(temp_db_file)
    assert isinstance(conn, sqlite3.Connection)
    conn.close()


def test_create_connection_invalid_path():
    """Test connection creation with invalid path raises error."""
    with pytest.raises(sqlite3.Error, match="Failed to create database connection"):
        create_connection("/invalid/path/to/database.db")


def test_create_table_success(db_connection, test_table_sql):
    """Test successful table creation."""
    create_table(db_connection, test_table_sql)

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
    )
    result = cursor.fetchone()

    assert result is not None
    assert result[0] == "test_table"


def test_create_table_invalid_sql(db_connection):
    """Test table creation with invalid SQL raises error."""
    invalid_sql = "INVALID SQL STATEMENT"

    with pytest.raises(sqlite3.Error, match="Failed to create table"):
        create_table(db_connection, invalid_sql)


def test_insert_dataframe_success(db_connection, sample_dataframe):
    """Test successful DataFrame insertion."""
    insert_dataframe(db_connection, "test_table", sample_dataframe)

    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    results = cursor.fetchall()

    assert len(results) == 3
    assert results[0] == (1, "Alice")
    assert results[1] == (2, "Bob")
    assert results[2] == (3, "Charlie")


def test_insert_dataframe_replace_existing(db_connection):
    """Test DataFrame insertion with replace functionality."""
    original_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    new_df = pd.DataFrame({"id": [3, 4, 5], "name": ["Charlie", "David", "Eve"]})

    insert_dataframe(db_connection, "test_table", original_df)
    insert_dataframe(db_connection, "test_table", new_df)

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM test_table")
    count = cursor.fetchone()[0]

    assert count == 3


def test_insert_dataframe_empty_dataframe(db_connection, empty_dataframe):
    """Test insertion of empty DataFrame."""
    insert_dataframe(db_connection, "empty_table", empty_dataframe)

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM empty_table")
    count = cursor.fetchone()[0]

    assert count == 0


def test_read_table_success(db_connection, sample_dataframe):
    """Test successful table reading."""
    insert_dataframe(db_connection, "test_table", sample_dataframe)
    result_df = read_table(db_connection, "test_table")

    assert len(result_df) == 3
    assert list(result_df.columns) == ["id", "name"]
    assert result_df["id"].tolist() == [1, 2, 3]
    assert result_df["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_read_table_nonexistent_table(db_connection):
    """Test reading nonexistent table raises error."""
    with pytest.raises(Exception, match="Failed to read table nonexistent_table"):
        read_table(db_connection, "nonexistent_table")


def test_read_table_empty_table(db_connection, empty_dataframe):
    """Test reading empty table returns empty DataFrame."""
    insert_dataframe(db_connection, "empty_table", empty_dataframe)
    result_df = read_table(db_connection, "empty_table")

    assert len(result_df) == 0
    assert list(result_df.columns) == ["id", "name"]


@patch("mlb_airflow_data_pipeline.db_utils.Path")
def test_get_database_path(mock_path):
    """Test database path generation."""
    mock_file = mock_path(__file__)
    mock_parent = mock_file.parent
    mock_data_dir = mock_parent / "data"
    mock_data_dir.mkdir.return_value = None
    mock_db_path = mock_data_dir / "mlb_data.db"
    mock_db_path.__str__.return_value = "/path/to/data/mlb_data.db"

    result = get_database_path()

    mock_data_dir.mkdir.assert_called_once_with(exist_ok=True)
    assert result == "/path/to/data/mlb_data.db"
