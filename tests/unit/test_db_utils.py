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


class TestCreateConnection:
    def test_create_connection_success(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

        Path(db_file).unlink()

    def test_create_connection_invalid_path(self):
        with pytest.raises(sqlite3.Error, match="Failed to create database connection"):
            create_connection("/invalid/path/to/database.db")


class TestCreateTable:
    def test_create_table_success(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)
        create_table_sql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        """

        create_table(conn, create_table_sql)

        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        )
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "test_table"

        conn.close()
        Path(db_file).unlink()

    def test_create_table_invalid_sql(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)
        invalid_sql = "INVALID SQL STATEMENT"

        with pytest.raises(sqlite3.Error, match="Failed to create table"):
            create_table(conn, invalid_sql)

        conn.close()
        Path(db_file).unlink()


class TestInsertDataframe:
    def test_insert_dataframe_success(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        insert_dataframe(conn, "test_table", test_df)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM test_table")
        results = cursor.fetchall()

        assert len(results) == 3
        assert results[0] == (1, "Alice")
        assert results[1] == (2, "Bob")
        assert results[2] == (3, "Charlie")

        conn.close()
        Path(db_file).unlink()

    def test_insert_dataframe_replace_existing(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        original_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        new_df = pd.DataFrame({"id": [3, 4, 5], "name": ["Charlie", "David", "Eve"]})

        insert_dataframe(conn, "test_table", original_df)
        insert_dataframe(conn, "test_table", new_df)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]

        assert count == 3

        conn.close()
        Path(db_file).unlink()

    def test_insert_dataframe_empty_dataframe(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        empty_df = pd.DataFrame({"id": [], "name": []})

        insert_dataframe(conn, "empty_table", empty_df)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM empty_table")
        count = cursor.fetchone()[0]

        assert count == 0

        conn.close()
        Path(db_file).unlink()


class TestReadTable:
    def test_read_table_success(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        insert_dataframe(conn, "test_table", test_df)

        result_df = read_table(conn, "test_table")

        assert len(result_df) == 3
        assert list(result_df.columns) == ["id", "name"]
        assert result_df["id"].tolist() == [1, 2, 3]
        assert result_df["name"].tolist() == ["Alice", "Bob", "Charlie"]

        conn.close()
        Path(db_file).unlink()

    def test_read_table_nonexistent_table(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        with pytest.raises(Exception, match="Failed to read table nonexistent_table"):
            read_table(conn, "nonexistent_table")

        conn.close()
        Path(db_file).unlink()

    def test_read_table_empty_table(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        conn = create_connection(db_file)

        empty_df = pd.DataFrame({"id": [], "name": []})
        insert_dataframe(conn, "empty_table", empty_df)

        result_df = read_table(conn, "empty_table")

        assert len(result_df) == 0
        assert list(result_df.columns) == ["id", "name"]

        conn.close()
        Path(db_file).unlink()


class TestGetDatabasePath:
    @patch("mlb_airflow_data_pipeline.db_utils.Path")
    def test_get_database_path(self, mock_path):
        mock_file = mock_path(__file__)
        mock_parent = mock_file.parent
        mock_data_dir = mock_parent / "data"
        mock_data_dir.mkdir.return_value = None
        mock_db_path = mock_data_dir / "mlb_data.db"
        mock_db_path.__str__.return_value = "/path/to/data/mlb_data.db"

        result = get_database_path()

        mock_data_dir.mkdir.assert_called_once_with(exist_ok=True)
        assert result == "/path/to/data/mlb_data.db"
