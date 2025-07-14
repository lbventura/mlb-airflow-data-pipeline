import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    create_table,
    get_database_path,
    insert_dataframe,
    read_table,
)


class TestDbUtilsIntegration:
    def test_full_workflow(self):
        """Test the complete workflow of database operations."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        try:
            conn = create_connection(db_file)

            create_table_sql = """
                CREATE TABLE player_stats (
                    player_id INTEGER,
                    player_name TEXT,
                    team_name TEXT,
                    hits INTEGER,
                    runs INTEGER,
                    avg REAL
                );
            """
            create_table(conn, create_table_sql)

            player_stats_df = pd.DataFrame(
                {
                    "player_id": [1, 2, 3],
                    "player_name": [
                        "Aaron Judge",
                        "Mookie Betts",
                        "Vladimir Guerrero Jr.",
                    ],
                    "team_name": ["Yankees", "Dodgers", "Blue Jays"],
                    "hits": [162, 158, 175],
                    "runs": [133, 117, 97],
                    "avg": [0.311, 0.269, 0.274],
                }
            )

            insert_dataframe(conn, "player_stats", player_stats_df)

            retrieved_df = read_table(conn, "player_stats")

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

            updated_stats_df = pd.DataFrame(
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

            insert_dataframe(conn, "player_stats", updated_stats_df)

            final_df = read_table(conn, "player_stats")

            assert len(final_df) == 4
            assert "Mike Trout" in final_df["player_name"].tolist()

            conn.close()

        finally:
            Path(db_file).unlink()

    def test_multiple_tables_workflow(self):
        """Test working with multiple tables."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        try:
            conn = create_connection(db_file)

            league_standings_df = pd.DataFrame(
                {
                    "team_name": ["Yankees", "Dodgers", "Blue Jays"],
                    "wins": [99, 100, 92],
                    "losses": [63, 62, 70],
                    "division": ["AL East", "NL West", "AL East"],
                }
            )

            player_stats_df = pd.DataFrame(
                {
                    "player_name": [
                        "Aaron Judge",
                        "Mookie Betts",
                        "Vladimir Guerrero Jr.",
                    ],
                    "team_name": ["Yankees", "Dodgers", "Blue Jays"],
                    "hits": [162, 158, 175],
                }
            )

            insert_dataframe(conn, "league_standings", league_standings_df)
            insert_dataframe(conn, "player_stats", player_stats_df)

            standings_result = read_table(conn, "league_standings")
            player_result = read_table(conn, "player_stats")

            assert len(standings_result) == 3
            assert len(player_result) == 3
            assert "wins" in standings_result.columns
            assert "hits" in player_result.columns

            conn.close()

        finally:
            Path(db_file).unlink()

    def test_database_path_creation(self):
        """Test that the database path function creates the data directory."""
        db_path = get_database_path()

        assert isinstance(db_path, str)
        assert db_path.endswith("mlb_data.db")
        assert "/data/" in db_path

        data_dir = Path(db_path).parent
        assert data_dir.exists()
        assert data_dir.is_dir()

    def test_error_handling_integration(self):
        """Test error handling in integrated workflow."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_file = tmp_file.name

        try:
            conn = create_connection(db_file)

            with pytest.raises(Exception, match="Failed to read table"):
                read_table(conn, "nonexistent_table")

            with pytest.raises(sqlite3.Error, match="Failed to create table"):
                create_table(conn, "INVALID SQL")

            conn.close()

        finally:
            Path(db_file).unlink()
