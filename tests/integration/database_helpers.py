"""SQLite source fixtures and state used by integration tests."""

import sqlite3
from pathlib import Path

import pandas as pd

from mlb_airflow_data_pipeline.db_utils import save_extraction_run

LEAGUE = "american_league"
DATE = "2026-10-11"
COUNTS = {"batter": 2, "pitcher": 2, "defender": 4}


def extracted_tables(conn: sqlite3.Connection) -> dict:
    """Include source rows, table definitions, and indexes (including archives)."""
    tables = conn.execute(
        """SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (
            'player_stats', 'league_standings',
            'legacy_player_stats', 'legacy_league_standings'
        ) ORDER BY name"""
    ).fetchall()
    return {
        table: (
            conn.execute(
                "SELECT type, name, sql FROM sqlite_master WHERE tbl_name = ? ORDER BY type, name",
                (table,),
            ).fetchall(),
            conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid').fetchall(),
        )
        for (table,) in tables
    }


def extract(
    database: Path, players: pd.DataFrame, league: str = LEAGUE, date: str = DATE
) -> None:
    with sqlite3.connect(database) as conn:
        save_extraction_run(
            conn,
            league,
            date,
            pd.DataFrame({"team_id": players.team_id.unique()}).assign(
                name="Team", w=50, l=40
            ),
            players,
        )
