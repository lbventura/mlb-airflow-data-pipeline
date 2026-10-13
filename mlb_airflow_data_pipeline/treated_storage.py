"""Publish and read treated snapshots within the caller's transaction."""

from datetime import date
import sqlite3

import numpy as np
import pandas as pd

from mlb_airflow_data_pipeline.db_utils import insert_rows
from mlb_airflow_data_pipeline.statsapi_parameters_script import LEAGUE_MAPPING
from mlb_airflow_data_pipeline.treated_schema import (
    LAYOUTS,
    TREATED_TABLES,
    metric_types,
    table_statements,
)


class SnapshotUnavailable(ValueError):
    """A requested treatment snapshot has not completed or was invalidated."""


def validate_scope(league_name: str, execution_date: str) -> None:
    """Require a supported league and a date written as YYYY-MM-DD."""
    if league_name not in LEAGUE_MAPPING:
        raise ValueError(f"Unknown league: {league_name}")
    if date.fromisoformat(execution_date).isoformat() != execution_date:
        raise ValueError(f"Expected ISO date: {execution_date}")


def ensure_treated_tables(conn: sqlite3.Connection) -> None:
    """Create treated tables and indexes without committing."""
    for table in ("treated_snapshots", *TREATED_TABLES.values()):
        for statement in table_statements(table):
            conn.execute(statement)


def save_treated_category(
    conn: sqlite3.Connection,
    category: str,
    league_name: str,
    execution_date: str,
    dataframe: pd.DataFrame,
    layout: str,
) -> None:
    """Replace a category and its completion record; the caller commits or rolls back."""
    validate_scope(league_name, execution_date)
    rows = _prepare_treated_rows(dataframe, category, layout)
    rows = rows.assign(league_name=league_name, date=execution_date, layout=layout)
    table = TREATED_TABLES[category]

    conn.execute(
        f"DELETE FROM {table} WHERE league_name = ? AND date = ?",
        (league_name, execution_date),
    )
    insert_rows(conn, table, rows)
    conn.execute(
        """
        INSERT OR REPLACE INTO treated_snapshots
            (league_name, date, category, layout, row_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (league_name, execution_date, category, layout, len(rows)),
    )


def read_treated_stats(
    conn: sqlite3.Connection,
    category: str,
    league_name: str,
    execution_date: str,
) -> pd.DataFrame:
    """Read a completed category, including an intentionally empty snapshot."""
    metrics = metric_types(category)
    validate_scope(league_name, execution_date)
    expected_count = _completed_row_count(conn, category, league_name, execution_date)
    table = TREATED_TABLES[category]

    rows = pd.read_sql_query(
        f"""
        SELECT * FROM {table}
        WHERE league_name = ? AND date = ?
        ORDER BY
            CASE position
                WHEN 'C'  THEN 0
                WHEN '1B' THEN 1
                WHEN '2B' THEN 2
                WHEN '3B' THEN 3
                WHEN 'SS' THEN 4
                WHEN 'LF' THEN 5
                WHEN 'CF' THEN 6
                WHEN 'RF' THEN 7
                ELSE 8
            END,
            team_id, player_id
        """,
        conn,
        params=(league_name, execution_date),
    )
    if len(rows) != expected_count:
        raise ValueError(
            f"Treatment row count mismatch: {league_name}/{execution_date}/{category}"
        )

    # SQLite returns object columns for empty results and all-NULL statistics.
    for column in [*metrics, "player_id"]:
        rows[column] = pd.to_numeric(rows[column], errors="raise")
    return rows.drop(columns="row_id")


def read_treated_history(
    conn: sqlite3.Connection,
    category: str,
    league_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Read completed snapshots in an inclusive date range, ordered by date."""
    frames = []
    if "treated_snapshots" in _existing_tables(conn):
        dates = conn.execute(
            """
            SELECT date FROM treated_snapshots
            WHERE league_name = ? AND category = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (league_name, category, start_date, end_date),
        ).fetchall()
        frames = [
            read_treated_stats(conn, category, league_name, day) for (day,) in dates
        ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def invalidate_treated_run(
    conn: sqlite3.Connection, league_name: str, execution_date: str
) -> None:
    """Delete a run's derived rows and completion records without committing."""
    existing_tables = _existing_tables(conn)
    for table in (*TREATED_TABLES.values(), "treated_snapshots"):
        if table in existing_tables:
            conn.execute(
                f"DELETE FROM {table} WHERE league_name = ? AND date = ?",
                (league_name, execution_date),
            )


def _prepare_treated_rows(
    dataframe: pd.DataFrame, category: str, layout: str
) -> pd.DataFrame:
    """Validate treatment output and copy it with infinite metrics replaced by NULL."""
    metrics = metric_types(category)
    if layout not in LAYOUTS:
        raise ValueError(f"Unsupported treatment layout: {layout}")

    allowed_columns = set(metrics) | {"player_id", "playername", "team_id", "position"}
    unknown_columns = set(dataframe.columns) - allowed_columns
    if unknown_columns:
        raise ValueError(f"Unsupported {category} columns: {sorted(unknown_columns)}")
    _validate_player_identity(dataframe, category, layout)

    rows = dataframe.copy()
    metric_columns = rows.columns.intersection(metrics)
    rows[metric_columns] = rows[metric_columns].replace([np.inf, -np.inf], np.nan)
    return rows


def _validate_player_identity(rows: pd.DataFrame, category: str, layout: str) -> None:
    """Require named players with positive IDs and unique team/player/position keys."""
    keys = ["team_id", "player_id"]
    if category == "defender" and layout == "grouped":
        keys.append("position")
    required_columns = [*keys, "playername"]

    missing_columns = set(required_columns) - set(rows.columns)
    if missing_columns:
        raise ValueError(f"Incomplete {category} identity: {sorted(missing_columns)}")
    if rows[required_columns].isna().any(axis=None):
        raise ValueError(f"Incomplete {category} identity: null values")

    for column in ("team_id", "player_id"):
        values = pd.to_numeric(rows[column], errors="raise")
        if ((values % 1) != 0).any() or (values <= 0).any():
            raise ValueError(f"Invalid {category} {column}")
    if rows.playername.eq("").any():
        raise ValueError(f"Empty {category} player name")
    if rows.duplicated(keys).any():
        raise ValueError(f"Duplicate {category} identity: {keys}")


def _completed_row_count(
    conn: sqlite3.Connection, category: str, league_name: str, execution_date: str
) -> int:
    """Require a completion record; a zero row count is still a completed snapshot."""
    record = None
    if "treated_snapshots" in _existing_tables(conn):
        record = conn.execute(
            """
            SELECT row_count FROM treated_snapshots
            WHERE league_name = ? AND date = ? AND category = ?
            """,
            (league_name, execution_date, category),
        ).fetchone()
    if record is None:
        raise SnapshotUnavailable(
            f"Treatment unavailable: {league_name}/{execution_date}/{category}"
        )
    row_count: int = record[0]
    return row_count


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    return {name for (name,) in tables}
