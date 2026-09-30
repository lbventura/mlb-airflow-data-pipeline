"""SQLite storage for extracted baseball statistics.

A run is one league's daily snapshot, identified by (league_name, date).
A subsequent successful extraction for that league and date replaces the snapshot.

The run tables, league_standings and player_stats, hold daily snapshots for
the American and National Leagues. Each standings row describes a team;
each player row describes a player on a team.

A run key uniquely identifies a row within these tables: league and date plus
team_id for standings, and additionally player_id for player statistics.
For example, ("american_league", "2026-09-26", 147, 592450) identifies one
player's row on one team that day. Keeping team_id allows a player to appear
on multiple teams in the same snapshot. RUN_KEYS defines these column sets;
all key columns must be non-null and together form the table's primary key.
"""

import os
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd

from mlb_airflow_data_pipeline.statsapi_parameters_script import DATA_FILE_LOCATION

RUN_KEYS = {
    "league_standings": ("league_name", "date", "team_id"),
    "player_stats": ("league_name", "date", "team_id", "player_id"),
}


@contextmanager
def create_connection(db_file: str) -> Iterator[sqlite3.Connection]:
    """Creates a connection to the SQLite database.

    Args:
        db_file: Path to the SQLite database file

    Yields:
        sqlite3.Connection: Database connection object

    Raises:
        sqlite3.Error: If connection fails
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        yield conn
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to create database connection: {e}")
    finally:
        if conn:
            conn.close()


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _column_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dtype) or pd.api.types.is_bool_dtype(
        series.dtype
    ):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series.dtype):
        return "REAL"
    return "TEXT"


def _add_dataframe_columns(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({_quoted(table_name)})")
    }
    for column in dataframe.columns:
        if column not in existing:
            conn.execute(
                f"ALTER TABLE {_quoted(table_name)} "
                f"ADD COLUMN {_quoted(column)} {_column_type(dataframe[column])}"
            )


def _has_run_key(table_info: list[tuple], keys: tuple[str, ...]) -> bool:
    """Check that the schema enforces the expected unique, non-null row key.

    table_info contains SQLite PRAGMA table_info rows: positions 1, 3, and 5
    are the column name, NOT NULL flag, and one-based primary-key position
    (zero for non-key columns). The primary key must match keys in order,
    with no extra columns. This checks the schema, not whether a run exists;
    it also tells migration code whether the table has already been upgraded.
    """
    constraints = {row[1]: (row[3], row[5]) for row in table_info}
    return sum(row[5] > 0 for row in table_info) == len(keys) and all(
        constraints.get(key) == (1, position)
        for position, key in enumerate(keys, start=1)
    )


def _create_run_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: dict[str, str],
    keys: tuple[str, ...],
) -> None:
    """Create a statistics table with one row per run key, without committing.

    keys includes the snapshot's league/date and its team or team/player IDs.
    Non-key columns hold names and statistics and may contain missing values.
    player_id is required for every new player row. Older database writes
    omitted the DataFrame index containing that ID; migration archives those
    rows when their IDs cannot be recovered from the stored data.
    """
    definitions = [
        f"{_quoted(column)} {column_type}" + (" NOT NULL" if column in keys else "")
        for column, column_type in columns.items()
    ]
    definitions.append(f"PRIMARY KEY ({', '.join(_quoted(key) for key in keys)})")
    conn.execute(f"CREATE TABLE {_quoted(table_name)} ({', '.join(definitions)})")


def _archive_legacy_rows(conn: sqlite3.Connection, table_name: str) -> None:
    """Copy every original row before rebuilding, within the caller's transaction.

    Preserve any earlier archive, extending its schema and appending by column name.
    Routine saves never modify the archive after the table has a non-null run key.
    """
    archive_name = f"legacy_{table_name}"
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {_quoted(archive_name)} AS "
        f"SELECT * FROM {_quoted(table_name)} WHERE 0"
    )
    archived_columns = {
        row[1] for row in conn.execute(f"PRAGMA table_info({_quoted(archive_name)})")
    }
    columns = []
    for row in conn.execute(f"PRAGMA table_info({_quoted(table_name)})"):
        column, column_type = row[1], row[2]
        columns.append(_quoted(column))
        if column not in archived_columns:
            conn.execute(
                f"ALTER TABLE {_quoted(archive_name)} "
                f"ADD COLUMN {_quoted(column)} {column_type}"
            )
    column_names = ", ".join(columns)
    conn.execute(
        f"INSERT INTO {_quoted(archive_name)} ({column_names}) "
        f"SELECT {column_names} FROM {_quoted(table_name)}"
    )


def _ensure_run_table(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    """Create or migrate a run table, then add new statistics without committing.

    An absent table is created with the required non-null primary key. An
    existing table with that key only needs new statistic columns. Otherwise
    it must be rebuilt: adding columns cannot repair its primary-key constraints.
    Migration archives the source and retains fully keyed, unambiguous history.
    """
    keys = RUN_KEYS[table_name]
    table_info = conn.execute(f"PRAGMA table_info({_quoted(table_name)})").fetchall()
    if not table_info:
        columns = {
            column: _column_type(dataframe[column]) for column in dataframe.columns
        }
        _create_run_table(conn, table_name, columns, keys)
    elif _has_run_key(table_info, keys):
        _add_dataframe_columns(conn, table_name, dataframe)
    else:
        _migrate_run_table(conn, table_name, dataframe)


def _migrate_run_table(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    """Archive and rebuild a legacy table once, retaining unambiguous keyed rows.

    DISTINCT collapses identical rows. GROUP BY keeps keys with exactly one
    remaining version; conflicting versions stay in the archive. Historical
    rows have no extraction timestamp, so this cannot select the latest version.
    """
    keys = RUN_KEYS[table_name]
    _archive_legacy_rows(conn, table_name)
    _add_dataframe_columns(conn, table_name, dataframe)
    columns = {
        row[1]: row[2]
        for row in conn.execute(f"PRAGMA table_info({_quoted(table_name)})")
    }
    replacement = f"migrated_{table_name}"
    _create_run_table(conn, replacement, columns, keys)
    key_columns = ", ".join(_quoted(key) for key in keys)
    required_keys = " AND ".join(f"{_quoted(key)} IS NOT NULL" for key in keys)
    conn.execute(
        f"""
        WITH distinct_rows AS (
            SELECT DISTINCT * FROM {_quoted(table_name)} WHERE {required_keys}
        ), unambiguous_keys AS (
            SELECT {key_columns} FROM distinct_rows
            GROUP BY {key_columns} HAVING COUNT(*) = 1
        )
        INSERT INTO {_quoted(replacement)}
        SELECT distinct_rows.* FROM distinct_rows
        JOIN unambiguous_keys USING ({key_columns})
        """
    )
    conn.execute(f"DROP TABLE {_quoted(table_name)}")
    conn.execute(f"ALTER TABLE {_quoted(replacement)} RENAME TO {_quoted(table_name)}")


def _insert_rows(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    columns = list(dataframe.columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_statement = (
        f"INSERT INTO {_quoted(table_name)} "
        f"({', '.join(_quoted(column) for column in columns)}) "
        f"VALUES ({placeholders})"
    )
    normalized = dataframe.astype(object).where(pd.notna(dataframe), None)
    conn.executemany(insert_statement, normalized.itertuples(index=False, name=None))


def backup_database_before_migration(database_path: str) -> None:
    """Keep one copy of a pre-migration database beside the source file."""
    source_path = Path(database_path)
    if not source_path.exists():
        return
    backup_path = source_path.with_name(source_path.name + ".pre-issue-62.bak")
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source:
        needs_migration = False
        for table_name, keys in RUN_KEYS.items():
            table_info = source.execute(
                f"PRAGMA table_info({_quoted(table_name)})"
            ).fetchall()
            if table_info and not _has_run_key(table_info, keys):
                needs_migration = True
        if not needs_migration or backup_path.exists():
            return

        with tempfile.NamedTemporaryFile(dir=source_path.parent, delete=False) as file:
            temporary_path = Path(file.name)
        try:
            with closing(sqlite3.connect(temporary_path)) as backup:
                source.backup(backup)
            try:
                os.link(temporary_path, backup_path)
            except FileExistsError:
                pass
        finally:
            temporary_path.unlink(missing_ok=True)


def save_extraction_run(
    conn: sqlite3.Connection,
    league_name: str,
    execution_date: str,
    standings: pd.DataFrame,
    player_stats: pd.DataFrame,
) -> None:
    """Replace one league's daily standings and player statistics atomically.

    league_name and execution_date select the snapshot replaced in both run
    tables. Run keys distinguish its individual team and player rows; repeated
    incoming keys keep the last row in DataFrame order. This is a deterministic
    tie-breaker, not a comparison of extraction times. player_stats carries
    required player IDs in its index; null IDs are rejected before any writes.
    """
    standings_for_run = standings.assign(league_name=league_name, date=execution_date)
    players_for_run = player_stats.assign(league_name=league_name, date=execution_date)
    players_for_run.insert(0, "player_id", player_stats.index)
    batches = {
        "league_standings": standings_for_run,
        "player_stats": players_for_run,
    }
    for table_name, dataframe in batches.items():
        keys = RUN_KEYS[table_name]
        if dataframe.empty:
            raise ValueError(f"{table_name} has no rows")
        missing_columns = [key for key in keys if key not in dataframe.columns]
        if missing_columns:
            raise ValueError(
                f"{table_name} has missing run key columns: {missing_columns}"
            )
        null_keys = dataframe[list(keys)].isna()
        if null_keys.any(axis=None):
            raise ValueError(f"{table_name} has missing run keys (null values)")
        batches[table_name] = dataframe.drop_duplicates(list(keys), keep="last")

    conn.execute("SAVEPOINT extraction_run")
    try:
        for table_name, dataframe in batches.items():
            _ensure_run_table(conn, table_name, dataframe)
            conn.execute(
                f"DELETE FROM {_quoted(table_name)} WHERE league_name = ? AND date = ?",
                (league_name, execution_date),
            )
            _insert_rows(conn, table_name, dataframe)
        conn.execute("RELEASE SAVEPOINT extraction_run")
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT extraction_run")
        conn.execute("RELEASE SAVEPOINT extraction_run")
        raise


def read_player_stats(
    conn: sqlite3.Connection, league_name: str, execution_date: str
) -> pd.DataFrame:
    """Read player statistics produced for one league on one date."""
    try:
        return pd.read_sql_query(
            """
            SELECT *
            FROM player_stats
            WHERE league_name = ? AND date = ?
            """,
            conn,
            params=(league_name, execution_date),
        )
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to read scoped player statistics: {e}")


def get_database_path() -> str:
    """Returns the path to the SQLite database file.

    Returns:
        str: Path to the database file in the data directory
    """
    data_dir = Path(DATA_FILE_LOCATION)
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "mlb_data.db"
    if not db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.close()
    return str(db_path)
