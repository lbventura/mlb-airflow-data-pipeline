import os
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Literal

import pandas as pd

from mlb_airflow_data_pipeline.statsapi_parameters_script import DATA_FILE_LOCATION


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


def create_table(conn: sqlite3.Connection, create_table_sql: str) -> None:
    """Creates a table from a SQL statement.

    Args:
        conn: Database connection object
        create_table_sql: SQL CREATE TABLE statement

    Raises:
        sqlite3.Error: If table creation fails
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to create table: {e}")


def insert_dataframe(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    mode: Literal["fail", "replace", "append"] | None = "append",
) -> None:
    """Inserts a pandas DataFrame into a table.

    Args:
        conn: Database connection object
        table_name: Name of the target table
        df: DataFrame to insert

    Raises:
        sqlite3.Error: If insertion fails
    """
    assert mode is not None, "Mode must be one of 'fail', 'replace', or 'append'."
    try:
        df.to_sql(table_name, conn, if_exists=mode, index=False)
        conn.commit()
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to insert DataFrame into table {table_name}: {e}")
    except Exception as e:
        raise Exception(f"Failed to insert DataFrame into table {table_name}: {e}")


def read_table(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """Reads a table into a pandas DataFrame.

    Args:
        conn: Database connection object
        table_name: Name of the table to read

    Returns:
        pd.DataFrame: DataFrame containing the table data

    Raises:
        sqlite3.Error: If reading fails
    """
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to read table {table_name}: {e}")
    except Exception as e:
        raise Exception(f"Failed to read table {table_name}: {e}")


def ensure_dataframe_columns(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    """Add new DataFrame columns to an existing SQLite table before appending."""
    quoted_table = '"' + table_name.replace('"', '""') + '"'
    existing_columns = {
        row[1] for row in conn.execute(f"PRAGMA table_info({quoted_table})")
    }
    if not existing_columns:
        return

    for column in dataframe.columns:
        if column in existing_columns:
            continue
        dtype = dataframe[column].dtype
        column_type = (
            "INTEGER"
            if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_bool_dtype(dtype)
            else "REAL" if pd.api.types.is_float_dtype(dtype) else "TEXT"
        )
        quoted_column = '"' + column.replace('"', '""') + '"'
        conn.execute(
            f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_column} {column_type}"
        )
    conn.commit()


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


def _ensure_run_table(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    key_columns = (
        ("league_name", "date", "team_id")
        if table_name == "league_standings"
        else ("league_name", "date", "team_id", "player_id")
    )
    definitions = [
        f'{_quoted(column)} {_column_type(dataframe[column])} NOT NULL'
        for column in key_columns
    ]
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS {_quoted(table_name)} '
        f'({", ".join(definitions)})'
    )
    existing_columns = {
        row[1] for row in conn.execute(f'PRAGMA table_info({_quoted(table_name)})')
    }
    for column in dataframe.columns:
        if column not in existing_columns:
            conn.execute(
                f'ALTER TABLE {_quoted(table_name)} '
                f'ADD COLUMN {_quoted(column)} {_column_type(dataframe[column])}'
            )


def _archive_legacy_rows(conn: sqlite3.Connection, table_name: str) -> None:
    archive_name = f"legacy_{table_name}"
    archive_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (archive_name,),
    ).fetchone()
    if not archive_exists:
        conn.execute(
            f'CREATE TABLE {_quoted(archive_name)} AS '
            f'SELECT * FROM {_quoted(table_name)} WHERE 0'
        )
    else:
        archived_columns = {
            row[1]
            for row in conn.execute(f'PRAGMA table_info({_quoted(archive_name)})')
        }
        for row in conn.execute(f'PRAGMA table_info({_quoted(table_name)})'):
            if row[1] not in archived_columns:
                conn.execute(
                    f'ALTER TABLE {_quoted(archive_name)} '
                    f'ADD COLUMN {_quoted(row[1])} {row[2]}'
                )
    missing_key = (
        "date IS NULL OR league_name IS NULL"
        if table_name == "league_standings"
        else "date IS NULL OR league_name IS NULL OR player_id IS NULL"
    )
    condition = missing_key if not archive_exists else "date IS NULL"
    conn.execute(
        f'INSERT INTO {_quoted(archive_name)} '
        f'SELECT * FROM {_quoted(table_name)} WHERE {condition}'
    )
    conn.execute(f'DELETE FROM {_quoted(table_name)} WHERE date IS NULL')


def _resolve_existing_duplicates(
    conn: sqlite3.Connection, table_name: str, keys: list[str]
) -> None:
    """Archive repeated stored keys; retain identical rows only once."""
    key_columns = ", ".join(_quoted(key) for key in keys)
    required_keys = " AND ".join(f'{_quoted(key)} IS NOT NULL' for key in keys)
    rows = conn.execute(
        f'SELECT rowid, * FROM {_quoted(table_name)} WHERE {required_keys} '
        f'ORDER BY {key_columns}, rowid'
    ).fetchall()
    groups: dict[tuple, list[tuple]] = {}
    columns = [
        row[1] for row in conn.execute(f'PRAGMA table_info({_quoted(table_name)})')
    ]
    key_positions = [columns.index(key) + 1 for key in keys]
    for row in rows:
        key = tuple(row[position] for position in key_positions)
        groups.setdefault(key, []).append(row)

    for group in groups.values():
        if len(group) < 2:
            continue
        for row in group:
            conn.execute(
                f'INSERT INTO {_quoted("legacy_" + table_name)} '
                f'SELECT * FROM {_quoted(table_name)} WHERE rowid = ?',
                (row[0],),
            )
        keep_one = all(row[1:] == group[0][1:] for row in group[1:])
        to_delete = group[1:] if keep_one else group
        conn.executemany(
            f'DELETE FROM {_quoted(table_name)} WHERE rowid = ?',
            ((row[0],) for row in to_delete),
        )


def _insert_rows(
    conn: sqlite3.Connection, table_name: str, dataframe: pd.DataFrame
) -> None:
    columns = list(dataframe.columns)
    placeholders = ", ".join("?" for _ in columns)
    sql = (
        f'INSERT INTO {_quoted(table_name)} '
        f'({", ".join(_quoted(column) for column in columns)}) '
        f'VALUES ({placeholders})'
    )
    rows = (
        tuple(value.item() if hasattr(value, "item") else value for value in row)
        for row in dataframe.astype(object).where(pd.notna(dataframe), None).itertuples(
            index=False, name=None
        )
    )
    conn.executemany(sql, rows)


def backup_database_before_migration(database_path: str) -> None:
    """Keep one copy of a pre-migration database beside the source file."""
    source_path = Path(database_path)
    if not source_path.exists():
        return
    backup_path = source_path.with_name(source_path.name + ".pre-issue-62.bak")
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(source_uri, uri=True) as source:
        has_run_tables = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name IN ('league_standings', 'player_stats')"
        ).fetchone()
        already_migrated = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'legacy_league_standings'"
        ).fetchone()
        if not has_run_tables or already_migrated or backup_path.exists():
            return

        with tempfile.NamedTemporaryFile(dir=source_path.parent, delete=False) as file:
            temporary_path = Path(file.name)
        try:
            with sqlite3.connect(temporary_path) as backup:
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
    """Replace one league's daily standings and player statistics atomically."""
    standings_for_run = standings.assign(
        league_name=league_name, date=execution_date
    )
    players_for_run = player_stats.assign(
        league_name=league_name, date=execution_date
    ).copy()
    players_for_run.insert(0, "player_id", player_stats.index)
    batches = (
        ("league_standings", standings_for_run, ["league_name", "date", "team_id"]),
        (
            "player_stats",
            players_for_run,
            ["league_name", "date", "team_id", "player_id"],
        ),
    )
    prepared = []
    for table_name, dataframe, keys in batches:
        if dataframe.empty or dataframe[keys].isna().any().any():
            raise ValueError(f"{table_name} has missing run keys or no rows")
        prepared.append(
            (table_name, dataframe.drop_duplicates(keys, keep="last"), keys)
        )

    conn.execute("SAVEPOINT extraction_run")
    try:
        for table_name, dataframe, keys in prepared:
            _ensure_run_table(conn, table_name, dataframe)
            _archive_legacy_rows(conn, table_name)

        team_ids = prepared[0][1]["team_id"].tolist()
        conn.execute(
            'DELETE FROM league_standings WHERE league_name = ? AND date = ?',
            (league_name, execution_date),
        )
        conn.execute(
            'DELETE FROM league_standings WHERE league_name IS NULL AND date = ? '
            f'AND team_id IN ({", ".join("?" for _ in team_ids)})',
            (execution_date, *team_ids),
        )
        conn.execute(
            'DELETE FROM player_stats WHERE league_name = ? AND date = ?',
            (league_name, execution_date),
        )
        for table_name, dataframe, keys in prepared:
            _resolve_existing_duplicates(conn, table_name, keys)
            conn.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS {_quoted(table_name + "_run_key")} '
                f'ON {_quoted(table_name)} ({", ".join(_quoted(key) for key in keys)})'
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
