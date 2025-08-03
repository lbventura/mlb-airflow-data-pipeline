import sqlite3
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
