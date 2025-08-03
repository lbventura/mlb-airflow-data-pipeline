import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd


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
    try:
        conn = sqlite3.connect(db_file)
        yield conn
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to create database connection: {e}")
    finally:
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
    conn: sqlite3.Connection, table_name: str, df: pd.DataFrame
) -> None:
    """Inserts a pandas DataFrame into a table by appending rows.

    Args:
        conn: Database connection object
        table_name: Name of the target table
        df: DataFrame to insert

    Raises:
        sqlite3.Error: If insertion fails
    """
    try:
        df.to_sql(table_name, conn, if_exists="append", index=False)
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
    current_dir = Path(__file__).parent
    data_dir = current_dir / "db_data"
    data_dir.mkdir(exist_ok=True)
    return str(data_dir / "mlb_data.db")
