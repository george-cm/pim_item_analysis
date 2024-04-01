"""Database module for PIM item analysis project.

Provides functions for creating, configuring, and interacting with a SQLite
database for storing and analyzing PIM item data.
"""

import re
import sqlite3
import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional
from typing import Literal


def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_epoch(val):
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return datetime.date.fromisoformat(val.decode("utf-8"))


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode("utf-8"))


def convert_timestamp(val):
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.datetime.fromtimestamp(val.decode("utf-8"))


def register_adapters_and_converters() -> None:
    """Register adapters and converters for SQLite."""
    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    # sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)
    sqlite3.register_converter("date", convert_date)
    sqlite3.register_converter("datetime", convert_datetime)
    sqlite3.register_converter("timestamp", convert_timestamp)


register_adapters_and_converters()


@lru_cache(maxsize=None)
def regexp(x, y) -> Literal[1] | Literal[0]:
    """Create a REGEXP function for SQLite. Searches for x regex pattern in y."""
    return 1 if re.search(x, y) else 0


def db_create_connection(db_file) -> sqlite3.Connection:
    """create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object
    :raises: Exception in case of error
    """
    try:
        conn: sqlite3.Connection = sqlite3.connect(
            db_file,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            # detect_types=sqlite3.PARSE_COLNAMES,
        )
        conn.create_function("REGEXP", 2, regexp)
    except sqlite3.Error as e:
        raise e
    return conn


def db_drop_tables(conn: sqlite3.Connection, tables_list: List[str]):
    """Drop tables in the database"""
    tables = list(tables_list)
    cur: sqlite3.Cursor = conn.cursor()
    if tables:
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()


def db_trim_column_in_table(conn: sqlite3.Connection, table: str, column: str):
    """Trim column in the table"""
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = f"""
        UPDATE {table}
        SET {column} = TRIM({column})
    """
    cur.execute(sql)
    conn.commit()


def db_add_column(conn: sqlite3.Connection, table: str, column: str, column_type: str):
    """Add column to the table"""
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = f"""
        ALTER TABLE {table}
        ADD {column} {column_type}
    """
    cur.execute(sql)
    conn.commit()


def db_get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """Get list of column names for the given table.

    Args:
        conn: Database connection object.
        table: Name of the table.

    Returns:
        List of column names.
    """
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = f"""
        SELECT * FROM {table} LIMIT 1
    """
    cur.execute(sql)
    return [description[0] for description in cur.description]


def db_create_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: Dict[str, str],
    unique_index_columns: Optional[List[str]] = None,
) -> None:
    """Create table in the database"""
    fields = [f"[{column}] {column_type}" for column, column_type in columns.items()]
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(fields)})
        """
    cur.execute(sql)
    conn.commit()
    if unique_index_columns:
        sql = f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_unique_index ON {table_name}
                ({', '.join([f"[{x}]" for x in unique_index_columns])})
            """
        cur.execute(sql)
        conn.commit()


def get_export_date_from_file(filepath: Path) -> datetime.datetime:
    """Get the export date from the filename of a PIM export data file.

    Parses the filename to extract the date in YYYYMMDDHHMMSS format. Handles
    both 12 digit and 14 digit date formats.

    Args:
        filepath (Path): Path to the PIM export data file.

    Returns:
        datetime: The export date parsed from the filename.

    Raises:
        ValueError: If no date can be parsed from the filename.
    """
    filename = filepath.name
    pattern = re.compile(r".*_(\d{12,14}).*\.csv")
    text_date_lst = pattern.findall(filename)
    if text_date_lst:
        text_date = text_date_lst[-1]
    else:
        raise ValueError(f"Cannot parse date from filename {filename}")
    if len(text_date) == 12:
        dt_format = "%y%m%d%H%M%S"
    elif len(text_date) == 14:
        dt_format = "%Y%m%d%H%M%S"
    else:
        raise ValueError(f"Cannot parse date from filename {filename}")
    return datetime.datetime.strptime(text_date, dt_format)


def file_suffix(file_path: Path) -> str:
    """Get the file suffix."""
    file_name_parts = file_path.stem.split("_")
    if len(file_name_parts) < 2:
        raise ValueError(
            f"File name should contain at least one underscore: {file_path.stem}"
        )
    if file_name_parts[1][0].isnumeric():
        return file_name_parts[0]
    else:
        return "_".join(file_name_parts[:2])


def main():
    """Main function"""


if __name__ == "__main__":
    main()
