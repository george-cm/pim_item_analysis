"""Database module for PIM item analysis project.

Provides functions for creating, configuring, and interacting with a SQLite
database for storing and analyzing PIM item data.
"""

import datetime
import re
import sqlite3
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import LiteralString
from typing import Optional
from typing import Union


class Missing:
    def __init__(self, column_name: str):
        self.column_name = column_name

    def __str__(self):
        return f"<MISSING VALUE IN COLUMN {self.column_name}>"

    def __repr__(self):
        return f"Missing({column_name})"


def adapt_missing_value(val: Missing) -> str:
    "Adapt Missing to str."
    return str(val)


def convert_missing_value(val: bytes) -> Missing:
    "Convert Missing string to Missing obj."
    return val.decode("utf-8").strip("<>").replace("MISSING VALUE IN COLUMN").strip()


def adapt_date_iso(val: datetime.date) -> str:
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val: datetime.datetime) -> str:
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_epoch(val: datetime.datetime) -> int:
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())


def convert_date(val: bytes) -> datetime.date:
    """Convert ISO 8601 date to datetime.date object."""
    return datetime.date.fromisoformat(val.decode("utf-8"))


def convert_datetime(val: bytes) -> datetime.datetime:
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode("utf-8"))


def convert_timestamp(val: bytes) -> datetime.datetime:
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.datetime.fromtimestamp(float(val.decode("utf-8")))


def register_adapters_and_converters() -> None:
    """Register adapters and converters for SQLite."""
    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    # sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)
    sqlite3.register_converter("date", convert_date)
    sqlite3.register_converter("datetime", convert_datetime)
    sqlite3.register_converter("timestamp", convert_timestamp)
    sqlite3.register_adapter(Missing, adapt_missing_value)
    sqlite3.register_converter("missing", convert_missing_value)


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


def normalize_name(name: str) -> str:
    """Normalize name for use in database."""
    name = name.lower()
    replacement_pattern: re.Pattern[str] = re.compile(r"[^a-z0-9]+")
    name = replacement_pattern.sub("_", name)
    name = name.strip("_")
    return name


def db_create_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: Dict[str, str],
    unique_index_columns: Optional[List[str]] = None,
) -> None:
    """Create table in the database"""
    # table_name: str = normalize_name(table_name)
    fields = [f"[{column}] {column_type}" for column, column_type in columns.items()]
    # print(f"db_create_table {fields=}")
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


def db_create_label_tables(conn: sqlite3.Connection) -> None:
    """Create labels tables in the database."""
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS labels_pim (
                id          INTEGER  PRIMARY KEY AUTOINCREMENT,
                export_date DATETIME NOT NULL UNIQUE,
                label       TEXT
            )
            """
    )
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS labels_hybris (
                id          INTEGER  PRIMARY KEY AUTOINCREMENT,
                export_date DATETIME NOT NULL UNIQUE,
                label       TEXT
            )
            """
    )
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS labels_doc (
                id              INTEGER  PRIMARY KEY AUTOINCREMENT,
                request_date  DATETIME NOT NULL UNIQUE,
                label           TEXT
            )
            """
    )


def db_get_pim_datasets(
    conn: sqlite3.Connection, descending: bool = False
) -> List[List[str | datetime.datetime | int]]:
    """Get list of PIM datasets."""
    cursor: sqlite3.Cursor = conn.cursor()
    datasets: sqlite3.Cursor = cursor.execute(
        f"""
        SELECT "left"."export_date",
               "left"."Item count",
               "right"."label"
        FROM
            (
                SELECT DISTINCT export_date,
                                count
                                ("Item no.") AS "Item count"
                FROM item_availability
                GROUP BY export_date
                ORDER BY export_date {'DESC' if descending else 'ASC'}
            )
            AS "left"
        LEFT JOIN labels_pim AS "right"
            ON "left"."export_date" = "right"."export_date"
    """
    )
    return list(datasets)


def db_get_hybris_datasets(
    conn: sqlite3.Connection, descending: bool = False
) -> List[List[str | datetime.datetime | int]]:
    """Get list of PIM datasets."""
    cursor: sqlite3.Cursor = conn.cursor()
    if not db_table_exists(conn, "skus_status"):
        return []
    datasets: sqlite3.Cursor = cursor.execute(
        f"""
        SELECT "left"."export_date",
               "left"."Item count",
               "right"."label"
        FROM
            (
                SELECT DISTINCT export_date,
                                count
                                ("Item no.") AS "Item count"
                FROM skus_status
                GROUP BY export_date
                ORDER BY export_date {'DESC' if descending else 'ASC'}
            )
            AS "left"
        LEFT JOIN labels_hybris AS "right"
            ON "left"."export_date" = "right"."export_date"
    """
    )
    return list(datasets)


def db_get_doc_datasets(
    conn: sqlite3.Connection, descending: bool = False
) -> List[List[str | datetime.datetime | int]]:
    """Get list of DoC datasets."""
    cursor: sqlite3.Cursor = conn.cursor()
    sql: Template = Template(
        """
        SELECT "left"."request_date",
            "left"."$counts_column",
            "left"."Sheet Name",
            "left"."File Name",
            "right"."label"
        FROM
            (
                SELECT DISTINCT
                    request_date,
                    count("$counts_column") AS "$counts_column",
                    "$table_name" as "Sheet Name",
                    "file_name" as "File Name"
                FROM $table_name
                GROUP BY request_date
                ORDER BY request_date $order
            )
            AS "left"
        LEFT JOIN labels_doc AS "right"
            ON "left"."request_date" = "right"."request_date"
    """
    )
    all_data: List[List[str | datetime.datetime | int]] = []
    tables: Dict[str, str] = {
        "doc_cert_data_template": "CERTIFICATION_NUMBER",
        "doc_sku_data_template": "ITEM_CODE",
        "sku_list_complete_translation": "Item No.",
        "pim_sku_load": "Item No.",
    }
    for table_name, counts_column in tables.items():
        order: str = "DESC" if descending else "ASC"
        if db_table_exists(conn, table_name):
            datasets: sqlite3.Cursor = cursor.execute(
                sql.substitute(table_name=table_name, counts_column=counts_column, order=order)
            )
            all_data.extend(list(datasets))
    return all_data


def db_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if table exists in the database."""
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = f"""
        SELECT count(name)
        FROM sqlite_master
        WHERE type = 'table' AND
            name = '{table_name}'
    """
    res = cur.execute(sql)
    return bool(res.fetchone()[0])


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


def db_add_label(
    conn: sqlite3.Connection,
    dataset_type: Literal["pim", "hybris", "doc"],
    dataset_datetime: datetime.datetime,
    label: Optional[str],
) -> None:
    """Add label to the lables table. Overwrites label if it exists."""
    date_column: str = "request_date" if dataset_type == "doc" else "export_date"
    db_create_label_tables(conn)
    sql: LiteralString = f"""
        INSERT INTO labels_{dataset_type} ({date_column}, label)
        VALUES (?, ?)
        ON CONFLICT ({date_column}) DO UPDATE SET label = ?
        """
    conn.execute(
        sql,
        (dataset_datetime, label, label),
    )
    conn.commit()


def db_get_label_for_date(
    conn: sqlite3.Connection,
    dataset_type: Literal["pim", "hybris", "doc"],
    dataset_datetime: Union[datetime.datetime, datetime.date],
) -> Optional[str]:
    """Add label to the lables table."""
    date_column: str = "request_date" if dataset_type == "doc" else "export_date"
    db_create_label_tables(conn)
    sql: LiteralString = f"""
        SELECT label
        FROM labels_{dataset_type}
        WHERE {date_column} = ?
        """
    result: Any = conn.execute(
        sql,
        (dataset_datetime,),
    ).fetchone()
    return result[0] if result else None


def file_prefix(file_path: Union[Path, str]) -> str:  # type: ignore
    """Get the file suffix."""
    if isinstance(file_path, str):
        file_path = Path(file_path)
    file_name_parts = normalize_name(file_path.stem).split("_")
    if len(file_name_parts) < 2:
        raise ValueError(f"File name should contain at least one underscore: {file_path.stem}")
    # check if the fist character in the second element of the list is a number
    if file_name_parts[1][0].isnumeric():
        return file_name_parts[0]
    else:
        return "_".join(file_name_parts[:2])


def round_seconds(precise_datetime: datetime.datetime) -> datetime.datetime:
    """Round to nearest second."""
    adjusted_datetime: datetime.datetime = precise_datetime + datetime.timedelta(seconds=0.5)
    return adjusted_datetime.replace(microsecond=0)


def main():
    """Main function"""
    print("This module doesn't do anything on it's own.")


if __name__ == "__main__":
    main()
