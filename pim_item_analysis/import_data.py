"""Import datat from CSV files into the database"""

import csv
import re
import sqlite3
import datetime
from functools import lru_cache
from pathlib import Path
from typing import List
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


sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
# sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return datetime.date.fromisoformat(val)


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    print(f"convert_datetime: {val=}")
    return datetime.datetime.fromisoformat(val)


def convert_timestamp(val):
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.datetime.fromtimestamp(val)


sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)
sqlite3.register_converter("timestamp", convert_timestamp)


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
            # detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            detect_types=sqlite3.PARSE_COLNAMES,
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


def db_create_tables(conn: sqlite3.Connection):
    """Create tables in the database"""
    cur: sqlite3.Cursor = conn.cursor()
    sql: str = """
        CREATE TABLE IF NOT EXISTS item_availability (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            EXPORT_DATE datetime NOT NULL,
            Status,
            [Item Source System],
            [Business Unit],
            [Item no.],
            SKU,
            [Item Short Description (English (US))],
            [Parent Product no.],
            [Parent Product Name (English (US))],
            [Ready for Enrichment],
            [Ready for Ecommerce],
            [Ecommerce Attribute Enrichment],
            [Item Approval],
            [Publish Ready Flag Ecomm],
            [Publish Override Ecomm],
            [Start Date Ecomm],
            [End Date Ecomm],
            Comment,
            [Publish Ready Flag AEM],
            [Publish Override AEM],
            [Start Date AEM],
            [End Date AEM],
            [Publish to Ecomm],
            [Created on (PIM)],
            [Created by (PIM).User name],
            [Created by (PIM).E-mail],
            [Last changed on (PIM)],
            [Last changed by (PIM).User name],
            [Last changed by (PIM).E-mail],
            [Deleted on (PIM)],
            [Item Delete Flag],
            [Kind Of Material],
            [SKU Target Market]
        )
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
    current_dir = Path(__file__).parent
    print(f"Current directory: {current_dir}")
    db_file: str = "pim_item_analysis.db"
    conn: sqlite3.Connection = db_create_connection(db_file)
    with db_create_connection(db_file) as conn:
        item_availability_file = (
            current_dir / "../input/test_data/"
            "item_availability_Static item list (4 items)_20240329145236_v10.csv"
        )
        current_file_suffix = file_suffix(item_availability_file)
        export_date = get_export_date_from_file(item_availability_file)
        print(current_file_suffix)
        print(export_date)
        db_drop_tables(conn, [current_file_suffix])
        db_create_tables(conn)
        with item_availability_file.open(encoding="utf-8") as f:
            csv_reader = csv.reader(f)
            header = ["EXPORT_DATE"] + next(csv_reader)
            columns = ", ".join([f"[{x}]" for x in header])
            sql = f"""
                INSERT INTO {current_file_suffix} ({columns})
                VALUES ({','.join(['?' for _ in header])})
            """
            data = [[export_date] + row for row in csv_reader]

            conn.executemany(sql, data)

            conn.commit()
        for row in conn.execute("SELECT * FROM item_availability").fetchall():
            print(row)

        print("\n\n==============\n\n")

        for row in conn.execute(
            """
                    SELECT * FROM item_availability
                    WHERE EXPORT_DATE >= ?
                """,
            (datetime.datetime(2024, 3, 28),),
        ).fetchall():
            print(row)


if __name__ == "__main__":
    main()
