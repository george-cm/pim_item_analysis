"""Load Excel file into a list of lists.

Parses an Excel file at the given file path and returns
a list containing the rows and columns of the first worksheet.
Each inner list represents a row, with the cell values as elements.
"""

import csv
import io
import sqlite3
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional

import openpyxl

from pim_item_analysis.db import db_create_table
from pim_item_analysis.db import db_drop_tables
from pim_item_analysis.db import file_prefix
from pim_item_analysis.db import get_export_date_from_file


def round_seconds(precise_datetime: datetime) -> datetime:
    """Round to nearest second."""
    adjusted_datetime: datetime = precise_datetime + timedelta(seconds=0.5)
    return adjusted_datetime.replace(microsecond=0)


def load_file_into_db(
    conn: sqlite3.Connection,
    file_path: Path,
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
    label: str | None = None,
) -> int:
    """Load file into the database"""
    current_file_suffix: str = file_prefix(file_path)
    export_date: datetime = get_export_date_from_file(file_path)
    print(current_file_suffix)
    print(export_date)
    with file_path.open(encoding="utf-8") as f:
        csv_reader: Iterator[List[str]] = csv.reader(f)
        header: List[str] = next(csv_reader)
        columns: list[str] = [x for x in (["export_date"] + header)]
        columns_str: str = ", ".join([f"[{x}]" for x in columns])
        extra_fields: dict[str, str] = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "export_date": "DATETIME NOT NULL",
        }
        fields: dict[str, str] = {**extra_fields, **{k: "TEXT" for k in columns[1:]}}
        if drop_table_first:
            db_drop_tables(conn, [current_file_suffix])
        db_create_table(
            conn, current_file_suffix, fields, unique_index_columns=unique_index_columns
        )

        sql: str = f"""
            INSERT OR IGNORE INTO {current_file_suffix} ({columns_str})
            VALUES ({','.join(['?' for _ in columns])})
        """
        data: List[List[str | datetime]] = [[export_date] + row for row in csv_reader]

        cursor = conn.cursor()
        cursor.executemany(sql, data)
        inserted_row_count = cursor.rowcount
        # add the label
        if label:
            sql: str = """
                INSERT INTO labels_pim (export_date, label)
                VALUES (?, ?)
                ON CONFLICT (export_date) DO UPDATE SET label = ?
                """
            cursor.execute(
                sql,
                (export_date, label, label),
            )
        conn.commit()
        return inserted_row_count


def load_excel_to_db(
    conn: sqlite3.Connection,
    file_path: Path,
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
    label: str | None = None,
) -> int:
    """Load Excel file to database."""
    current_file_suffix: str = file_prefix(file_path)
    export_date: datetime = round_seconds(
        datetime.fromtimestamp(file_path.stat().st_ctime)
    )
    # print(export_date)
    with file_path.open("rb") as f:
        in_memory_file = io.BytesIO(f.read())

    workbook: openpyxl.Workbook = openpyxl.load_workbook(in_memory_file, read_only=True)
    sheet = workbook.worksheets[0]

    row_iter = sheet.iter_rows(values_only=True)  # type: ignore
    header: tuple[str] = next(row_iter)  # type: ignore
    columns = list(("export_date", "Item no.", *header))
    columns_str = ", ".join([f"[{x}]" for x in columns])
    extra_fields: dict[str, str] = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "export_date": "DATETIME NOT NULL",
    }
    fields: dict[str, str] = {**extra_fields, **{k: "TEXT" for k in columns[1:]}}
    # print(f"{header=}")
    # print(f"{columns=}")
    # print(f"{columns_str=}")
    # print(f"{fields=}")

    if drop_table_first:
        db_drop_tables(conn, [current_file_suffix])
    db_create_table(
        conn, current_file_suffix, fields, unique_index_columns=unique_index_columns
    )
    # data: Tuple[Any]
    # for i, row in enumerate(sheet.iter_rows(values_only=True)):
    #     data.append(row)
    data: List[List[Any]] = [
        [export_date, f"{row[1]}~{row[0]}", *row] for row in row_iter
    ]

    sql: str = f"""
        INSERT OR IGNORE INTO {current_file_suffix} ({columns_str})
        VALUES ({','.join(['?' for _ in columns])})
    """

    cursor: sqlite3.Cursor = conn.cursor()
    cursor.executemany(sql, data)
    inserted_row_count: int = cursor.rowcount

    # add the label
    if label:
        sql: str = """
            INSERT INTO labels_hybris (export_date, label)
            VALUES (?, ?)
            ON CONFLICT (export_date) DO UPDATE SET label = ?
            """
        cursor.execute(
            sql,
            (export_date, label, label),
        )
    conn.commit()
    return inserted_row_count


def load_docfile_into_db(
    conn: sqlite3.Connection,
    file_path: Path,
    prefix: str,
    request_date: datetime,
    config: Dict[str, Any],
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
    label: str | None = None,
) -> int:
    """Load doc Excel xlsx file into the database"""
    with file_path.open("rb") as f:
        in_memory_file = io.BytesIO(f.read())

    sheets: List[str] = config["doc"]["file_sheets"][prefix]
    workbook: openpyxl.Workbook = openpyxl.load_workbook(in_memory_file, read_only=True)
    for sh in workbook.worksheets:
        if sh.title.lower() not in sheets:
            continue
        print(f"sheet name: {sh.title}")
        config_header = list(config["doc"]["sheet_headers"][sh.title.lower()].keys())
        config_header_requirements = list(
            config["doc"]["sheet_headers"][sh.title.lower()].values()
        )
        print(f"{config_header=}")
        print(f"{config_header_requirements=}")
        start_row_header = config["doc"]["start_rows"][sh.title.lower()]["header"]
        start_row_data = config["doc"]["start_rows"][sh.title.lower()]["data"]

        empty_rows: int = 0
        for i, row in enumerate(sh.iter_rows(values_only=True), start=1):
            if i == start_row_header:
                print(f"{i}:header: {row}")
            if i >= start_row_data:
                print(f"{i}:data: {row}")
                data = [x.strip() if isinstance(x, str) else x for x in row]
                print(f"{data=}")
                if not any(data):
                    empty_rows += 1
            if empty_rows >= 3:
                break
        print()
    return 1


def main():
    """Main function."""
    print("This module doesn't do anything on it's own.")


if __name__ == "__main__":
    main()
