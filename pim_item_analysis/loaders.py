"""Load Excel file into a list of lists.
Parses an Excel file at the given file path and returns
a list containing the rows and columns of the first worksheet.
Each inner list represents a row, with the cell values as elements.
"""

import csv
import datetime
import io
import sqlite3
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import dateutil
import dateutil.parser
import openpyxl
from rich.console import Console

from pim_item_analysis.db import Missing
from pim_item_analysis.db import db_add_label
from pim_item_analysis.db import db_create_table
from pim_item_analysis.db import db_drop_tables
from pim_item_analysis.db import file_prefix
from pim_item_analysis.db import get_export_date_from_file
from pim_item_analysis.db import normalize_name
from pim_item_analysis.db import round_seconds

console = Console()


def load_pimfile_to_db(
    conn: sqlite3.Connection,
    file_path: Path,
    config: Dict[str, Any],
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
    label: str | None = None,
) -> int:
    """Load file into the database"""
    header_maps: Dict[str, Dict[str, str]] = config["header_maps"]
    current_file_suffix: str = file_prefix(file_path)
    export_date: datetime.datetime = get_export_date_from_file(file_path)
    console.print(f"{current_file_suffix=}")
    console.print(f"{export_date=}")

    cursor = conn.cursor()

    # add the export_data to the pim_datasets table
    db_create_table(
        conn,
        "pim_datasets",
        {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "export_date": "DATETIME NOT NULL",
        },
    )
    sql: str = "SELECT id FROM pim_datasets WHERE export_date = ?"
    res = cursor.execute(sql, (export_date,)).fetchone()
    if not res:
        sql = "INSERT INTO pim_datasets (export_date) VALUES (?) RETURNING id"
        res = cursor.execute(sql, (export_date,)).fetchone()
    pim_dataset_id: int = res[0]

    with file_path.open(encoding="utf-8") as f:
        csv_reader: Iterator[List[str]] = csv.reader(f)
        header: List[str] = next(csv_reader)
        if current_file_suffix in header_maps:
            header = [header_maps[current_file_suffix].get(x, x) for x in header]
        columns: list[str] = [x for x in (["export_date", "dataset_id"] + header)]
        columns_str: str = ", ".join([f"[{x}]" for x in columns])
        extra_fields: dict[str, str] = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "export_date": "DATETIME NOT NULL REFERENCES pim_datasets('export_date')",
            "dataset_id": "INTEGER NOT NULL REFERENCES pim_datasets('id')",
        }
        fields: dict[str, str] = {**extra_fields, **{k: "TEXT" for k in columns[2:]}}
        if drop_table_first:
            db_drop_tables(conn, [current_file_suffix])
        db_create_table(
            conn, current_file_suffix, fields, unique_index_columns=unique_index_columns
        )
        sql = f"""
            INSERT OR IGNORE INTO {current_file_suffix} ({columns_str})
            VALUES ({','.join(['?' for _ in columns])})
        """
        data: List[List[str | datetime.datetime | int]] = [
            [export_date, pim_dataset_id] + row for row in csv_reader
        ]
        cursor.executemany(sql, data)
        inserted_row_count = cursor.rowcount
        conn.commit()
        # add the label
        if label and inserted_row_count > 0:
            db_add_label(
                conn, dataset_type="pim", dataset_id=pim_dataset_id, label=label
            )
    return inserted_row_count


def load_hybris_excel_to_db(
    conn: sqlite3.Connection,
    file_path: Path,
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
    label: str | None = None,
) -> int:
    """Load Excel file to database."""
    current_file_suffix: str = file_prefix(file_path)
    export_date: datetime.datetime = round_seconds(
        datetime.datetime.fromtimestamp(file_path.stat().st_ctime)
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
    if drop_table_first:
        db_drop_tables(conn, [current_file_suffix])
    db_create_table(
        conn, current_file_suffix, fields, unique_index_columns=unique_index_columns
    )
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
    conn.commit()
    # add the label
    if label and inserted_row_count > 0:
        db_add_label(conn, dataset_type="hybris", dataset_id=export_date, label=label)
    return inserted_row_count


def preprocess_doc_values(
    row: List[str | datetime.datetime | int | float | None],
    columns: List[str],
    required_columns: List[str],
) -> List[str | datetime.datetime | int | float | Missing]:
    """Preprocess the row to replace empty values with Missing."""
    new_row = []
    columns = [name.lower() for name in columns]
    x: str | datetime.datetime | int | float | None
    for i, x in enumerate(row):
        # print(f"Processing {columns[i]=} {x=}")
        if isinstance(x, str):
            x = x.strip()
            if "date" in columns[i].lower() and x:
                x = x.replace(".", "/")
                x = parse_doc_date(x)
                # print(f"PARSED DATE={x}")

        if not x and columns[i] in required_columns:
            new_row.append(Missing(columns[i]))
        else:
            new_row.append(x)
    return new_row


def parse_doc_date(date_str: str):
    """Parse the date string"""
    # date is in the format of dd.mm.yyyy
    # date_str = date_str.replace(".", "/")
    # print(f"{date_str=}")

    parsed_date: datetime.datetime = dateutil.parser.parse(date_str)

    return parsed_date


def load_docfile_into_db(
    conn: sqlite3.Connection,
    file_path: Path,
    prefix: str,
    request_date: datetime.datetime,
    analysis_name: str,
    config: Dict[str, Any],
    drop_table_first: bool = False,
    label: str | None = None,
) -> int:
    """Load doc Excel xlsx file into the database"""
    total_inserted_row_count: int = 0
    cursor: sqlite3.Cursor = conn.cursor()
    with file_path.open("rb") as f:
        in_memory_file = io.BytesIO(f.read())
    sheets: List[str] = config["doc"]["file_sheets"][prefix]
    workbook: openpyxl.Workbook = openpyxl.load_workbook(
        in_memory_file, read_only=True, data_only=True
    )
    # add the analysis to the analyses table if not present
    db_create_table(
        conn,
        "analyses",
        {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "request_date": "DATETIME NOT NULL",
            "analysis_name": "TEXT UNIQUE NOT NULL",
        },
        ["analysis_name"],
    )
    sql: str = "SELECT id FROM analyses WHERE analysis_name = ?"
    res = cursor.execute(sql, (analysis_name,)).fetchone()
    if not res:
        sql = "INSERT INTO analyses (request_date, analysis_name) VALUES (?, ?) RETURNING id"
        res = cursor.execute(sql, (request_date, analysis_name)).fetchone()
    analysis_id: int = res[0]

    for sh in workbook.worksheets:
        if sh.title.lower() not in sheets:
            continue
        # print(f"sheet name: {sh.title}")
        table_name: str = normalize_name(sh.title)
        config_header = list(config["doc"]["sheet_headers"][sh.title.lower()].keys())
        start_row_header = config["doc"]["start_rows"][sh.title.lower()]["header"]
        start_row_data = config["doc"]["start_rows"][sh.title.lower()]["data"]
        data: List[Any] = []
        empty_rows: int = 0
        for i, row in enumerate(sh.iter_rows(values_only=True), start=1):  # type: ignore
            # getting rid of extra columns from the end which are not
            # part of the inital template that the user might have added
            trimmed_row: Tuple[str | float | datetime.datetime | None, ...] = row[
                : len(config_header)
            ]
            if i == start_row_header:
                columns: List[str] = list(
                    ("request_date", "analysis_id", *trimmed_row, "file_name")
                )  # type: ignore
                columns_str: str = ", ".join([f"[{x}]" for x in columns])
                extra_fields: dict[str, str] = {
                    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                    "request_date": "DATETIME NOT NULL REFERENCES analyses('request_date')",
                    "analysis_id": "INTEGER NOT NULL REFERENCES analyses('id')",
                }
                required_columns: List[str] = [
                    k
                    for k, v in config["doc"]["sheet_headers"][sh.title.lower()].items()
                    if v == "required"
                ]
                # console.print(required_columns)
                unique_index_columns: List[str] = ["request_date"] + [
                    x for x in columns if x.lower() in required_columns
                ]
                fields_pass1: Dict[str, str] = {
                    k: "DATETIME" if "date" in k.lower() else "TEXT"
                    for k in columns[2:]
                }
                fields_pass2: Dict[str, str] = {
                    k: f"{v} NOT NULL" if k.lower() in required_columns else v
                    for k, v in fields_pass1.items()
                }
                fields: Dict[str, str] = {
                    **extra_fields,
                    **fields_pass2,
                }
                # console.print("fields = ")
                # console.print(fields)
                if drop_table_first:
                    db_drop_tables(conn, [table_name])
                # print(unique_index_columns)
                db_create_table(
                    conn,
                    table_name,
                    fields,
                    unique_index_columns=unique_index_columns,
                )
                sql = f"""
                    INSERT OR IGNORE INTO {table_name} ({columns_str})
                    VALUES ({','.join(['?' for _ in columns])})
                """
                # print(f"{i}:header: {row}")
            if i >= start_row_data:
                # print(f"{i}:data: {row}")
                if any(trimmed_row):
                    data_row_raw: List[int | float | str | datetime.datetime | None] = (
                        list((request_date, analysis_id, *trimmed_row, file_path.name))
                    )
                    assert len(data_row_raw) == len(columns)
                    data_row: List[str | datetime.datetime | int | float | Missing] = (
                        preprocess_doc_values(data_row_raw, columns, required_columns)
                    )
                    data.append(list(data_row))
                else:
                    empty_rows += 1
            if empty_rows >= 3:
                break
        cursor.executemany(sql, data)
        inserted_row_count: int = cursor.rowcount
        total_inserted_row_count += inserted_row_count
        conn.commit()
        print(
            f"Inserted {inserted_row_count} rows from sheet {sh.title} from file {file_path}\n"
        )
    # add the label
    if label and inserted_row_count > 0:
        db_add_label(conn, dataset_type="doc", dataset_id=analysis_id, label=label)
    return total_inserted_row_count


def load_doc_analysis_date_pair(
    conn: sqlite3.Connection,
    analysis_name: str,
    doc_request_date: datetime.datetime,
    pim_export_date: datetime.datetime,
    drop_tables: bool = False,
) -> int:
    """Add DoC analysis data pair."""
    table_name: str = "doc_analyses_date_pairs"
    if drop_tables:
        db_drop_tables(conn, [table_name])
    columns: Dict[str, str] = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "analysis_id": "INTEGER NOT NULL REFERENCES analyses('id')",
        "pim_export_date": "DATETIME NOT NULL",
    }
    column_names: List[str] = list(columns.keys())[1:]
    db_create_table(
        conn,
        table_name,
        columns=columns,
        unique_index_columns=[
            "analysis_name"
        ],  # , "pim_export_date", "doc_request_date"],
    )
    columns_str: str = ", ".join([f'"{k}"' for k in column_names])
    sql: str = f"""
        INSERT OR IGNORE INTO doc_analyses_date_pairs ({columns_str})
        VALUES ({','.join(['?' for _ in column_names])})
    """
    cursor: sqlite3.Cursor = conn.cursor()
    cursor.execute(sql, (analysis_name, doc_request_date, pim_export_date))
    inserted_row_count: int = cursor.rowcount
    conn.commit()
    return inserted_row_count


def main():
    """Main function."""
    print("This module doesn't do anything on it's own.")


if __name__ == "__main__":
    main()
