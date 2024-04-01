"""Import data from CSV files into the database"""

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Optional

from pim_item_analysis.db import db_create_connection
from pim_item_analysis.db import db_create_table
from pim_item_analysis.db import db_drop_tables
from pim_item_analysis.db import file_suffix
from pim_item_analysis.db import get_export_date_from_file
from pim_item_analysis.db import register_adapters_and_converters

register_adapters_and_converters()


def main() -> None:
    """Main function"""
    parser = argparse.ArgumentParser(
        prog="pim_item_analysis",
        description="Analyze PIM item data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser] = (  # pylint: disable=E1136:unsubscriptable-object
        parser.add_subparsers(
            title="subcommands",
            description="valid subcommands",
            help="additional help",
            required=True,
        )
    )

    # create the parser for the "load_data" command
    parser_load_data: argparse.ArgumentParser = subparsers.add_parser(
        "load_data",
        help="Load data from a folder containing CSV files of PIM item data into the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_load_data.add_argument(
        "input_folder", type=str, help="Folder containing CSV files."
    )
    parser_load_data.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser_load_data.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser_load_data.set_defaults(func=load_data)

    # create the parser for the "list" command
    parser_list: argparse.ArgumentParser = subparsers.add_parser(
        "list",
        help="List the datasets in the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_list.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser_list.add_argument(
        "--descending",
        "-d",
        action="store_true",
        help="List in descending order.",
        default=False,
    )
    parser_list.set_defaults(func=list_data)
    args: argparse.Namespace = parser.parse_args()
    args.func(args)


def list_data(args) -> None:
    """List the datasets in the database"""
    db_file: str = args.db_file
    conn: sqlite3.Connection = db_create_connection(db_file)
    with db_create_connection(db_file) as conn:
        cursor: sqlite3.Cursor = conn.cursor()
        result: sqlite3.Cursor = cursor.execute(f"""
            SELECT DISTINCT [export_date], count([Item no.]) AS [Item count]
            FROM item_availability
            GROUP BY export_date
            ORDER BY export_date {'DESC' if args.descending else 'ASC'}
        """)
        print(f"\nExport date{' ' * (19 - len('Export date'))}\tItem count")
        print(f"{'-' * 19}\t----------")
        for row in result:
            print(f"{row[0].strftime("%Y/%m/%d %H:%M:%S")}\t{row[1]}")


def load_data(args) -> None:
    """Load data into the database"""
    current_dir = Path(__file__).parent
    print(f"Current directory: {current_dir}")
    # db_file: str = "pim_item_analysis.db"
    db_file: str = args.db_file

    input_folder = Path(args.input_folder)

    index_columns: Optional[List[str]]

    conn: sqlite3.Connection = db_create_connection(db_file)
    with db_create_connection(db_file) as conn:
        for file_path in input_folder.glob("*.csv"):
            current_file_suffix: str = file_suffix(file_path)

            if current_file_suffix == "item_availability":
                index_columns = ["export_date", "Item no."]
            elif current_file_suffix == "item_classification":
                index_columns = [
                    "export_date",
                    "Item no.",
                    "Structure.Identifier",
                    "Structure group.Structure group identifier",
                ]
            elif current_file_suffix == "item_pricing":
                index_columns = [
                    "export_date",
                    "Item no.",
                    "Condition record no.",
                ]
            elif current_file_suffix == "product_availability":
                index_columns = ["export_date", "Product no."]
            elif current_file_suffix == "product_classification":
                index_columns = [
                    "export_date",
                    "Product no.",
                    "Structure.Identifier",
                    "Structure group.Structure group identifier",
                ]
            else:
                index_columns = None

            inserted_rows_count: int = load_file_into_db(
                conn,
                file_path,
                drop_table_first=args.drop_tables,
                unique_index_columns=index_columns,
            )
            print(f"Inserted {inserted_rows_count} rows from {file_path}\n")
            # print(f"============= Rows from {current_file_suffix} =============")
            # for row in conn.execute(f"SELECT * FROM {current_file_suffix}").fetchall():
            #     print(row)
            # print(f"============= End {current_file_suffix} =============\n")


def load_file_into_db(
    conn: sqlite3.Connection,
    file_path: Path,
    drop_table_first: bool = False,
    unique_index_columns: Optional[List[str]] = None,
) -> int:
    """Load file into the database"""
    current_file_suffix: str = file_suffix(file_path)
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
        fields: dict[str, str] = {**extra_fields, **{k: "TEXT" for k in header}}
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

        conn.commit()
        return inserted_row_count


if __name__ == "__main__":
    main()
