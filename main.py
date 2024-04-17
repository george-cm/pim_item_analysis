"""Import data from CSV files into the database"""

import argparse
import datetime
import sqlite3
from pathlib import Path
from typing import List
from typing import Optional

from pim_item_analysis.db import db_add_label
from pim_item_analysis.db import db_create_connection
from pim_item_analysis.db import db_create_label_tables
from pim_item_analysis.db import db_get_hybris_datasets
from pim_item_analysis.db import db_get_pim_datasets
from pim_item_analysis.db import file_suffix

# from pim_item_analysis.db import register_adapters_and_converters
from pim_item_analysis.loaders import load_excel_to_db
from pim_item_analysis.loaders import load_file_into_db

# from pim_item_analysis.views import display_in_table

# register_adapters_and_converters()


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
    parser_load_pim_data: argparse.ArgumentParser = subparsers.add_parser(
        "load_pim_data",
        help="Load data from a folder containing CSV files of PIM item data into the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_load_pim_data.add_argument(
        "input_folder", type=str, help="Folder containing CSV files."
    )
    parser_load_pim_data.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser_load_pim_data.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser_load_pim_data.add_argument(
        "--label",
        "-l",
        type=str,
        help="Add a label to the loaded data.",
        default=None,
    )
    parser_load_pim_data.set_defaults(func=load_pim_data)

    # create the parser for the load_hybris_data command
    parser_load_hybris_data: argparse.ArgumentParser = subparsers.add_parser(
        "load_hybris_data",
        help="Load data from a xlsx Excel file containing Hybris data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_load_hybris_data.add_argument(
        "hybris_file",
        type=str,
        help="Path to Hybris file. Should be named something like 'Sku status - dd.mm.xlsx'",
    )
    parser_load_hybris_data.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser_load_hybris_data.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser_load_hybris_data.set_defaults(func=load_hybris_data)

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

    # create the parser for adding a label to a dataset
    parser_add_label: argparse.ArgumentParser = subparsers.add_parser(
        "add_label",
        help="Add a label to a dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_add_label.add_argument(
        "dataset_type", choices=["pim", "hybris"], help="Dataset type, pim or hybris."
    )
    parser_add_label.add_argument(
        "dataset_number",
        type=int,
        help="Number of the dataset to add a label to, from the list command."
        " Overwrites any existing label.",
    )
    parser_add_label.add_argument("dataset_label", type=str, help="Label to add.")
    parser_add_label.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser_add_label.set_defaults(func=add_label)

    args: argparse.Namespace = parser.parse_args()
    args.func(args)


def add_label(args) -> None:
    """Add a label to a dataset"""
    db_file: str = args.db_file
    dataset_number: int = args.dataset_number
    label: str = args.dataset_label
    dataset_type: str = args.dataset_type
    selected_date: datetime.datetime
    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        if dataset_type == "pim":
            selected_date = db_get_pim_datasets(conn)[dataset_number - 1][0]  # type: ignore
        elif dataset_type == "hybris":
            selected_date = db_get_hybris_datasets(conn)[dataset_number - 1][0]  # type: ignore
        else:
            raise ValueError(f"Unknown dataset type {dataset_type}")
        db_add_label(conn, db_file, dataset_type, selected_date, label)


def load_hybris_data(args) -> None:
    """Load data from a xlsx Excel file containing Hybris data"""
    db_file: str = args.db_file
    hybris_file: Path = Path(args.hybris_file)
    conn: sqlite3.Connection = db_create_connection(db_file)
    index_columns: List[str] = ["export_date", "Item no."]
    # print(f"{args.drop_tables=}")
    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        inserted_rows_count = load_excel_to_db(
            conn,
            hybris_file,
            drop_table_first=args.drop_tables,
            unique_index_columns=index_columns,
        )
        print(f"Inserted {inserted_rows_count} rows from {hybris_file}\n")


def list_data(args) -> None:
    """List the datasets in the database"""
    db_file: str = args.db_file
    conn: sqlite3.Connection = db_create_connection(db_file)
    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        pim_datasets: List[List[str | datetime.datetime | int]] = db_get_pim_datasets(
            conn
        )
        print("\nPIM data")
        print("=" * len("PIM data"))
        print(
            "\nDataset\nNumber\t"
            "Export date string\t"
            f"Export date{' ' * (19 - len('Export date'))}\t"
            "Item count\t"
            "Label"
        )
        print(f"{'':->7}\t{'':->19}\t{'':->19}\t{'':->10}\t{'':->32}")
        for i, row in enumerate(pim_datasets, start=1):
            label: str = str(row[2] if row[2] else "")
            print(
                f"{i: >7}\t{row[0].strftime("%Y-%m-%dT%H:%M:%S")}\t"  # type: ignore
                f"{row[0].strftime("%Y/%m/%d %H:%M:%S")}\t"  # type: ignore
                f"{row[1]:>10}\t"  # type: ignore
                f"{label}"
            )
        hybris_datasets: List[List[str | datetime.datetime | int]] = (
            db_get_hybris_datasets(conn)
        )
        print("\nHybris data")
        print("=" * len("Hybris data"))
        print(
            "\nDataset\nNumber\t"
            "Export date string\t"
            f"Export date{' ' * (19 - len('Export date'))}\t"
            "Item count\t"
            "Label"
        )
        print(f"{'':->7}\t{'':->19}\t{'':->19}\t{'':->10}\t{'':->32}")
        for i, row in enumerate(hybris_datasets, start=1):
            label = str(row[2] if row[2] else "")
            print(
                f"{i: >7}\t{row[0].strftime("%Y-%m-%dT%H:%M:%S")}\t"  # type: ignore
                f"{row[0].strftime("%Y/%m/%d %H:%M:%S")}\t"  # type: ignore
                f"{row[1]:>10}\t"  # type: ignore
                f"{label}"
            )


def load_pim_data(args) -> None:
    """Load data into the database"""
    current_dir = Path(__file__).parent
    print(f"Current directory: {current_dir}")
    # db_file: str = "pim_item_analysis.db"
    db_file: str = args.db_file

    input_folder = Path(args.input_folder)

    index_columns: Optional[List[str]]

    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
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
                label=args.label,
            )
            print(f"Inserted {inserted_rows_count} rows from {file_path}\n")
            # print(f"============= Rows from {current_file_suffix} =============")
            # for row in conn.execute(f"SELECT * FROM {current_file_suffix}").fetchall():
            #     print(row)
            # print(f"============= End {current_file_suffix} =============\n")


if __name__ == "__main__":
    main()
