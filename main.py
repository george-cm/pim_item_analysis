"""Import data from CSV files into the database"""

import argparse
import datetime
import sqlite3
import tomllib
from pathlib import Path
from typing import Any
from typing import List
from typing import Literal
from typing import Optional

from rich.console import Console

from pim_item_analysis.db import db_add_label
from pim_item_analysis.db import db_create_connection
from pim_item_analysis.db import db_create_label_tables
from pim_item_analysis.db import db_get_doc_datasets
from pim_item_analysis.db import db_get_hybris_datasets
from pim_item_analysis.db import db_get_label_for_date
from pim_item_analysis.db import db_get_pim_datasets
from pim_item_analysis.db import file_prefix
from pim_item_analysis.db import get_export_date_from_file
from pim_item_analysis.db import round_seconds
from pim_item_analysis.loaders import load_docfile_into_db
from pim_item_analysis.loaders import load_excel_to_db
from pim_item_analysis.loaders import load_file_into_db
from pim_item_analysis.views import display_in_table

console = Console()


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
    parser_load_pim_data(subparsers)
    parser_load_hybris_data(subparsers)
    parser_load_doc_data(subparsers)
    parser_list(subparsers)
    parser_add_label(subparsers)
    parser_doc(subparsers)

    args: argparse.Namespace = parser.parse_args()
    args.func(args)


def parser_load_pim_data(
    subparsers,  # pylint: disable=E1136:unsubscriptable-object
) -> None:
    """Create the parser for the load_pim_data command"""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "load_pim_data",
        help="Load PIM data from CSV files into the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Folder containing CSV files to load.",
    )
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser.add_argument(
        "--label",
        "-l",
        type=str,
        help="Add a label to the loaded data.",
        default=None,
    )
    parser.set_defaults(func=load_pim_data)


def parser_load_hybris_data(
    subparsers,  # pylint: disable=E1136:unsubscriptable-object
) -> None:
    """Create the parser for the load_hybris_data command"""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "load_hybris_data",
        help="Load data from a xlsx Excel file containing Hybris data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "hybris_file",
        type=str,
        help="Path to Hybris file. Should be named something like 'Sku status - dd.mm.xlsx'",
    )
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser.add_argument(
        "--label",
        "-l",
        type=str,
        help="Add a label to the loaded data.",
        default=None,
    )
    parser.set_defaults(func=load_hybris_data)


def parser_load_doc_data(subparsers) -> None:
    """Create the parser for the load_doc_data command"""

    parser: argparse.ArgumentParser = subparsers.add_parser(
        "load_doc_data",
        help="Load data from a folder containing Excel xlsx files with DoC"
        " data templates from the Quality department.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Folder containing DoC template Excel xlsx files.",
    )
    parser.add_argument(
        "request_date",
        type=datetime.date.fromisoformat,
        help="Date of the request. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser.add_argument(
        "--drop_tables",
        "-dt",
        action="store_true",
        help="Drop tables before loading data.",
        default=False,
    )
    parser.add_argument(
        "--label",
        "-l",
        type=str,
        help="Add a label to the loaded data.",
        default=None,
    )
    parser.set_defaults(func=load_doc_data)


def parser_list(subparsers) -> None:
    """Create the parser for the "list" command"""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "list",
        help="List the datasets in the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser.add_argument(
        "--descending",
        "-d",
        action="store_true",
        help="List in descending order.",
        default=False,
    )
    parser.set_defaults(func=list_data)


def parser_doc(subparsers) -> None:
    """Create the parser for the "list" command"""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "doc",
        help="DoC related commands.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    # parser.add_argument(
    #     "--descending",
    #     "-d",
    #     action="store_true",
    #     help="List in descending order.",
    #     default=False,
    # )
    parser.set_defaults(func=doc_analysis)


def parser_add_label(subparsers) -> None:
    """Create the parser for the "add_label" command"""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "add_label",
        help="Add a label to a dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "dataset_type",
        choices=["pim", "hybris", "doc"],
        help="Dataset type, pim or hybris.",
    )
    parser.add_argument(
        "dataset_number",
        type=int,
        help="Number of the dataset to add a label to, from the list command."
        " Overwrites any existing label.",
    )
    parser.add_argument("dataset_label", type=str, help="Label to add.")
    parser.add_argument(
        "--db_file",
        "-dbf",
        type=str,
        help="Database file.",
        default="pim_item_analysis.db",
    )
    parser.set_defaults(func=add_label)


def doc_analysis(args) -> None:
    """DoC related commands"""
    db_file: str = args.db_file
    with db_create_connection(db_file) as conn:
        sql: str = """
            SELECT
                CERTIFICATION_NUMBER,
                "MODULE NUMBER",
                LEGISLATION_TYPE,
                LEGISLATION_ID,
                -- replace(CERT_ISSUE_DATE,"T", " ") as CERT_ISSUE_DATE,
                -- replace(CERT_EXP_DATE, "T", " ") as CERT_EXP_DATE,
                CERT_ISSUE_DATE,
                CERT_EXP_DATE,
                CERT_STANDARD_LIST,
                NB_NUMBER,
                MNFR_CODE_NAME,
                PRODUCT_NAME_CERT,
                file_name
            FROM
                doc_cert_data_template
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for result in results:
            print(result)


def add_label(args) -> None:
    """Add a label to a dataset"""
    db_file: str = args.db_file
    dataset_number: int = args.dataset_number
    label: str = args.dataset_label
    dataset_type: Literal["pim", "hybris"] = args.dataset_type
    selected_date: datetime.datetime
    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        if dataset_type == "pim":
            selected_date = db_get_pim_datasets(conn)[dataset_number - 1][0]  # type: ignore
        elif dataset_type == "hybris":
            selected_date = db_get_hybris_datasets(conn)[dataset_number - 1][0]  # type: ignore
        else:
            raise ValueError(f"Unknown dataset type {dataset_type}")
        db_add_label(conn, dataset_type, selected_date, label)


def load_hybris_data(args) -> None:
    """Load data from a xlsx Excel file containing Hybris data"""
    db_file: str = args.db_file
    hybris_file: Path = Path(args.hybris_file)
    conn: sqlite3.Connection = db_create_connection(db_file)
    index_columns: List[str] = ["export_date", "Item no."]
    # print(f"{args.drop_tables=}")
    export_date: datetime.datetime = round_seconds(
        datetime.datetime.fromtimestamp(hybris_file.stat().st_ctime)
    )
    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        label: Optional[str] = args.label
        existing_label: Optional[str] = db_get_label_for_date(
            conn, "hybris", export_date
        )
        inserted_rows_count = load_excel_to_db(
            conn,
            hybris_file,
            drop_table_first=args.drop_tables,
            unique_index_columns=index_columns,
            label=label if existing_label != label else None,
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
        hybris_datasets: List[List[str | datetime.datetime | int]] = (
            db_get_hybris_datasets(conn)
        )
        doc_datasets: List[List[str | datetime.datetime | int]] = db_get_doc_datasets(
            conn
        )
        header: List[str] = [
            "Dataset\nNumber",
            "Export date string",
            "Export date",
            "Item count",
            "Label",
        ]

        print("\nPIM data")
        display_in_table(
            header,
            [
                [
                    i,
                    x[0].strftime("%Y-%m-%dT%H:%M:%S"),  # type: ignore
                    x[0].strftime("%Y-%m-%d %H:%M:%S"),  # type: ignore
                    x[1],
                    str(x[2] if x[2] else ""),
                ]
                for i, x in enumerate(pim_datasets, start=1)
            ],
            ["right", "left", "left", "right", "left"],
        )

        print("\nHybris data")
        display_in_table(
            header,
            [
                [
                    i,
                    x[0].strftime("%Y-%m-%dT%H:%M:%S"),  # type: ignore
                    x[0].strftime("%Y-%m-%d %H:%M:%S"),  # type: ignore
                    x[1],
                    str(x[2] if x[2] else ""),
                ]
                for i, x in enumerate(hybris_datasets, start=1)
            ],
            ["right", "left", "left", "right", "left"],
        )
        header_doc: List[str] = [
            "Dataset\nNumber",
            "Request date string",
            "Request date",
            "Record count",
            "Sheet Name",
            "File Name",
            "Label",
        ]
        print("\nDoC data")
        display_in_table(
            header_doc,
            [
                [
                    i,
                    x[0].strftime("%Y-%m-%dT%H:%M:%S"),  # type: ignore
                    x[0].strftime("%Y-%m-%d %H:%M:%S"),  # type: ignore
                    x[1],
                    x[2],
                    x[3],
                    str(x[4] if x[4] else ""),
                ]
                for i, x in enumerate(doc_datasets, start=1)
            ],
            ["right", "left", "left", "right", "left"],
        )


def load_pim_data(args) -> None:
    """Load data into the database"""
    current_dir = Path(__file__).parent
    print(f"Current directory: {current_dir}")
    db_file: str = args.db_file
    label: Optional[str] = args.label

    input_folder = Path(args.input_folder)

    index_columns: Optional[List[str]]
    label_set: bool = False

    with db_create_connection(db_file) as conn:
        db_create_label_tables(conn)
        for file_path in input_folder.glob("*.csv"):
            export_date: datetime.datetime = get_export_date_from_file(file_path)
            current_file_suffix: str = file_prefix(file_path)

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
            if not label_set:
                existing_label: Optional[str] = db_get_label_for_date(
                    conn, "pim", export_date
                )
                if existing_label == label:
                    label = None
                label_set = True
            else:
                label = None
            inserted_rows_count: int = load_file_into_db(
                conn,
                file_path,
                drop_table_first=args.drop_tables,
                unique_index_columns=index_columns,
                label=label,
            )
            print(f"Inserted {inserted_rows_count} rows from {file_path}\n")


def load_doc_data(args) -> None:
    """Load data into the database"""
    current_dir = Path(__file__).parent
    config_file = current_dir / "config.toml"
    with config_file.open("rb") as f:
        config: dict[str, Any] = tomllib.load(f)
    print(f"Current directory: {current_dir}")
    # print(f"{config=}")
    # console.print(config)

    db_file: str = args.db_file
    label: Optional[str] = args.label
    label_set: bool = False
    request_date: datetime.date = args.request_date

    input_folder = Path(args.input_folder)
    print(f"{input_folder=}")

    index_columns: Optional[List[str]] = None

    with db_create_connection(db_file) as conn:
        for file_path in input_folder.glob("*.xlsx"):
            # find out if the file is part of the DoC templates
            for prefix in config["doc"]["file_prefixes"]:
                if file_path.name.lower().startswith(prefix):
                    current_file_prefix = prefix
                    break
            console.print(f"{current_file_prefix=} - {file_path.name=}")
            if not label_set:
                existing_label: Optional[str] = db_get_label_for_date(
                    conn, "doc", request_date
                )
                if existing_label == label:
                    label = None
                label_set = True
            total_inserted_rows_count: int = load_docfile_into_db(
                conn,
                file_path,
                prefix=current_file_prefix,
                request_date=request_date,  # type: ignore
                config=config,
                drop_table_first=args.drop_tables,
                unique_index_columns=index_columns,
                label=label,
            )

            print(f"Total inserted {total_inserted_rows_count} rows from {file_path}\n")

            # if current_file_suffix == "item_availability":
            #     index_columns = ["export_date", "Item no."]
            # elif current_file_suffix == "item_classification":
            #     index_columns = [
            #         "export_date",
            #         "Item no.",
            #         "Structure.Identifier",
            #         "Structure group.Structure group identifier",
            #     ]
            # elif current_file_suffix == "item_pricing":
            #     index_columns = [
            #         "export_date",
            #         "Item no.",
            #         "Condition record no.",
            #     ]
            # elif current_file_suffix == "product_availability":
            #     index_columns = ["export_date", "Product no."]
            # elif current_file_suffix == "product_classification":
            #     index_columns = [
            #         "export_date",
            #         "Product no.",
            #         "Structure.Identifier",
            #         "Structure group.Structure group identifier",
            #     ]
            # else:
            #     index_columns = None

            # inserted_rows_count: int = load_file_into_db(
            #     conn,
            #     file_path,
            #     drop_table_first=args.drop_tables,
            #     unique_index_columns=index_columns,
            #     label=args.label,
            # )
            # print(f"Inserted {inserted_rows_count} rows from {file_path}\n")


if __name__ == "__main__":
    main()
