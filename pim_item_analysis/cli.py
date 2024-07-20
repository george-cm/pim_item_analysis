"""CLI Module"""

import datetime

from pim_item_analysis.db import db_create_connection

# from pim_item_analysis.db import db_create_table
from pim_item_analysis.loaders import load_doc_analysis_date_pair


def main():
    """This module does nothing on it's own"""


def add_doc_analysis_date_pairs(args) -> None:
    """Add DoC Analysis date pairs"""
    db_file: str = args.db_file
    analysis_name: str = args.analysis_name
    drop_tables = args.drop_tables
    doc_request_date: datetime.datetime = args.doc_request_date
    pim_export_date: datetime.datetime = args.pim_export_date
    with db_create_connection(db_file) as conn:
        inserted_rows: int = load_doc_analysis_date_pair(
            conn, analysis_name, doc_request_date, pim_export_date, drop_tables
        )
        print(f"Inserted {inserted_rows} rows")


if __name__ == "__main__":
    main()
