"""CLI Module"""

import datetime

from pim_item_analysis.db import db_create_connection
from pim_item_analysis.db import db_create_table
from pim_item_analysis.loaders import add_doc_analysis_date_pair_loader


def main():
    """This module does nothing on it's own"""
    pass


def add_doc_analysis_date_pairs(args) -> None:
    """Add DoC Analysis date pairs"""
    db_file: str = args.db_file
    name: str = args.name
    doc_request_date: datetime.datetime = args.doc_request_date
    pim_export_date: datetime.datetime = args.pim_export_date
    inserted_rows: int = add_doc_analysis_date_pair_loader(db_file, name, doc_request_date, pim_export_date)


if __name__ == "__main__":
    main()
