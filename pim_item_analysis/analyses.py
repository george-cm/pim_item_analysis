"""Module for analyses."""

import sqlite3


def doc_sku_analysis(conn: sqlite3.Connection, analysis_name: str) -> None:
    """DoC SKU analysis."""
    cursor: sqlite3.Cursor = conn.cursor()
    sql = """
        SELECT * FROM doc_analyses_date_pairs
    """
