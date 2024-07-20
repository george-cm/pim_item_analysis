"""Module for analyses."""

import sqlite3
from typing import List


def doc_item_search_list(conn: sqlite3.Connection, request_date: str) -> List[str]:
    """DoC SKU analysis."""
    cursor: sqlite3.Cursor = conn.cursor()
    sql = """
        WITH items(ITEM_CODE) AS (
        SELECT DISTINCT
            dsdt.ITEM_CODE
        FROM
            doc_sku_data_template dsdt
        WHERE dsdt.request_date = ?
/*        LEFT JOIN
        (
            SELECT
                dadp.doc_request_date
            FROM
                doc_analyses_date_pairs dadp
            WHERE
                dadp.analysis_name = ?) AS b
        ON
            dsdt.request_date = b.doc_request_date */
            )
        SELECT ITEM_CODE AS "Item no." FROM items
        UNION ALL
        SELECT
            (prefix || ITEM_CODE) as "Item no."
        FROM
            items i
        INNER JOIN (SELECT "BRP900~" AS "prefix")
        UNION ALL
        SELECT
            (prefix || ITEM_CODE) as "Item no."
        FROM
            items i
        INNER JOIN (SELECT "PRD010~" AS "prefix")
    """
    res = cursor.execute(sql, (request_date,)).fetchall()
    item_search_list: List[str] = [x[0] for x in res]
    return item_search_list
