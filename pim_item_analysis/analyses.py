"""Module for analyses."""

import sqlite3
from typing import List


def doc_item_search_list(conn: sqlite3.Connection, analysis_name: str) -> List[str]:
    """DoC SKU analysis."""
    cursor: sqlite3.Cursor = conn.cursor()
    sql = """
        WITH items(ITEM_CODE) AS (
        SELECT
                DISTINCT
                        dsdt.ITEM_CODE
        FROM
                doc_sku_data_template dsdt
        LEFT JOIN (
            SELECT
                    a.id
            FROM
                    analyses a
            WHERE
                    a.analysis_name = ?) AS b
            ON
                dsdt.analysis_id = b.id
        )
        SELECT
                ITEM_CODE AS "Item no."
        FROM
            items
        UNION ALL
            SELECT
                (prefix || ITEM_CODE) as "Item no."
        FROM
                items i
        INNER JOIN (
            SELECT
                    "BRP900~" AS "prefix")
        UNION ALL
            SELECT
                (prefix || ITEM_CODE) as "Item no."
        FROM
                items i
        INNER JOIN (
            SELECT
                    "PRD010~" AS "prefix")
    """
    res = cursor.execute(sql, (analysis_name,)).fetchall()
    item_search_list: List[str] = [x[0] for x in res]
    return item_search_list
