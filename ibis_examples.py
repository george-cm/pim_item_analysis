"""ibis playground"""

import datetime
from pathlib import Path
from typing import Any

import ibis
from ibis.expr.types.relations import Table


def main():
    """Main function."""
    # con = ibis.connect(
    #     "sqlite://C:/Users/e313532/Documents/Projects/pim_item_analysis/pim_item_analysis.db"
    # )
    con: ibis.BaseBackend = ibis.connect(
        "sqlite://C:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - General/Projects"
        "/DoC Documentation - Hungarian DOC 13728358/DoC Documentation - Hungarian DOC 13728358.db"
    )
    print(con.list_tables())

    doc_cert: Table = con.table("doc_cert_data_template")
    doc_sku: Table = con.table("doc_sku_data_template")
    skus: Table = doc_sku["ITEM_CODE"]
    print("; ".join(skus.to_pandas().to_list()))  # type: ignore

    item_av: Table = con.table("item_availability")
    t1: Table = doc_sku.rename(dict(sku_id="id"))
    t1 = t1[t1["request_date"] == datetime.datetime(2024, 4, 30).date().isoformat()]
    t2: Table = item_av["SKU", "Item no.", "export_date"]
    t2 = t2[
        t2["export_date"]
        # == datetime.datetime(2024, 4, 30, 17, 40, 13).strftime("%Y-%m-%d %H:%M:%S")
        == datetime.datetime(2024, 4, 30, 17, 40, 13).isoformat()
        # == "2024-04-30T17:40:13"
    ]
    sku_items: Table = t1.join(
        t2,
        t1["ITEM_CODE"] == t2["SKU"],  # type: ignore
        how="left",
        lname="left_{name}",
    )
    print(ibis.to_sql(sku_items))
    skus_to_upload = sku_items.mutate([**{"ITEM_CODE": sku_items["Item no."]}]).drop(
        "Item no.", "export_date", "request_date", "file_name", "sku_id"
    )
    print()
    print(skus_to_upload.to_pandas())
    import_fld = Path(
        "c:/Users/e313532/Honeywell/PUBLIC SPS PIM Data Management - General/Projects/DoC Documentation - Hungarian DOC 13728358/import"
    )
    skus_to_upload.to_csv(import_fld / "upload_DoC SKU Data.csv")


if __name__ == "__main__":
    # import timeit
    # from datetime import timedelta

    # print(str(timedelta(seconds=timeit.timeit(main, number=1))))
    main()
