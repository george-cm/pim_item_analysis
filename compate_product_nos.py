"""Quic check if there are extra chars."""

import csv
from pathlib import Path


def main():
    """Main function."""
    csv_file = Path(
        "C:/Users/e313532/OneDrive - Honeywell/SPS/PIM/ePIM/Projects/MySPS Taxonomy - project 2/EIS Deliverables/data load files/pim_prod/products_availability_Static product list (1 products)_20240809120732_v10.csv"
    )

    with csv_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, dialect="excel")
        for line in reader:
            print(line["Product no."])
            prod_from_pim = line["Product no."]
            break
        prod_from_clipboard = '"T" Handle Adapter'
        prod_from_eis = '"T" Handle Adapter'

        for i, c in enumerate(prod_from_pim):
            print(
                f"{c} - {prod_from_clipboard[i]} - {prod_from_clipboard[i]}: {c==prod_from_clipboard[i]} - {c==prod_from_eis[i]} - {prod_from_eis[i]==prod_from_clipboard[i]}"
            )


if __name__ == "__main__":
    main()
