"""Holds various views for the application."""

import datetime
from typing import List
from typing import Literal
from typing import Optional

from rich import box
from rich.console import Console
from rich.table import Table


def display_in_table(
    header: List[str],
    data: List[List[str | datetime.datetime | int]],
    column_justification: Optional[
        List[Literal["default", "left", "right", "center", "full"]]
    ] = None,
) -> None:
    """Display data in a table."""
    # table = Table(show_edge=False, show_lines=False, row_styles=["", "dim"])
    if column_justification is None:
        column_justification = ["left"] * len(header)  # type: ignore
    elif column_justification is None:
        column_justification = ["left"] * len(header)  # type: ignore
    elif len(header) > len(column_justification):
        column_justification = column_justification + ["left"] * (
            len(header) - len(column_justification)
        )  # type: ignore
    elif len(header) < len(column_justification):
        column_justification = column_justification[: len(header)]

    console = Console()
    table = Table(box=box.SIMPLE_HEAD)
    for column, justification in zip(header, column_justification):  # type: ignore
        table.add_column(column, justify=justification)
    for row in data:
        table.add_row(*[str(x) for x in row])
    if not table.rows:
        console.print("No data to display.")
    else:
        console.print(table)


def main():
    """Main function."""
    header = ["C1", "C2", "C3"]
    lst: List[List[str | datetime.datetime | int]] = [
        ["a", "b", "c"],
        ["d", "e", "f"],
        ["g", "h", "i"],
    ]
    display_in_table(header, lst)


if __name__ == "__main__":
    main()
