"""Holds various views for the application."""

from typing import List

from rich.console import Console
from rich.table import Table


def display_in_table(header: List[str], data: List[List[str]]) -> None:
    """Display data in a table."""
    table = Table()
    for column in header:
        table.add_column(column)
    for row in data:
        table.add_row(*row)
    console = Console()
    console.print(table)
