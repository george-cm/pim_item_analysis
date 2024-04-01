"""test_main.py"""

# pylint: disable=W0621:redefined-outer-name,C0116:missing-function-docstring

from pathlib import Path
import sqlite3
import pytest
from pim_item_analysis.db import (
    db_create_connection,
    db_drop_tables,
    db_trim_column_in_table,
    db_add_column,
    db_create_table,
)


@pytest.fixture
def db_file(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def conn(db_file: Path) -> sqlite3.Connection:
    return db_create_connection(db_file)


def test_db_create_connection(db_file: Path) -> None:
    conn = db_create_connection(db_file)
    assert isinstance(conn, sqlite3.Connection)


def test_db_drop_tables(conn):
    db_drop_tables(conn, ["table1", "table2"])

    # Verify tables were dropped
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    assert len(tables) == 0


def test_db_trim_column(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (col1 text)")
    cur.execute("INSERT INTO test VALUES ('   abc   ')")

    db_trim_column_in_table(conn, "test", "col1")

    cur.execute("SELECT * FROM test")
    rec = cur.fetchone()
    assert rec[0] == "abc"


def test_db_add_column(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (col1 text)")

    db_add_column(conn, "test", "col2", "integer")

    cur.execute("PRAGMA table_info(test)")
    info = cur.fetchall()
    assert len(info) == 2
    assert info[1][1] == "col2"
    assert info[1][2] == "INTEGER"


def test_db_create_table(conn):
    columns = {"id": "integer primary key", "name": "text", "count": "integer"}
    db_create_table(conn, "test", columns)

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(test)")

    info = cur.fetchall()
    assert len(info) == 3

    assert info[0][1] == "id"
    assert info[0][2].lower() == "integer"

    assert info[1][1] == "name"
    assert info[1][2].lower() == "text"

    assert info[2][1] == "count"
    assert info[2][2].lower() == "integer"
