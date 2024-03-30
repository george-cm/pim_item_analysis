# test_import_data.py

import sqlite3
import pytest
from pim_item_analysis.import_data import (
    db_create_connection,
    db_drop_tables,
    db_trim_column_in_table,
    db_add_column,
    db_create_tables,
)


@pytest.fixture
def db_file(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def conn(db_file):
    return db_create_connection(db_file)


def test_db_create_connection(db_file):
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


def test_db_create_tables(conn):
    db_create_tables(conn)
    # Verify item_availability table was created
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    assert ("item_availability",) in tables

    # Verify columns
    cur.execute("PRAGMA table_info(item_availability);")
    columns = cur.fetchall()
    assert len(columns) == 34

    # Spot check some columns
    assert columns[0][1] == "ID"
    assert columns[1][1] == "EXPORT_DATE"
    assert columns[2][1] == "Status"
