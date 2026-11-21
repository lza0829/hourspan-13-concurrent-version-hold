import os
import sqlite3
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
FIXTURES_DIR = Path(
    os.environ.get("FIXTURES_DIR", Path(__file__).resolve().parent.parent.parent / "fixtures")
)
DB_PATH = DATA_DIR / "hourspan.db"


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def connect_immediate() -> sqlite3.Connection:
    """Connection for explicit BEGIN IMMEDIATE / COMMIT sections.

    Autocommit mode so a lease claim and the official hist write can be two
    short write transactions with no SQLite write lock held in between.
    """
    conn = connect()
    conn.isolation_level = None
    return conn
