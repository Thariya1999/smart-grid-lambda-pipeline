"""Postgres access layer for the FastAPI serving layer."""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "smartgrid")
PG_USER = os.environ.get("PG_USER", "smartgrid")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "smartgrid")


@contextmanager
def get_conn():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = query(sql, params)
    return rows[0] if rows else None
