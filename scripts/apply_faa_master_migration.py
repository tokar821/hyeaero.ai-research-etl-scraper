#!/usr/bin/env python3
"""Apply database/migrations/ensure_faa_master.sql (creates faa_master table + indexes)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def main() -> int:
    etl = Path(__file__).resolve().parents[1]
    repo = etl.parent
    for p in (etl / ".env", repo / "backend" / ".env", repo / ".env"):
        if p.exists():
            load_dotenv(p, override=False)

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING") or os.getenv("DATABASE_URL")
    if not conn_str:
        print("Set POSTGRES_CONNECTION_STRING (or DATABASE_URL) in etl-pipeline/.env", file=sys.stderr)
        return 1

    sql_path = etl / "database" / "migrations" / "ensure_faa_master.sql"
    if not sql_path.is_file():
        print(f"Missing: {sql_path}", file=sys.stderr)
        return 1

    sql = sql_path.read_text(encoding="utf-8")
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    text = "\n".join(lines)
    statements = [s.strip() for s in text.split(";") if s.strip()]

    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt + ";")
    finally:
        conn.close()

    print("faa_master: migration applied OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
