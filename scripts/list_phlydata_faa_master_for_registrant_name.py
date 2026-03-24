#!/usr/bin/env python3
"""
List **PhlyData aircraft** rows that join to **faa_master** where ``registrant_name`` matches a string.

Uses the same **canonical N-number** join as ``list_phlydata_faa_master_registration_matches.py``.
Default name filter: **KENMORE CREW LEASING INC TRUSTEE** (case-insensitive substring).

Environment: ``POSTGRES_CONNECTION_STRING`` or ``DATABASE_URL`` (``etl-pipeline/.env``).

Usage::

    cd D:\\HyeAero\\etl-pipeline
    python scripts/list_phlydata_faa_master_for_registrant_name.py

    python scripts/list_phlydata_faa_master_for_registrant_name.py --registrant "KENMORE CREW LEASING" --out kenmore_matches.csv

    python scripts/list_phlydata_faa_master_for_registrant_name.py --require-serial --exact-name
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_CANON_EXPR = r"""
    NULLIF(
      REGEXP_REPLACE(
        REPLACE(REPLACE(UPPER(TRIM(COALESCE({col}, ''))), ' ', ''), '-', ''),
        '^N', '', 'i'
      ),
      ''
    )
"""

_SERIAL_NORM = r"""
REPLACE(REPLACE(REPLACE(TRIM(UPPER(COALESCE({col}, ''))), ' ', ''), '-', ''), '.', '')
"""

_SERIAL_MATCH = """(
    ({phly_sn}) = ({fm_sn})
    OR (
      {phly_sn} <> '' AND {fm_sn} <> ''
      AND {phly_sn} ~ '^[0-9]+$' AND {fm_sn} ~ '^[0-9]+$'
      AND {phly_sn}::bigint = {fm_sn}::bigint
    )
  )"""

LIST_SQL = """
WITH phly AS (
  SELECT
    p.aircraft_id,
    p.serial_number AS phly_serial,
    p.registration_number AS phly_registration,
    p.manufacturer,
    p.model,
    p.manufacturer_year,
    p.delivery_year,
    p.category,
    {phly_canon} AS n_canon
  FROM public.{table} p
  WHERE p.registration_number IS NOT NULL
    AND BTRIM(p.registration_number::text) <> ''
),
fm AS (
  SELECT
    fm.n_number,
    fm.serial_number AS faa_serial,
    fm.registrant_name,
    fm.street,
    fm.city,
    fm.state,
    fm.zip_code,
    fm.country,
    fm.ingestion_date,
    {fm_canon} AS n_canon
  FROM public.faa_master fm
  WHERE {name_predicate}
)
SELECT
  phly.aircraft_id,
  phly.phly_registration,
  phly.phly_serial,
  phly.manufacturer,
  phly.model,
  phly.manufacturer_year,
  phly.delivery_year,
  phly.category,
  fm.n_number AS faa_n_number,
  fm.faa_serial,
  fm.registrant_name AS faa_registrant_name,
  fm.street AS faa_street,
  fm.city AS faa_city,
  fm.state AS faa_state,
  fm.zip_code AS faa_zip,
  fm.country AS faa_country,
  fm.ingestion_date AS faa_ingestion_date
FROM phly
INNER JOIN fm ON fm.n_canon = phly.n_canon
WHERE
  CASE WHEN %(require_serial)s::boolean THEN
    (phly.phly_serial IS NULL OR BTRIM(phly.phly_serial::text) = '')
    OR {serial_match}
  ELSE TRUE
  END
ORDER BY phly.phly_registration, phly.aircraft_id, fm.ingestion_date DESC NULLS LAST;
"""


def _load_env() -> None:
    etl = Path(__file__).resolve().parents[1]
    repo = etl.parent
    for p in (etl / ".env", repo / "backend" / ".env", repo / ".env"):
        if p.is_file():
            load_dotenv(p, override=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--registrant",
        default="KENMORE CREW LEASING INC TRUSTEE",
        help="Registrant name filter (default: KENMORE CREW LEASING INC TRUSTEE).",
    )
    parser.add_argument(
        "--exact-name",
        action="store_true",
        help="Match registrant_name with trim equality (case-insensitive) instead of ILIKE %%…%%.",
    )
    parser.add_argument(
        "--table",
        default="phlydata_aircraft",
        help="PhlyData table (default: phlydata_aircraft).",
    )
    parser.add_argument(
        "--require-serial",
        action="store_true",
        help="Require faa_master serial to match PhlyData serial when serial is set.",
    )
    parser.add_argument("--out", default="", help="Write CSV here; default stdout.")
    args = parser.parse_args()

    table = args.table.replace('"', "").replace(";", "").replace(" ", "")
    phly_canon = _CANON_EXPR.format(col="p.registration_number")
    fm_canon = _CANON_EXPR.format(col="fm.n_number")
    phly_sn = _SERIAL_NORM.format(col="phly.phly_serial")
    fm_sn = _SERIAL_NORM.format(col="fm.faa_serial")
    serial_match = _SERIAL_MATCH.format(phly_sn=phly_sn, fm_sn=fm_sn)

    if args.exact_name:
        name_predicate = "UPPER(BTRIM(COALESCE(fm.registrant_name, ''))) = UPPER(BTRIM(%(reg_exact)s))"
        params_extra = {"reg_exact": (args.registrant or "").strip()}
    else:
        # Substring match without ILIKE wildcards (safe for arbitrary text)
        raw = (args.registrant or "").strip().lower()
        name_predicate = (
            "POSITION(%(reg_sub)s IN LOWER(COALESCE(fm.registrant_name, ''))) > 0"
        )
        params_extra = {"reg_sub": raw}

    sql = LIST_SQL.format(
        table=table,
        phly_canon=phly_canon,
        fm_canon=fm_canon,
        name_predicate=name_predicate,
        serial_match=serial_match,
    )
    params = {"require_serial": args.require_serial, **params_extra}

    _load_env()
    conn_str = os.getenv("POSTGRES_CONNECTION_STRING") or os.getenv("DATABASE_URL")
    if not conn_str:
        print("ERROR: POSTGRES_CONNECTION_STRING or DATABASE_URL not set.", file=sys.stderr)
        return 1

    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description]
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        return 1

    print(f"Rows: {len(rows):,}", file=sys.stderr)
    if not rows:
        return 0

    out_path = (args.out or "").strip()
    if out_path:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(rows)
        print(f"Wrote {out_path}", file=sys.stderr)
    else:
        w = csv.writer(sys.stdout, lineterminator="\n")
        w.writerow(colnames)
        w.writerows(rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
