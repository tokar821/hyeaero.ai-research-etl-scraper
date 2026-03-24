#!/usr/bin/env python3
"""
List **registration numbers** (tails / N-numbers) from PhlyData that also appear in ``faa_master``.

Uses the same **canonical N-number** idea as the backend (``faa_master_lookup.registration_tail_canonical`` /
SQL ``REGEXP_REPLACE(TRIM(UPPER(...)), '^N', '')``): ``N277G`` and ``277G`` align.

**Matching**

- **Default**: PhlyData row matches if canonical tail equals canonical ``faa_master.n_number``.

- **``--require-serial``**: when PhlyData has a non-empty ``serial_number``, require an
  ``faa_master`` row whose serial matches after normalizing spaces/hyphens/dots; for **all-digit**
  serials, **numeric** equality is also accepted (``000102`` vs ``102``).

- **``--list-phly-registrations``**: print **one column** — distinct PhlyData ``registration_number``
  values that match (sorted). Good for quick counts / piping.

Environment: ``POSTGRES_CONNECTION_STRING`` or ``DATABASE_URL`` (``etl-pipeline/.env``).

Usage::

    cd D:\\HyeAero\\etl-pipeline
    python scripts/list_phlydata_faa_master_registration_matches.py --stats

    python scripts/list_phlydata_faa_master_registration_matches.py --list-phly-registrations > tails.txt

    python scripts/list_phlydata_faa_master_registration_matches.py --distinct-registrations-only --out pairs.csv

    python scripts/list_phlydata_faa_master_registration_matches.py --require-serial --out matches.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Canonical N-number: align with backend services.faa_master_lookup (strip spaces/hyphens, leading N).
_CANON_EXPR = r"""
    NULLIF(
      REGEXP_REPLACE(
        REPLACE(REPLACE(UPPER(TRIM(COALESCE({col}, ''))), ' ', ''), '-', ''),
        '^N', '', 'i'
      ),
      ''
    )
"""

# Serial compare: align with ``faa_master_lookup._FM_SERIAL_NORM`` (trim, upper, strip space, hyphen, dot).
_SERIAL_NORM = r"""
REPLACE(REPLACE(REPLACE(TRIM(UPPER(COALESCE({col}, ''))), ' ', ''), '-', ''), '.', '')
"""

# When --require-serial: exact normalized match OR both numeric-only and same integer value.
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
    {phly_canon} AS n_canon
  FROM public.{table} p
  WHERE p.registration_number IS NOT NULL
    AND BTRIM(p.registration_number::text) <> ''
),
fm AS (
  SELECT
    fm.n_number,
    fm.serial_number AS faa_serial,
    fm.ingestion_date,
    fm.registrant_name,
    {fm_canon} AS n_canon
  FROM public.faa_master fm
)
SELECT
  phly.aircraft_id,
  phly.phly_registration,
  phly.phly_serial,
  phly.manufacturer,
  phly.model,
  fm.n_number AS faa_n_number,
  fm.faa_serial,
  fm.ingestion_date AS faa_ingestion_date,
  fm.registrant_name AS faa_registrant_name
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

DISTINCT_REG_SQL = """
WITH phly AS (
  SELECT
    p.aircraft_id,
    p.serial_number AS phly_serial,
    p.registration_number AS phly_registration,
    {phly_canon} AS n_canon
  FROM public.{table} p
  WHERE p.registration_number IS NOT NULL
    AND BTRIM(p.registration_number::text) <> ''
),
fm AS (
  SELECT
    fm.n_number,
    fm.serial_number AS faa_serial,
    {fm_canon} AS n_canon
  FROM public.faa_master fm
),
joined AS (
  SELECT phly.phly_registration, fm.n_number AS faa_n_number
  FROM phly
  INNER JOIN fm ON fm.n_canon = phly.n_canon
  WHERE
    CASE WHEN %(require_serial)s::boolean THEN
      (phly.phly_serial IS NULL OR BTRIM(phly.phly_serial::text) = '')
      OR {serial_match}
    ELSE TRUE
    END
)
SELECT DISTINCT phly_registration, faa_n_number
FROM joined
ORDER BY phly_registration, faa_n_number;
"""

PHLY_REGISTRATIONS_LIST_SQL = """
WITH phly AS (
  SELECT
    p.aircraft_id,
    p.serial_number AS phly_serial,
    p.registration_number AS phly_registration,
    {phly_canon} AS n_canon
  FROM public.{table} p
  WHERE p.registration_number IS NOT NULL
    AND BTRIM(p.registration_number::text) <> ''
),
fm AS (
  SELECT
    fm.n_number,
    fm.serial_number AS faa_serial,
    {fm_canon} AS n_canon
  FROM public.faa_master fm
),
joined AS (
  SELECT phly.phly_registration
  FROM phly
  INNER JOIN fm ON fm.n_canon = phly.n_canon
  WHERE
    CASE WHEN %(require_serial)s::boolean THEN
      (phly.phly_serial IS NULL OR BTRIM(phly.phly_serial::text) = '')
      OR {serial_match}
    ELSE TRUE
    END
)
SELECT DISTINCT phly_registration
FROM joined
ORDER BY phly_registration;
"""

STATS_SQL = """
WITH phly AS (
  SELECT
    p.aircraft_id,
    p.serial_number AS phly_serial,
    p.registration_number AS phly_registration,
    {phly_canon} AS n_canon
  FROM public.{table} p
  WHERE p.registration_number IS NOT NULL
    AND BTRIM(p.registration_number::text) <> ''
),
fm AS (
  SELECT
    fm.n_number,
    fm.serial_number AS faa_serial,
    {fm_canon} AS n_canon
  FROM public.faa_master fm
),
joined AS (
  SELECT phly.aircraft_id, phly.phly_registration, phly.n_canon, fm.n_number AS faa_n_number
  FROM phly
  INNER JOIN fm ON fm.n_canon = phly.n_canon
  WHERE
    CASE WHEN %(require_serial)s::boolean THEN
      (phly.phly_serial IS NULL OR BTRIM(phly.phly_serial::text) = '')
      OR {serial_match}
    ELSE TRUE
    END
)
SELECT
  COUNT(*)::bigint AS join_rows,
  COUNT(DISTINCT aircraft_id)::bigint AS distinct_phly_aircraft,
  COUNT(DISTINCT phly_registration)::bigint AS distinct_phly_registration_display,
  COUNT(DISTINCT n_canon)::bigint AS distinct_canonical_n,
  COUNT(DISTINCT faa_n_number)::bigint AS distinct_faa_n_number_values
FROM joined;
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
        "--table",
        default="phlydata_aircraft",
        help="PhlyData aircraft table (default: phlydata_aircraft).",
    )
    parser.add_argument(
        "--require-serial",
        action="store_true",
        help="When PhlyData serial is set, require faa_master serial match (normalized; digit serials also match by numeric value).",
    )
    parser.add_argument(
        "--list-phly-registrations",
        action="store_true",
        help="Distinct PhlyData registration numbers that match faa_master (sorted). Plain lines to stdout; CSV if --out.",
    )
    parser.add_argument(
        "--distinct-registrations-only",
        action="store_true",
        help="Output only unique phly_registration / faa_n_number pairs (no aircraft_id).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="",
        help="Write CSV to this path. Default: print TSV to stdout.",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print match counts only (no CSV row listing).",
    )
    args = parser.parse_args()

    if args.stats and args.out:
        print("ERROR: use --stats alone (no --out).", file=sys.stderr)
        return 1
    mode_n = sum(
        bool(x)
        for x in (
            args.stats,
            args.distinct_registrations_only,
            args.list_phly_registrations,
        )
    )
    if mode_n > 1:
        print(
            "ERROR: use only one of --stats, --distinct-registrations-only, --list-phly-registrations.",
            file=sys.stderr,
        )
        return 1

    table = args.table.replace('"', "").replace(";", "").replace(" ", "")
    phly_canon = _CANON_EXPR.format(col="p.registration_number")
    fm_canon = _CANON_EXPR.format(col="fm.n_number")
    phly_sn = _SERIAL_NORM.format(col="phly.phly_serial")
    fm_sn = _SERIAL_NORM.format(col="fm.faa_serial")
    serial_match = _SERIAL_MATCH.format(phly_sn=phly_sn, fm_sn=fm_sn)

    _load_env()
    conn_str = os.getenv("POSTGRES_CONNECTION_STRING") or os.getenv("DATABASE_URL")
    if not conn_str:
        print(
            "ERROR: POSTGRES_CONNECTION_STRING or DATABASE_URL not set.",
            file=sys.stderr,
        )
        return 1

    # SQL bodies only use these placeholders; phly_sn/fm_sn are already inside serial_match.
    fmt_kw = dict(
        table=table,
        phly_canon=phly_canon,
        fm_canon=fm_canon,
        serial_match=serial_match,
    )
    if args.stats:
        sql = STATS_SQL.format(**fmt_kw)
        params = {"require_serial": args.require_serial}
    elif args.distinct_registrations_only:
        sql = DISTINCT_REG_SQL.format(**fmt_kw)
        params = {"require_serial": args.require_serial}
    elif args.list_phly_registrations:
        sql = PHLY_REGISTRATIONS_LIST_SQL.format(**fmt_kw)
        params = {"require_serial": args.require_serial}
    else:
        sql = LIST_SQL.format(**fmt_kw)
        params = {"require_serial": args.require_serial}

    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description]
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        return 1

    if args.stats:
        r = dict(zip(colnames, rows[0])) if rows else {}
        print("PhlyData ↔ faa_master registration (canonical N) match stats", file=sys.stderr)
        print(f"  require_serial: {args.require_serial}", file=sys.stderr)
        for k, v in r.items():
            if v is None:
                line = f"  {k}: —"
            elif hasattr(v, "bit_length") or isinstance(v, int):
                line = f"  {k}: {int(v):,}"
            else:
                line = f"  {k}: {v}"
            print(line, file=sys.stderr)
        return 0

    if not rows:
        print("No matching rows.", file=sys.stderr)
        return 0

    out_path = (args.out or "").strip()
    if args.list_phly_registrations:
        if out_path:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["phly_registration"])
                for r in rows:
                    w.writerow([r[0]])
            print(f"Wrote {len(rows):,} registrations to {out_path}", file=sys.stderr)
        else:
            for r in rows:
                print(r[0] or "")
        return 0

    if out_path:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(rows)
        print(f"Wrote {len(rows):,} rows to {out_path}", file=sys.stderr)
    else:
        w = csv.writer(sys.stdout, lineterminator="\n")
        w.writerow(colnames)
        w.writerows(rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
