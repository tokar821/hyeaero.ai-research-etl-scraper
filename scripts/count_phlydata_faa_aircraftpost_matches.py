#!/usr/bin/env python3
"""
Count how many PhlyData aircraft rows match FAA registry vs AircraftPost fleet data.

**Matching rules** (aligned with ``GET /api/phlydata/owners`` in ``backend/api/main.py``):

- **FAA** (``faa_registrations``):
  - If ``phlydata_aircraft.model`` is non-empty: match on ``serial_number`` plus
    ``mfr_mdl_code`` in ``faa_aircraft_reference`` where ``model`` ILIKE ``%phly_model%``.
  - If ``model`` is empty/NULL: match on ``serial_number`` only (latest-ingestion rows still qualify).
  - A row counts as a match only if at least one matching FAA row has a non-empty ``registrant_name``.

- **AircraftPost** (``aircraftpost_fleet_aircraft``):
  - Match on **normalized registration** only (uppercase, spaces and hyphens stripped),
    same as ``lookup_aircraftpost_owner_rows_by_registration``.

Environment: ``POSTGRES_CONNECTION_STRING`` (e.g. in ``etl-pipeline/.env``).

Usage::

    cd etl-pipeline
    python scripts/count_phlydata_faa_aircraftpost_matches.py

Optional::

    python scripts/count_phlydata_faa_aircraftpost_matches.py --table phlydata_aircraft
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


AGGREGATE_SQL = """
WITH per_row AS (
  SELECT
    p.aircraft_id,
    p.serial_number,
    p.registration_number,
    p.model,
    CASE
      WHEN p.serial_number IS NULL OR BTRIM(p.serial_number::text) = '' THEN FALSE
      WHEN p.model IS NOT NULL AND BTRIM(p.model::text) <> '' THEN
        EXISTS (
          SELECT 1
          FROM faa_registrations fr
          WHERE fr.serial_number = p.serial_number
            AND fr.registrant_name IS NOT NULL
            AND BTRIM(fr.registrant_name::text) <> ''
            AND fr.mfr_mdl_code IN (
              SELECT far.code
              FROM faa_aircraft_reference far
              WHERE far.model ILIKE ('%' || BTRIM(p.model::text) || '%')
            )
        )
      ELSE
        EXISTS (
          SELECT 1
          FROM faa_registrations fr
          WHERE fr.serial_number = p.serial_number
            AND fr.registrant_name IS NOT NULL
            AND BTRIM(fr.registrant_name::text) <> ''
        )
    END AS faa_matched,
    CASE
      WHEN p.registration_number IS NULL OR BTRIM(p.registration_number::text) = '' THEN FALSE
      ELSE EXISTS (
        SELECT 1
        FROM aircraftpost_fleet_aircraft ap
        WHERE REPLACE(REPLACE(UPPER(TRIM(COALESCE(ap.registration_number, ''))), ' ', ''), '-', '') =
              REPLACE(REPLACE(UPPER(TRIM(p.registration_number)), ' ', ''), '-', '')
          AND BTRIM(COALESCE(ap.registration_number::text, '')) <> ''
      )
    END AS ap_matched
  FROM public.{table} p
)
SELECT
  COUNT(*)::bigint AS total_phlydata_aircraft,
  COUNT(*) FILTER (WHERE serial_number IS NOT NULL AND BTRIM(serial_number::text) <> '')::bigint
    AS phlydata_with_serial,
  COUNT(*) FILTER (
    WHERE registration_number IS NOT NULL AND BTRIM(registration_number::text) <> ''
  )::bigint AS phlydata_with_registration,
  COUNT(*) FILTER (WHERE faa_matched)::bigint AS phlydata_rows_matching_faa,
  COUNT(*) FILTER (WHERE ap_matched)::bigint AS phlydata_rows_matching_aircraftpost,
  COUNT(*) FILTER (WHERE faa_matched AND ap_matched)::bigint AS phlydata_rows_matching_both,
  COUNT(*) FILTER (WHERE faa_matched AND NOT ap_matched)::bigint AS phlydata_faa_only,
  COUNT(*) FILTER (WHERE ap_matched AND NOT faa_matched)::bigint AS phlydata_aircraftpost_only,
  COUNT(*) FILTER (WHERE NOT faa_matched AND NOT ap_matched)::bigint AS phlydata_neither
FROM per_row;
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--table",
        default="phlydata_aircraft",
        help="PhlyData aircraft table name (default: phlydata_aircraft).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        print(
            "ERROR: POSTGRES_CONNECTION_STRING not set "
            "(set in etl-pipeline/.env or environment).",
            file=sys.stderr,
        )
        return 1

    table = args.table.replace('"', "").replace(";", "")
    sql = AGGREGATE_SQL.format(table=table)

    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if not row:
                    print("No result.")
                    return 1
                cols = [d[0] for d in cur.description]
                data = dict(zip(cols, row))
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        return 1

    total = data["total_phlydata_aircraft"]
    faa_n = data["phlydata_rows_matching_faa"]
    ap_n = data["phlydata_rows_matching_aircraftpost"]
    both = data["phlydata_rows_matching_both"]

    print("=== PhlyData ↔ FAA / AircraftPost match counts ===\n")
    print(f"Table: public.{table}")
    print(f"Total PhlyData aircraft rows:           {total:,}")
    print(f"  With non-empty serial:                {data['phlydata_with_serial']:,}")
    print(f"  With non-empty registration:          {data['phlydata_with_registration']:,}")
    print()
    print("Matches (same rules as /api/phlydata/owners):")
    print(f"  PhlyData rows with FAA match:         {faa_n:,}")
    if total:
        print(f"    ({100.0 * faa_n / total:.1f}% of PhlyData rows)")
    print(f"  PhlyData rows with AircraftPost match: {ap_n:,}")
    if total:
        print(f"    ({100.0 * ap_n / total:.1f}% of PhlyData rows)")
    print()
    print("Overlap:")
    print(f"  Both FAA and AircraftPost:            {both:,}")
    print(f"  FAA only:                             {data['phlydata_faa_only']:,}")
    print(f"  AircraftPost only:                    {data['phlydata_aircraftpost_only']:,}")
    print(f"  Neither:                              {data['phlydata_neither']:,}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
