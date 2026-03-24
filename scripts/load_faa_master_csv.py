#!/usr/bin/env python3
"""
Load FAA MASTER CSV into PostgreSQL table `faa_master`.

Prereq: run migration
  psql "$DATABASE_URL" -f etl-pipeline/database/migrations/ensure_faa_master.sql

Usage (PowerShell, from repo root):
  cd D:\\HyeAero
  $env:POSTGRES_CONNECTION_STRING = "postgresql://user:pass@host:5432/dbname"
  python etl-pipeline/scripts/load_faa_master_csv.py `
    --csv-path "D:\\HyeAero\\etl-pipeline\\store\\export\\faa_master_2026-01-23.csv" `
    --ingestion-date 2026-01-23 `
    --apply

Dry-run (no DB writes):
  python etl-pipeline/scripts/load_faa_master_csv.py --csv-path "..." --ingestion-date 2026-01-23
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Install psycopg2-binary: pip install psycopg2-binary", file=sys.stderr)
    raise

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_CANDIDATES = [
    REPO_ROOT / "etl-pipeline" / ".env",
    REPO_ROOT / "backend" / ".env",
    REPO_ROOT / ".env",
]


def load_dotenv_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


for _p in ENV_CANDIDATES:
    load_dotenv_file(_p)


def _norm_header(h: str) -> str:
    # FAA exports often use UTF-8 BOM on first column (shows as "\ufeffN-NUMBER").
    s = (h or "").lstrip("\ufeff").strip()
    return re.sub(r"\s+", " ", s).upper()


HEADER_TO_DB = {
    "N-NUMBER": "n_number",
    "SERIAL NUMBER": "serial_number",
    "MFR MDL CODE": "mfr_mdl_code",
    "ENG MFR MDL": "eng_mfr_mdl",
    "YEAR MFR": "year_mfr",
    "TYPE REGISTRANT": "type_registrant",
    "NAME": "registrant_name",
    "STREET": "street",
    "STREET2": "street2",
    "CITY": "city",
    "STATE": "state",
    "ZIP CODE": "zip_code",
    "REGION": "region",
    "COUNTY": "county",
    "COUNTRY": "country",
    "LAST ACTION DATE": "last_action_date",
    "CERT ISSUE DATE": "cert_issue_date",
    "CERTIFICATION": "certification",
    "TYPE AIRCRAFT": "type_aircraft",
    "TYPE ENGINE": "type_engine",
    "STATUS CODE": "status_code",
    "MODE S CODE": "mode_s_code",
    "FRACT OWNER": "fract_owner",
    "AIR WORTH DATE": "air_worth_date",
    "OTHER NAMES(1)": "other_name_1",
    "OTHER NAMES(2)": "other_name_2",
    "OTHER NAMES(3)": "other_name_3",
    "OTHER NAMES(4)": "other_name_4",
    "OTHER NAMES(5)": "other_name_5",
    "EXPIRATION DATE": "expiration_date",
    "UNIQUE ID": "unique_id",
    "KIT MFR": "kit_mfr",
    "KIT MODEL": "kit_model",
    "MODE S CODE HEX": "mode_s_code_hex",
}


def parse_faa_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    s = str(val).strip()
    if not s or not re.fullmatch(r"\d{8}", s):
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def parse_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip()
    if not s or not re.fullmatch(r"-?\d+", s):
        return None
    return int(s)


def parse_smallint(val: Any) -> Optional[int]:
    i = parse_int(val)
    if i is None:
        return None
    if i < -32768 or i > 32767:
        return None
    return i


def norm_n_number(val: Any) -> str:
    s = str(val or "").strip().upper()
    s = re.sub(r"\s+", "", s)
    return s


def row_to_tuple(
    raw: Dict[str, str],
    col_map: Dict[str, str],
    source_file: str,
    ingestion_date: date,
) -> tuple:
    def get(db_col: str) -> str:
        for csv_h, k in col_map.items():
            if k == db_col:
                return raw.get(csv_h, "") or ""
        return ""

    n_number = norm_n_number(get("n_number"))
    if not n_number:
        raise ValueError("missing N-NUMBER")

    return (
        n_number,
        (get("serial_number").strip() or None),
        (get("mfr_mdl_code").strip() or None),
        (get("eng_mfr_mdl").strip() or None),
        parse_int(get("year_mfr")),
        parse_smallint(get("type_registrant")),
        (get("registrant_name").strip() or None),
        (get("street").strip() or None),
        (get("street2").strip() or None),
        (get("city").strip() or None),
        (get("state").strip() or None),
        (get("zip_code").strip() or None),
        (get("region").strip() or None),
        (get("county").strip() or None),
        (get("country").strip() or None),
        parse_faa_date(get("last_action_date")),
        parse_faa_date(get("cert_issue_date")),
        (get("certification").strip() or None),
        (get("type_aircraft").strip() or None),
        (get("type_engine").strip() or None),
        (get("status_code").strip() or None),
        (get("mode_s_code").strip() or None),
        (get("fract_owner").strip() or None),
        parse_faa_date(get("air_worth_date")),
        (get("other_name_1").strip() or None),
        (get("other_name_2").strip() or None),
        (get("other_name_3").strip() or None),
        (get("other_name_4").strip() or None),
        (get("other_name_5").strip() or None),
        parse_faa_date(get("expiration_date")),
        (get("unique_id").strip() or None),
        (get("kit_mfr").strip() or None),
        (get("kit_model").strip() or None),
        (get("mode_s_code_hex").strip() or None),
        source_file,
        ingestion_date,
    )


INSERT_SQL = """
INSERT INTO faa_master (
    n_number, serial_number, mfr_mdl_code, eng_mfr_mdl, year_mfr, type_registrant,
    registrant_name, street, street2, city, state, zip_code, region, county, country,
    last_action_date, cert_issue_date, certification, type_aircraft, type_engine,
    status_code, mode_s_code, fract_owner, air_worth_date,
    other_name_1, other_name_2, other_name_3, other_name_4, other_name_5,
    expiration_date, unique_id, kit_mfr, kit_model, mode_s_code_hex,
    source_file, ingestion_date
) VALUES %s
ON CONFLICT (n_number, ingestion_date) DO UPDATE SET
    serial_number = EXCLUDED.serial_number,
    mfr_mdl_code = EXCLUDED.mfr_mdl_code,
    eng_mfr_mdl = EXCLUDED.eng_mfr_mdl,
    year_mfr = EXCLUDED.year_mfr,
    type_registrant = EXCLUDED.type_registrant,
    registrant_name = EXCLUDED.registrant_name,
    street = EXCLUDED.street,
    street2 = EXCLUDED.street2,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    region = EXCLUDED.region,
    county = EXCLUDED.county,
    country = EXCLUDED.country,
    last_action_date = EXCLUDED.last_action_date,
    cert_issue_date = EXCLUDED.cert_issue_date,
    certification = EXCLUDED.certification,
    type_aircraft = EXCLUDED.type_aircraft,
    type_engine = EXCLUDED.type_engine,
    status_code = EXCLUDED.status_code,
    mode_s_code = EXCLUDED.mode_s_code,
    fract_owner = EXCLUDED.fract_owner,
    air_worth_date = EXCLUDED.air_worth_date,
    other_name_1 = EXCLUDED.other_name_1,
    other_name_2 = EXCLUDED.other_name_2,
    other_name_3 = EXCLUDED.other_name_3,
    other_name_4 = EXCLUDED.other_name_4,
    other_name_5 = EXCLUDED.other_name_5,
    expiration_date = EXCLUDED.expiration_date,
    unique_id = EXCLUDED.unique_id,
    kit_mfr = EXCLUDED.kit_mfr,
    kit_model = EXCLUDED.kit_model,
    mode_s_code_hex = EXCLUDED.mode_s_code_hex,
    source_file = EXCLUDED.source_file,
    updated_at = NOW()
"""


def main() -> int:
    p = argparse.ArgumentParser(description="Load FAA MASTER CSV into faa_master table.")
    p.add_argument("--csv-path", required=True, help="Path to FAA MASTER CSV export.")
    p.add_argument(
        "--ingestion-date",
        required=True,
        help="Snapshot date YYYY-MM-DD (stored on every row for lineage).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Write to database. Without this flag, only parses CSV and prints summary.",
    )
    p.add_argument("--batch-size", type=int, default=2000, help="Rows per INSERT batch.")
    args = p.parse_args()

    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    try:
        ingestion_date = datetime.strptime(args.ingestion_date.strip(), "%Y-%m-%d").date()
    except ValueError:
        print("--ingestion-date must be YYYY-MM-DD", file=sys.stderr)
        return 1

    conn_str = os.environ.get("POSTGRES_CONNECTION_STRING") or os.environ.get("DATABASE_URL")
    if args.apply and not conn_str:
        print("Set POSTGRES_CONNECTION_STRING or DATABASE_URL for --apply", file=sys.stderr)
        return 1

    with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print("CSV has no header row.", file=sys.stderr)
            return 1

        col_map: Dict[str, str] = {}
        unknown: list[str] = []
        for h in reader.fieldnames:
            if h is None:
                continue
            if not str(h).strip():
                continue  # trailing empty column from CSV
            nh = _norm_header(h)
            if nh in HEADER_TO_DB:
                col_map[h] = HEADER_TO_DB[nh]
            else:
                nh2 = nh.lstrip()
                if nh2 in HEADER_TO_DB:
                    col_map[h] = HEADER_TO_DB[nh2]
                else:
                    unknown.append(h)

        required_csv = "N-NUMBER"
        if not any(_norm_header(h) == required_csv for h in col_map.keys()):
            print(
                "Expected column N-NUMBER in CSV. "
                f"Mapped headers: {list(col_map.keys())[:15]}... Unknown: {unknown[:10]}",
                file=sys.stderr,
            )
            return 1

        rows: list[tuple] = []
        errors = 0
        for i, raw in enumerate(reader, start=2):
            try:
                rows.append(
                    row_to_tuple(raw, col_map, str(csv_path.name), ingestion_date)
                )
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"Row {i} skip: {e}", file=sys.stderr)

    print(f"Parsed {len(rows)} rows from {csv_path.name} (errors: {errors}).")
    if unknown:
        print(f"Ignored unknown CSV columns ({len(unknown)}): {unknown[:8]}...")

    if not args.apply:
        print("Dry-run only. Re-run with --apply to load into PostgreSQL.")
        return 0

    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            for start in range(0, len(rows), args.batch_size):
                batch = rows[start : start + args.batch_size]
                execute_values(cur, INSERT_SQL, batch, page_size=len(batch))
            conn.commit()
        print(f"Upserted {len(rows)} rows into faa_master for ingestion_date={ingestion_date}.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
