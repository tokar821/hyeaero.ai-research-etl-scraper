from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import psycopg2
from dotenv import load_dotenv


def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip()


def load_db_faa_registrations(
    conn_str: str,
    ingestion_date: str,
) -> Tuple[Set[str], Dict[str, str], int]:
    """
    Returns:
      - db_serials (non-empty serial_number)
      - db_registrant_name_by_serial (serial -> registrant_name)
      - total_db_rows_loaded
    """
    sql = """
        SELECT serial_number, registrant_name
        FROM faa_registrations
        WHERE ingestion_date = %s
          AND serial_number IS NOT NULL
          AND TRIM(serial_number) <> ''
    """
    db_serials: Set[str] = set()
    db_by_serial: Dict[str, str] = {}
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ingestion_date,))
            rows = cur.fetchall()

    total = 0
    for serial_number, registrant_name in rows:
        total += 1
        s = _norm(serial_number)
        if not s:
            continue
        db_serials.add(s)
        # In practice there should be one row per serial_number for a given ingestion_date,
        # but we keep last one if duplicates exist.
        db_by_serial[s] = _norm(registrant_name)

    return db_serials, db_by_serial, total


def cross_reference(
    *,
    master_csv_path: Path,
    conn_str: str,
    ingestion_date: str,
    master_serial_col: str,
    master_name_col: str,
    report_csv_path: Path,
    max_master_rows: Optional[int] = None,
    max_mismatch_rows: int = 2000,
) -> Dict[str, int]:
    report_csv_path.parent.mkdir(parents=True, exist_ok=True)

    db_serials, db_name_by_serial, db_rows_loaded = load_db_faa_registrations(
        conn_str=conn_str,
        ingestion_date=ingestion_date,
    )

    # Pass 1: stream master CSV and build:
    # - master_serials: all serial_number values present in master CSV
    # - master_names_by_serial: only for serials that exist in the DB (to avoid huge memory)
    master_serials: Set[str] = set()
    master_names_by_serial: Dict[str, Set[str]] = {}

    with open(master_csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"MASTER CSV has no header: {master_csv_path}")

        if master_serial_col not in reader.fieldnames:
            raise RuntimeError(
                f"MASTER CSV missing column '{master_serial_col}'. "
                f"Available columns: {reader.fieldnames[:10]}..."
            )
        if master_name_col not in reader.fieldnames:
            raise RuntimeError(
                f"MASTER CSV missing column '{master_name_col}'. "
                f"Available columns: {reader.fieldnames[:10]}..."
            )

        for i, row in enumerate(reader, start=1):
            if max_master_rows is not None and i > max_master_rows:
                break

            serial = _norm(row.get(master_serial_col))
            if not serial:
                continue
            master_serials.add(serial)

            if serial in db_serials:
                name = _norm(row.get(master_name_col))
                if serial not in master_names_by_serial:
                    master_names_by_serial[serial] = set()
                if name:
                    master_names_by_serial[serial].add(name)

    # Compute set-level differences
    missing_serial_in_db = master_serials - db_serials
    extra_serial_in_db = db_serials - master_serials
    intersection_serials = db_serials & master_serials

    # Name mismatches (DB registrant_name not present among master names for that serial)
    mismatch_rows: list[dict] = []
    mismatches = 0

    for serial in sorted(intersection_serials):
        db_name = db_name_by_serial.get(serial, "")
        master_names = master_names_by_serial.get(serial, set())

        if not db_name:
            continue

        if db_name not in master_names:
            mismatches += 1
            if len(mismatch_rows) < max_mismatch_rows:
                mismatch_rows.append(
                    {
                        "type": "name_mismatch",
                        "serial_number": serial,
                        "db_registrant_name": db_name,
                        "master_names_found": "; ".join(sorted(master_names))[:4000],
                    }
                )

    # Missing/extra serial reports (also capped to avoid huge CSVs)
    missing_rows: list[dict] = []
    for serial in sorted(missing_serial_in_db):
        if len(missing_rows) >= max_mismatch_rows:
            break
        missing_rows.append(
            {
                "type": "missing_in_db",
                "serial_number": serial,
                "db_registrant_name": "",
                "master_names_found": "",
            }
        )

    extra_rows: list[dict] = []
    for serial in sorted(extra_serial_in_db):
        if len(extra_rows) >= max_mismatch_rows:
            break
        extra_rows.append(
            {
                "type": "extra_in_db",
                "serial_number": serial,
                "db_registrant_name": db_name_by_serial.get(serial, ""),
                "master_names_found": "",
            }
        )

    # Write report CSV
    with open(report_csv_path, "w", newline="", encoding="utf-8") as wf:
        writer = csv.DictWriter(
            wf,
            fieldnames=[
                "type",
                "serial_number",
                "db_registrant_name",
                "master_names_found",
            ],
        )
        writer.writeheader()
        for r in missing_rows:
            writer.writerow(r)
        for r in extra_rows:
            writer.writerow(r)
        for r in mismatch_rows:
            writer.writerow(r)

    return {
        "db_rows_loaded": db_rows_loaded,
        "master_serials_count": len(master_serials),
        "db_serials_count": len(db_serials),
        "missing_serial_in_db": len(missing_serial_in_db),
        "extra_serial_in_db": len(extra_serial_in_db),
        "name_mismatches": mismatches,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-reference faa_registrations.serial_number + registrant_name against exported Master CSV (key-level QA; no full field-by-field compare)."
    )
    parser.add_argument("--master-csv", required=True, type=str, help="Path to exported master CSV (e.g. store/export/faa_master_YYYY-MM-DD.csv)")
    parser.add_argument("--ingestion-date", required=True, type=str, help="faa_registrations ingestion_date (YYYY-MM-DD)")
    parser.add_argument("--report-csv", required=True, type=str, help="Output report CSV path")
    parser.add_argument("--master-serial-col", default="SERIAL NUMBER", type=str, help="Column name for serial number in Master CSV")
    parser.add_argument("--master-name-col", default="NAME", type=str, help="Column name for registrant/owner name in Master CSV")
    parser.add_argument("--max-master-rows", default=None, type=int, help="Optional cap for testing (first N master CSV rows)")
    parser.add_argument("--max-mismatch-rows", default=2000, type=int, help="Cap the number of output rows per mismatch type")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]  # etl-pipeline/
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not found in etl-pipeline/.env")

    stats = cross_reference(
        master_csv_path=Path(args.master_csv),
        conn_str=conn_str,
        ingestion_date=args.ingestion_date,
        master_serial_col=args.master_serial_col,
        master_name_col=args.master_name_col,
        report_csv_path=Path(args.report_csv),
        max_master_rows=args.max_master_rows,
        max_mismatch_rows=args.max_mismatch_rows,
    )

    print(stats)


if __name__ == "__main__":
    main()

