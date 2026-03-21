from __future__ import annotations

import argparse
import csv
import uuid
import os
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


def _normalize_str(v: Any) -> str:
    return "" if v is None else str(v).replace("\u0000", "").strip()


def _parse_int_or_none(v: Any) -> int | None:
    s = _normalize_str(v)
    if not s:
        return None
    try:
        # Sometimes CSV numeric fields come through like "42000258.0" or " 2023".
        return int(float(s))
    except Exception:
        return None


def _stable_uuid_from_row(
    serial_number: str,
    registration_number: str,
    manufacturer: str,
    model: str,
    manufacturer_year: int | None,
    delivery_year: int | None,
    category: str,
) -> uuid.UUID:
    # Deterministic UUID so repeated runs won't create duplicates.
    # uuid5 uses SHA-1 internally; it's stable across runs.
    namespace = uuid.UUID("d7f64b2c-2db5-4d2f-9c1c-5bbd8c2b2b6a")
    name = "|".join(
        [
            serial_number,
            registration_number,
            manufacturer,
            model,
            "" if manufacturer_year is None else str(manufacturer_year),
            "" if delivery_year is None else str(delivery_year),
            category,
        ]
    )
    return uuid.uuid5(namespace, name)


def _load_internal_csv_rows(internal_csv_path: Path) -> list[tuple]:
    """
    Parse internaldb/aircraft.csv and return rows ready for insertion.

    Note: We DO NOT match against the `aircraft` table; this table is built directly from CSV.
    """
    if not internal_csv_path.exists():
        raise FileNotFoundError(internal_csv_path)

    rows: list[tuple] = []
    with open(internal_csv_path, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"Internal CSV has no header: {internal_csv_path}")

        for row in reader:
            serial_number = _normalize_str(row.get("Serial Number"))
            registration_number = _normalize_str(row.get("Registration Number"))
            manufacturer = _normalize_str(row.get("Make"))
            model = _normalize_str(row.get("Model"))
            manufacturer_year = _parse_int_or_none(row.get("Manufacturer Year"))
            delivery_year = _parse_int_or_none(row.get("Delivery Year"))
            category = _normalize_str(row.get("Category"))

            # We still require at least one identifier; otherwise the row isn't useful for lookups.
            if not serial_number and not registration_number:
                continue

            aircraft_id = _stable_uuid_from_row(
                serial_number=serial_number,
                registration_number=registration_number,
                manufacturer=manufacturer,
                model=model,
                manufacturer_year=manufacturer_year,
                delivery_year=delivery_year,
                category=category,
            )

            rows.append(
                (
                    str(aircraft_id),
                    serial_number or None,
                    registration_number or None,
                    manufacturer or None,
                    model or None,
                    manufacturer_year,
                    delivery_year,
                    category or None,
                )
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a dedicated PostgreSQL table for PHlyData (internaldb) aircraft using internaldb/aircraft.csv directly (no join to aircraft table)."
    )
    parser.add_argument("--table-name", default="phlydata_aircraft", type=str, help="Destination table name.")
    parser.add_argument(
        "--internal-csv",
        default="",
        type=str,
        help="Optional path to internaldb/aircraft.csv. Defaults to etl-pipeline/store/raw/internaldb/aircraft.csv.",
    )
    parser.add_argument("--reset", action="store_true", help="TRUNCATE the destination table before inserting.")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not write data.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]  # etl-pipeline/
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not found (expected in etl-pipeline/.env)")

    internal_csv_path = (
        Path(args.internal_csv)
        if args.internal_csv
        else repo_root / "store" / "raw" / "internaldb" / "aircraft.csv"
    )

    table = args.table_name

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
      aircraft_id UUID PRIMARY KEY,
      serial_number VARCHAR(100),
      registration_number VARCHAR(50),
      manufacturer VARCHAR(100),
      model VARCHAR(100),
      manufacturer_year INTEGER,
      delivery_year INTEGER,
      category VARCHAR(50)
    );
    """

    insert_sql = f"""
    INSERT INTO {table} (
      aircraft_id,
      serial_number,
      registration_number,
      manufacturer,
      model,
      manufacturer_year,
      delivery_year,
      category
    )
    VALUES %s
    ON CONFLICT (aircraft_id) DO UPDATE SET
      serial_number = EXCLUDED.serial_number,
      registration_number = EXCLUDED.registration_number,
      manufacturer = EXCLUDED.manufacturer,
      model = EXCLUDED.model,
      manufacturer_year = EXCLUDED.manufacturer_year,
      delivery_year = EXCLUDED.delivery_year,
      category = EXCLUDED.category;
    """

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            rows = _load_internal_csv_rows(internal_csv_path)
            total = len(rows)
            print(f"[{table}] internal CSV rows to upsert: {total:,}")

            if args.dry_run:
                print("Dry-run enabled; not modifying DB.")
                return

            cur.execute(create_sql)
            if args.reset:
                cur.execute(f"TRUNCATE TABLE {table};")

            if rows:
                execute_values(
                    cur,
                    insert_sql,
                    rows,
                    page_size=2000,
                )
            conn.commit()
            print(f"[{table}] insert/upsert completed.")

        with conn.cursor() as cur2:
            cur2.execute(f"SELECT COUNT(*) AS total FROM {table};")
            after = cur2.fetchone()[0]
            print(f"[{table}] final row count: {after:,}")


if __name__ == "__main__":
    main()

