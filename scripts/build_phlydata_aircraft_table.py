from __future__ import annotations

import argparse
import csv
import re
import uuid
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --- Canonical CSV → PostgreSQL (typed columns, 1:1 with standard export) ---

# (postgres_column, PostgreSQL type) — fixed order for INSERT/UPSERT.
PHLYDATA_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("serial_number", "VARCHAR(100)"),
    ("registration_number", "VARCHAR(50)"),
    ("manufacturer", "VARCHAR(200)"),
    ("model", "VARCHAR(200)"),
    ("manufacturer_year", "INTEGER"),
    ("delivery_year", "INTEGER"),
    ("category", "VARCHAR(100)"),
    ("aircraft_status", "VARCHAR(100)"),
    ("transaction_status", "VARCHAR(100)"),
    ("ask_price", "TEXT"),
    ("take_price", "TEXT"),
    ("sold_price", "TEXT"),
    ("airframe_total_time", "NUMERIC(14, 2)"),
    ("apu_total_time", "NUMERIC(14, 2)"),
    ("prop_total_time", "NUMERIC(14, 2)"),
    ("engine_program", "VARCHAR(200)"),
    ("engine_program_deferment", "VARCHAR(200)"),
    ("engine_program_deferment_amount", "NUMERIC(18, 2)"),
    ("apu_program", "VARCHAR(200)"),
    ("apu_program_deferment", "VARCHAR(200)"),
    ("apu_program_deferment_amount", "NUMERIC(18, 2)"),
    ("airframe_program", "VARCHAR(200)"),
    ("maintenance_tracking_program", "VARCHAR(200)"),
    ("registration_country", "VARCHAR(100)"),
    ("based_country", "VARCHAR(100)"),
    ("number_of_passengers", "INTEGER"),
    ("date_listed", "DATE"),
    ("interior_year", "INTEGER"),
    ("exterior_year", "INTEGER"),
    ("seller_broker", "VARCHAR(255)"),
    ("buyer_broker", "VARCHAR(255)"),
    ("seller", "VARCHAR(255)"),
    ("buyer", "VARCHAR(255)"),
    ("source_updated_at", "DATE"),
    ("updated_by", "VARCHAR(200)"),
    ("has_damage", "VARCHAR(50)"),
    ("feature_source", "VARCHAR(100)"),
    ("features", "TEXT"),
    ("next_inspections", "TEXT"),
)

CANONICAL_PG_COLS: frozenset[str] = frozenset(c for c, _ in PHLYDATA_COLUMN_TYPES)

# Normalized CSV header (strip) → postgres column. Covers the standard internaldb/aircraft.csv.
CSV_HEADER_TO_PG: dict[str, str] = {
    "Serial Number": "serial_number",
    "Manufacturer Year": "manufacturer_year",
    "Delivery Year": "delivery_year",
    "Make": "manufacturer",
    "Model": "model",
    "Category": "category",
    "Aircraft Status": "aircraft_status",
    "Transaction Status": "transaction_status",
    "Registration Number": "registration_number",
    "Ask Price": "ask_price",
    "Take Price": "take_price",
    "Sold Price": "sold_price",
    "Airframe Total Time": "airframe_total_time",
    "APU Total Time": "apu_total_time",
    "Prop Total Time": "prop_total_time",
    "Engine Program": "engine_program",
    "Engine Program Deferment": "engine_program_deferment",
    "Engine Program Deferment Amount": "engine_program_deferment_amount",
    "APU Program": "apu_program",
    "APU Program Deferment": "apu_program_deferment",
    "APU Program Deferment Amount": "apu_program_deferment_amount",
    "Airframe Program": "airframe_program",
    "Maintenance Tracking Program": "maintenance_tracking_program",
    "Registration Country": "registration_country",
    "Based Country": "based_country",
    "Number of Passengers": "number_of_passengers",
    "Date Listed": "date_listed",
    "Interior Year": "interior_year",
    "Exterior Year": "exterior_year",
    "Seller Broker": "seller_broker",
    "Buyer Broker": "buyer_broker",
    "Seller": "seller",
    "Buyer": "buyer",
    "Updated at": "source_updated_at",
    "Updated by": "updated_by",
    "Has Damage": "has_damage",
    "Feature Source": "feature_source",
    "Features": "features",
    "Next Inspections": "next_inspections",
}

KNOWN_CSV_HEADERS: frozenset[str] = frozenset(CSV_HEADER_TO_PG.keys())
CSV_HEADERS_USED = KNOWN_CSV_HEADERS  # back-compat


def _normalize_str(v: Any) -> str:
    return "" if v is None else str(v).replace("\u0000", "").strip()


def _parse_int_or_none(v: Any) -> int | None:
    s = _normalize_str(v)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _parse_numeric_or_none(v: Any) -> float | None:
    s = _normalize_str(v)
    if not s or s.upper() in ("M/O", "N/A", "-", "—", "NA", "TBD"):
        return None
    s = re.sub(r"[$€£,\s]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def _parse_date_or_none(v: Any) -> date | None:
    s = _normalize_str(v)
    if not s:
        return None
    head = s.split("T")[0].strip()[:10]
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", head)
    if m:
        try:
            return datetime.strptime(m.group(0), "%Y-%m-%d").date()
        except ValueError:
            pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(head if len(head) >= 8 else s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _price_text(v: Any) -> str | None:
    s = _normalize_str(v)
    return s if s else None


def _extra_cell_text(v: Any) -> str | None:
    """Store any non-canonical CSV cell as TEXT (lossless string after NUL strip + trim)."""
    s = _normalize_str(v)
    return s if s else None


def _canonical_parser(pg: str) -> Callable[[Any], Any]:
    def _s(v: Any) -> str | None:
        t = _normalize_str(v)
        return t if t else None

    if pg in (
        "serial_number",
        "registration_number",
        "manufacturer",
        "model",
        "category",
        "aircraft_status",
        "transaction_status",
        "engine_program",
        "engine_program_deferment",
        "apu_program",
        "apu_program_deferment",
        "airframe_program",
        "maintenance_tracking_program",
        "registration_country",
        "based_country",
        "seller_broker",
        "buyer_broker",
        "seller",
        "buyer",
        "updated_by",
        "has_damage",
        "feature_source",
        "features",
        "next_inspections",
    ):
        return _s
    if pg in ("manufacturer_year", "delivery_year", "number_of_passengers", "interior_year", "exterior_year"):
        return _parse_int_or_none
    if pg in ("airframe_total_time", "apu_total_time", "prop_total_time"):
        return _parse_numeric_or_none
    if pg in (
        "engine_program_deferment_amount",
        "apu_program_deferment_amount",
    ):
        return _parse_numeric_or_none
    if pg in ("ask_price", "take_price", "sold_price"):
        return _price_text
    if pg in ("date_listed", "source_updated_at"):
        return _parse_date_or_none
    return _s


def _norm_header(h: str | None) -> str:
    return (h or "").strip()


def _slugify_extra_header(fn_norm: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", fn_norm.lower())
    base = re.sub(r"_+", "_", base).strip("_") or "unnamed"
    if base[0].isdigit():
        base = "c_" + base
    return ("csv_" + base)[:60]


def _allocate_extra_pg_column(fn_norm: str, reserved: Set[str]) -> str:
    name = _slugify_extra_header(fn_norm)
    if name not in reserved:
        reserved.add(name)
        return name
    i = 2
    while True:
        suffix = f"_{i}"
        cand = (name[: 60 - len(suffix)] + suffix)[:60]
        if cand not in reserved:
            reserved.add(cand)
            return cand
        i += 1


def _build_header_mapping(fieldnames: Sequence[str | None]) -> tuple[dict[str, str], List[str]]:
    """
    normalized_header -> pg_column.
    Canonical headers use typed column names; any other CSV column gets its own ``csv_*`` TEXT column.
    """
    reserved: Set[str] = set(CANONICAL_PG_COLS)
    header_to_pg: dict[str, str] = {}
    for raw in fieldnames:
        fn = _norm_header(str(raw) if raw is not None else "")
        if not fn:
            continue
        if fn in CSV_HEADER_TO_PG:
            pg = CSV_HEADER_TO_PG[fn]
        else:
            pg = _allocate_extra_pg_column(fn, reserved)
        header_to_pg[fn] = pg

    extra_pg: List[str] = []
    seen: Set[str] = set()
    for raw in fieldnames:
        fn = _norm_header(str(raw) if raw is not None else "")
        if not fn or fn in CSV_HEADER_TO_PG:
            continue
        pg = header_to_pg[fn]
        if pg not in seen:
            seen.add(pg)
            extra_pg.append(pg)
    extra_pg.sort()
    return header_to_pg, extra_pg


def _validate_csv_fieldnames(fieldnames: list[str] | None, header_to_pg: dict[str, str]) -> None:
    if not fieldnames:
        raise RuntimeError("Internal CSV has no header row.")
    fn_set = {_norm_header(x) for x in fieldnames if _norm_header(x)}
    missing = KNOWN_CSV_HEADERS - fn_set
    if missing:
        print(
            "[warn] aircraft.csv is missing some canonical column(s) (those cells will be NULL): "
            + ", ".join(sorted(missing))
        )
    extra = fn_set - KNOWN_CSV_HEADERS
    if extra:
        print(
            "[info] CSV has extra column(s) — each gets its own PostgreSQL TEXT column: "
            + ", ".join(sorted(extra))
        )


def _stable_uuid_from_row(
    serial_number: str,
    registration_number: str,
    manufacturer: str,
    model: str,
    manufacturer_year: int | None,
    delivery_year: int | None,
    category: str,
) -> uuid.UUID:
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


def _row_to_pg_dict(
    row: dict[str, Any],
    fieldnames: Sequence[str],
    header_to_pg: dict[str, str],
    extra_pg_cols: Sequence[str],
) -> dict[str, Any] | None:
    """One pass over CSV columns: typed canonical values + raw TEXT for every extra column."""
    by_pg: dict[str, Any] = {}

    for fn in fieldnames:
        fn_key = str(fn)
        nk = _norm_header(fn_key)
        if not nk:
            continue
        pg = header_to_pg.get(nk)
        if not pg:
            continue
        raw_val = row.get(fn_key)
        if pg in CANONICAL_PG_COLS:
            parser = _canonical_parser(pg)
            by_pg[pg] = parser(raw_val)
        else:
            by_pg[pg] = _extra_cell_text(raw_val)

    serial_number = _normalize_str(by_pg.get("serial_number") or "")
    registration_number = _normalize_str(by_pg.get("registration_number") or "")
    if not serial_number and not registration_number:
        return None

    manufacturer = _normalize_str(by_pg.get("manufacturer") or "")
    model = _normalize_str(by_pg.get("model") or "")
    category = _normalize_str(by_pg.get("category") or "")
    manufacturer_year = by_pg.get("manufacturer_year")
    delivery_year = by_pg.get("delivery_year")
    if manufacturer_year is not None and not isinstance(manufacturer_year, int):
        manufacturer_year = _parse_int_or_none(manufacturer_year)
    if delivery_year is not None and not isinstance(delivery_year, int):
        delivery_year = _parse_int_or_none(delivery_year)

    aircraft_id = _stable_uuid_from_row(
        serial_number=serial_number,
        registration_number=registration_number,
        manufacturer=manufacturer,
        model=model,
        manufacturer_year=manufacturer_year,
        delivery_year=delivery_year,
        category=category,
    )
    by_pg["aircraft_id"] = str(aircraft_id)

    # Ensure every canonical column exists (NULL if absent from CSV)
    for c, _ in PHLYDATA_COLUMN_TYPES:
        if c not in by_pg:
            by_pg[c] = None
    # Every dynamic TEXT column in this load (matches INSERT column list)
    for c in extra_pg_cols:
        by_pg.setdefault(c, None)

    return by_pg


def _pg_dict_to_tuple(by_pg: dict[str, Any], column_order: Sequence[str]) -> tuple[Any, ...]:
    return tuple(by_pg.get(c) for c in column_order)


def _ensure_table_schema(cur: Any, table: str, extra_pg_cols: Sequence[str]) -> list[str]:
    """Create table if missing; add canonical typed columns and one TEXT column per extra CSV header."""
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          aircraft_id UUID PRIMARY KEY
        );
        """
    )
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    existing = {r[0] for r in cur.fetchall()}
    added: list[str] = []
    for col_name, col_type in PHLYDATA_COLUMN_TYPES:
        if col_name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type};")
            added.append(col_name)
            existing.add(col_name)
    for col_name in extra_pg_cols:
        if col_name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} TEXT;")
            added.append(col_name)
            existing.add(col_name)
    return added


def _column_order(extra_pg_cols: Sequence[str]) -> list[str]:
    return ["aircraft_id"] + [c for c, _ in PHLYDATA_COLUMN_TYPES] + list(extra_pg_cols)


def _build_upsert_sql(table: str, column_order: Sequence[str]) -> str:
    cols = list(column_order)
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "aircraft_id")
    return f"""
    INSERT INTO {table} ({col_list})
    VALUES %s
    ON CONFLICT (aircraft_id) DO UPDATE SET
      {updates}
    """


def _load_internal_csv_rows(
    internal_csv_path: Path,
    header_to_pg: dict[str, str],
    column_order: Sequence[str],
    extra_pg_cols: Sequence[str],
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Single pass: exact DictReader headers + data rows."""
    if not internal_csv_path.exists():
        raise FileNotFoundError(internal_csv_path)
    rows: list[tuple[Any, ...]] = []
    with open(internal_csv_path, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        fn_list = list(reader.fieldnames or [])
        if not fn_list:
            raise RuntimeError("Internal CSV has no header row.")
        _validate_csv_fieldnames(fn_list, header_to_pg)

        for row in reader:
            d = _row_to_pg_dict(row, fn_list, header_to_pg, extra_pg_cols)
            if d is not None:
                rows.append(_pg_dict_to_tuple(d, column_order))
    return fn_list, rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Load internaldb/aircraft.csv into PostgreSQL phlydata_aircraft: every CSV column becomes a "
            "Postgres column (typed canonical set + TEXT per extra header)."
        )
    )
    parser.add_argument("--table-name", default="phlydata_aircraft", type=str, help="Destination table name.")
    parser.add_argument(
        "--internal-csv",
        default="",
        type=str,
        help="Path to aircraft.csv. Default: etl-pipeline/store/raw/internaldb/aircraft.csv.",
    )
    parser.add_argument("--reset", action="store_true", help="TRUNCATE the destination table before inserting.")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not write data.")
    parser.add_argument(
        "--drop-legacy-csv-extra",
        action="store_true",
        help="DROP COLUMN csv_extra if it exists (old JSON bucket; not used anymore).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    hye_root = repo_root.parent
    load_dotenv(repo_root / ".env")
    if not (os.getenv("POSTGRES_CONNECTION_STRING") or "").strip():
        load_dotenv(hye_root / "backend" / ".env")
    if not (os.getenv("POSTGRES_CONNECTION_STRING") or "").strip():
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not found (expected in etl-pipeline/.env)")

    internal_csv_path = (
        Path(args.internal_csv)
        if args.internal_csv
        else repo_root / "store" / "raw" / "internaldb" / "aircraft.csv"
    )

    # Peek headers only (fast) so we can ALTER TABLE before the full read.
    with open(internal_csv_path, "r", encoding="utf-8-sig", errors="ignore", newline="") as _peek:
        _dr = csv.DictReader(_peek)
        _fn = list(_dr.fieldnames or [])
        if not _fn:
            raise RuntimeError("Internal CSV has no header row.")
    header_to_pg, extra_pg_cols = _build_header_mapping(_fn)
    column_order = _column_order(extra_pg_cols)

    table = args.table_name
    insert_sql = _build_upsert_sql(table, column_order)

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            added_cols = _ensure_table_schema(cur, table, extra_pg_cols)
            if added_cols:
                print(f"[{table}] Added PostgreSQL column(s): {', '.join(added_cols)}")
            if args.drop_legacy_csv_extra:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = 'csv_extra'
                    """,
                    (table,),
                )
                if cur.fetchone():
                    cur.execute(f"ALTER TABLE {table} DROP COLUMN csv_extra;")
                    print(f"[{table}] Dropped legacy column csv_extra.")
            conn.commit()

        with conn.cursor() as cur:
            _fn2, rows = _load_internal_csv_rows(
                internal_csv_path, header_to_pg, column_order, extra_pg_cols
            )
            total = len(rows)
            print(f"[{table}] internal CSV rows to upsert: {total:,}")
            print(f"[{table}] insert column count: {len(column_order)} (aircraft_id + {len(PHLYDATA_COLUMN_TYPES)} typed + {len(extra_pg_cols)} extra)")

            if args.dry_run:
                print("Dry-run enabled; not modifying DB.")
                return

            if args.reset:
                cur.execute(f"TRUNCATE TABLE {table};")

            if rows:
                execute_values(
                    cur,
                    insert_sql,
                    rows,
                    page_size=500,
                )
            conn.commit()
            print(f"[{table}] insert/upsert completed.")

        with conn.cursor() as cur2:
            cur2.execute(f"SELECT COUNT(*) AS total FROM {table};")
            after = cur2.fetchone()[0]
            print(f"[{table}] final row count: {after:,}")


if __name__ == "__main__":
    main()
