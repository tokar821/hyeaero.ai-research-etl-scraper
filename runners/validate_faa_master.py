"""
Validate FAA MASTER.txt -> PostgreSQL persistence.

Provides two capabilities:
1) Export MASTER.txt to CSV (raw rows).
2) Cross-reference MASTER.txt rows against DB rows for a given ingestion_date:
   - Primary match key: N-NUMBER (tail number) when present, otherwise SERIAL NUMBER.
   - Compares owner/registrant related fields saved in faa_registrations.
   - Optionally compares a few aircraft fields updated by the FAA loader.

This is intended for QA/debugging of ETL correctness (not for production ETL runs).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import sys

# Allow "from database...." imports when run from runners/
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from database.postgres_client import PostgresClient

_env_file = Path(__file__).parent.parent / ".env"
# Load DB credentials for PostgresClient() default parameters.
# This avoids falling back to localhost:5432 when running this script directly.
if _env_file.exists():
    load_dotenv(_env_file)


logger = logging.getLogger(__name__)


def parse_int(value: Any) -> Optional[int]:
    """Parse integer-like values (including values stored as floats with .0 in source files)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        # Handles values like "0030" and "12.0"
        return int(float(s.replace(",", "")))
    except (ValueError, TypeError):
        return None


def parse_date(value: Any) -> Optional[date]:
    """Parse dates in common FAA loader formats."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # MASTER.txt looks like YYYYMMDD but the loader also supports ISO.
    # We'll support both.
    if re.fullmatch(r"\d{8}", s):
        # YYYYMMDD
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def clean_registration(registration: Optional[str]) -> Optional[str]:
    """Mimic BaseLoader._clean_registration enough for matching aircraft.registration_number."""
    if not registration:
        return None
    cleaned = str(registration).strip()
    cleaned = re.sub(r"\([^)]*\)", "", cleaned).strip()
    parts = cleaned.split()
    if parts:
        cleaned = parts[0].strip()
    # DB column size is 50 for registration_number
    return cleaned[:50] if cleaned else None


def parse_other_names(row: Dict[str, Any], max_names: int = 5) -> List[str]:
    """Parse OTHER NAMES(1..5) style fields into a list."""
    out: List[str] = []
    for i in range(1, max_names + 1):
        key1 = f"OTHER NAMES({i})"
        key2 = f"other names({i})"
        val = row.get(key1) or row.get(key2) or ""
        val = str(val).strip()
        if val:
            out.append(val)
    return out


def normalize_other_names(db_value: Any) -> List[str]:
    """Normalize JSONB other_names coming from Postgres into a Python list."""
    if db_value is None:
        return []
    if isinstance(db_value, list):
        return [str(x).strip() for x in db_value if str(x).strip()]
    if isinstance(db_value, str):
        # Sometimes psycopg may return JSON as string depending on adapters.
        try:
            parsed = json.loads(db_value)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            # Fallback: treat as single name
            s = db_value.strip()
            return [s] if s else []
    # Fallback: unknown type
    return [str(db_value).strip()]


def load_acftref_academy(acftref_path: Path) -> Dict[str, Dict[str, str]]:
    """
    Decode ACFTREF.txt mapping from MFR/MDL CODE -> {manufacturer, model}.
    Mimics the loader's "ACFTREF loaded first" step.
    """
    if not acftref_path.exists():
        logger.warning("ACFTREF.txt not found; aircraft manufacturer/model decoding will be skipped: %s", acftref_path)
        return {}

    mapping: Dict[str, Dict[str, str]] = {}
    with open(acftref_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}

        # Normalize BOM in header names
        fieldnames = [fn.strip("\ufeff").strip() if fn else fn for fn in reader.fieldnames]
        if fieldnames != reader.fieldnames:
            # Re-wrap with normalized fieldnames: simplest is to just read via a second reader
            pass

        for row in reader:
            code = (row.get("CODE") or row.get("\ufeffCODE") or row.get("code") or row.get("Code") or "").strip()
            if not code:
                continue
            manufacturer = (row.get("MFR") or row.get("mfr") or "").strip()
            model = (row.get("MODEL") or row.get("model") or "").strip()
            mapping[code] = {"manufacturer": manufacturer, "model": model}
    return mapping


@dataclass
class DbMatch:
    key_type: str  # "n_number" or "serial_number"
    db_row: Dict[str, Any]


def get_db_match(
    row: Dict[str, Any],
    db_by_n: Dict[str, Dict[str, Any]],
    db_by_serial: Dict[str, Dict[str, Any]],
) -> Optional[DbMatch]:
    n_number = (row.get("N-NUMBER") or row.get("n-number") or row.get("n_number") or "").strip()
    serial_number = (row.get("SERIAL NUMBER") or row.get("serial number") or row.get("serial_number") or "").strip()
    if n_number and n_number in db_by_n:
        return DbMatch("n_number", db_by_n[n_number])
    if serial_number and serial_number in db_by_serial:
        return DbMatch("serial_number", db_by_serial[serial_number])
    return None


def export_master_to_csv(master_path: Path, out_csv: Path, max_rows: Optional[int] = None) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(master_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"MASTER.txt has no header: {master_path}")

        # If MASTER has a trailing comma, DictReader may include an empty string field name.
        fieldnames = list(reader.fieldnames)
        with open(out_csv, "w", newline="", encoding="utf-8") as wf:
            writer = csv.DictWriter(wf, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            count = 0
            for i, row in enumerate(reader, start=1):
                if max_rows is not None and count >= max_rows:
                    break
                writer.writerow(row)
                count += 1

    logger.info("Exported %s rows from %s -> %s", (max_rows if max_rows else "all"), master_path, out_csv)


def iter_master_rows(master_path: Path, max_rows: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    with open(master_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"MASTER.txt has no header: {master_path}")
        count = 0
        for row in reader:
            if max_rows is not None and count >= max_rows:
                return
            yield row
            count += 1


def build_expected_master_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the FAA loader's "expected values" from a raw MASTER row.
    Focused on fields stored in faa_registrations (owner/registrant info).
    """
    n_number = (row.get("N-NUMBER") or row.get("n-number") or row.get("n_number") or "").strip() or None
    serial_number = (row.get("SERIAL NUMBER") or row.get("serial number") or row.get("serial_number") or "").strip() or None

    mfr_mdl_code = (row.get("MFR MDL CODE") or row.get("mfr mdl code") or row.get("mfr_mdl_code") or "").strip() or None
    eng_mfr_mdl = (row.get("ENG MFR MDL") or row.get("eng mfr mdl") or row.get("eng_mfr_mdl") or "").strip() or None

    year_mfr = parse_int(row.get("YEAR MFR") or row.get("year mfr") or row.get("year_mfr"))

    type_registrant = parse_int(row.get("TYPE REGISTRANT") or row.get("type registrant") or row.get("type_registrant"))

    registrant_name = (row.get("NAME") or row.get("name") or "").strip() or None
    street = (row.get("STREET") or row.get("street") or "").strip() or None
    street2 = (row.get("STREET2") or row.get("street2") or "").strip() or None
    city = (row.get("CITY") or row.get("city") or "").strip() or None
    state = (row.get("STATE") or row.get("state") or "").strip() or None
    zip_code = (row.get("ZIP CODE") or row.get("zip code") or row.get("zip_code") or "").strip() or None
    region = (row.get("REGION") or row.get("region") or "").strip() or None
    county = (row.get("COUNTY") or row.get("county") or "").strip() or None
    country = (row.get("COUNTRY") or row.get("country") or "").strip() or "US"

    status_code = (row.get("STATUS CODE") or row.get("status code") or row.get("status_code") or "").strip() or None
    fract_owner = (row.get("FRACT OWNER") or row.get("fract owner") or row.get("fract_owner") or "").strip() or None
    mode_s_code = (row.get("MODE S CODE") or row.get("mode s code") or row.get("mode_s_code") or "").strip() or None
    mode_s_code_hex = (row.get("MODE S CODE HEX") or row.get("mode s code hex") or row.get("mode_s_code_hex") or "").strip() or None

    air_worth_date = parse_date(row.get("AIR WORTH DATE") or row.get("air worth date") or row.get("air_worth_date"))
    last_action_date = parse_date(row.get("LAST ACTION DATE") or row.get("last action date") or row.get("last_action_date"))
    cert_issue_date = parse_date(row.get("CERT ISSUE DATE") or row.get("cert issue date") or row.get("cert_issue_date"))
    expiration_date = parse_date(row.get("EXPIRATION DATE") or row.get("expiration date") or row.get("expiration_date"))

    certification = (row.get("CERTIFICATION") or row.get("certification") or "").strip() or None

    type_aircraft = (row.get("TYPE AIRCRAFT") or row.get("type aircraft") or row.get("type_aircraft") or "").strip() or None
    type_engine = (row.get("TYPE ENGINE") or row.get("type engine") or row.get("type_engine") or "").strip() or None

    unique_id = (row.get("UNIQUE ID") or row.get("unique id") or row.get("unique_id") or "").strip() or None

    kit_field = (row.get("KIT MFR, KIT MODEL") or row.get("kit mfr, kit model") or row.get("kit_mfr_kit_model") or "").strip()
    kit_mfr = None
    kit_model = None
    if kit_field:
        # Loader splits by comma
        parts = kit_field.split(",", 1)
        if len(parts) >= 1 and parts[0].strip():
            kit_mfr = parts[0].strip()
        if len(parts) >= 2 and parts[1].strip():
            kit_model = parts[1].strip()

    other_names = parse_other_names(row, max_names=5)

    return {
        # Key-ish identifiers
        "n_number": n_number,
        "serial_number": serial_number,
        # faa_registrations fields
        "mfr_mdl_code": mfr_mdl_code,
        "eng_mfr_mdl": eng_mfr_mdl,
        "year_mfr": year_mfr,
        "type_registrant": type_registrant,
        "registrant_name": registrant_name,
        "street": street,
        "street2": street2,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "region": region,
        "county": county,
        "country": country,
        "last_action_date": last_action_date,
        "cert_issue_date": cert_issue_date,
        "certification": certification,
        "expiration_date": expiration_date,
        "air_worth_date": air_worth_date,
        "type_aircraft": type_aircraft,
        "type_engine": type_engine,
        "status_code": status_code,
        "mode_s_code": mode_s_code,
        "mode_s_code_hex": mode_s_code_hex,
        "fract_owner": fract_owner,
        "unique_id": unique_id,
        "kit_mfr": kit_mfr,
        "kit_model": kit_model,
        "other_names": other_names,
    }


def validate_master_against_db(
    master_path: Path,
    ingestion_date: str,
    report_csv: Path,
    max_rows: Optional[int] = None,
    max_mismatches: int = 2000,
    owner_only: bool = True,
) -> Dict[str, Any]:
    """
    Cross-reference MASTER rows against faa_registrations (and selected aircraft fields).
    """
    report_csv.parent.mkdir(parents=True, exist_ok=True)

    # Connect
    pc = PostgresClient()

    # Load DB rows once
    # We include aircraft columns that the FAA loader updates from MASTER.
    query = """
        SELECT
            f.id,
            f.n_number,
            f.serial_number,
            f.mfr_mdl_code,
            f.eng_mfr_mdl,
            f.year_mfr,
            f.type_registrant,
            f.registrant_name,
            f.street,
            f.street2,
            f.city,
            f.state,
            f.zip_code,
            f.region,
            f.county,
            f.country,
            f.last_action_date,
            f.cert_issue_date,
            f.certification,
            f.expiration_date,
            f.air_worth_date,
            f.type_aircraft,
            f.type_engine,
            f.status_code,
            f.mode_s_code,
            f.mode_s_code_hex,
            f.fract_owner,
            f.unique_id,
            f.kit_mfr,
            f.kit_model,
            f.other_names,
            a.registration_number AS aircraft_registration_number,
            a.manufacturer AS aircraft_manufacturer,
            a.model AS aircraft_model,
            a.manufacturer_year AS aircraft_manufacturer_year,
            a.aircraft_status AS aircraft_status,
            a.registration_country AS aircraft_registration_country,
            a.based_country AS aircraft_based_country,
            a.airworthiness_date AS aircraft_airworthiness_date,
            a.certification AS aircraft_certification,
            a.type_aircraft AS aircraft_type_aircraft,
            a.type_engine AS aircraft_type_engine,
            a.mode_s_code AS aircraft_mode_s_code,
            a.mode_s_code_hex AS aircraft_mode_s_code_hex
        FROM faa_registrations f
        LEFT JOIN aircraft a ON a.id = f.aircraft_id
        WHERE f.ingestion_date = %s
    """
    db_rows = pc.execute_query(query, (ingestion_date,))

    db_by_n: Dict[str, Dict[str, Any]] = {}
    db_by_serial: Dict[str, Dict[str, Any]] = {}
    for r in db_rows:
        n = (r.get("n_number") or "").strip()
        s = (r.get("serial_number") or "").strip()
        if n and n not in db_by_n:
            db_by_n[n] = r
        if s and s not in db_by_serial:
            db_by_serial[s] = r

    fields_to_compare_owner_only = [
        "registrant_name",
        "street",
        "street2",
        "city",
        "state",
        "zip_code",
        "region",
        "county",
        "country",
    ]
    fields_to_compare_full = [
        "mfr_mdl_code",
        "eng_mfr_mdl",
        "year_mfr",
        "type_registrant",
        "registrant_name",
        "street",
        "street2",
        "city",
        "state",
        "zip_code",
        "region",
        "county",
        "country",
        "last_action_date",
        "cert_issue_date",
        "certification",
        "expiration_date",
        "air_worth_date",
        "type_aircraft",
        "type_engine",
        "status_code",
        "mode_s_code",
        "mode_s_code_hex",
        "fract_owner",
        "unique_id",
        "kit_mfr",
        "kit_model",
        "other_names",
    ]
    fields_to_compare = fields_to_compare_owner_only if owner_only else fields_to_compare_full

    # Report
    mismatch_rows: List[Dict[str, Any]] = []
    counts = {
        "master_rows_processed": 0,
        "db_matches_found": 0,
        "missing_in_db": 0,
        "mismatch_rows": 0,
        "mismatches_total_fields": 0,
    }
    missing_rows_written = 0

    # Write header early
    with open(report_csv, "w", newline="", encoding="utf-8") as wf:
        writer = csv.DictWriter(
            wf,
            fieldnames=[
                "master_row_index",
                "n_number",
                "serial_number",
                "match_key",
                "missing_in_db",
                "mismatched_fields",
                "expected_json",
                "actual_json",
            ],
        )
        writer.writeheader()

        for idx, master_row in enumerate(iter_master_rows(master_path, max_rows=max_rows), start=1):
            counts["master_rows_processed"] += 1

            expected = build_expected_master_fields(master_row)
            match = get_db_match(master_row, db_by_n=db_by_n, db_by_serial=db_by_serial)
            if match is None:
                counts["missing_in_db"] += 1
                if missing_rows_written < max_mismatches:
                    writer.writerow(
                        {
                            "master_row_index": idx,
                            "n_number": expected["n_number"],
                            "serial_number": expected["serial_number"],
                            "match_key": "",
                            "missing_in_db": True,
                            "mismatched_fields": "",
                            "expected_json": "",
                            "actual_json": "",
                        }
                    )
                    missing_rows_written += 1
                continue

            counts["db_matches_found"] += 1
            db_row = match.db_row

            mismatched: List[str] = []
            expected_comp: Dict[str, Any] = {}
            actual_comp: Dict[str, Any] = {}

            # Compare faa_registrations fields
            for f in fields_to_compare:
                exp_val = expected.get(f)
                act_val = db_row.get(f)

                if f == "other_names":
                    exp_norm = normalize_other_names(exp_val)
                    act_norm = normalize_other_names(act_val)
                    if exp_norm != act_norm:
                        mismatched.append(f)
                    expected_comp[f] = exp_norm
                    actual_comp[f] = act_norm
                    continue

                # For dates, psycopg returns date objects. exp_val is date or None.
                if exp_val == "":
                    exp_val = None
                if act_val == "":
                    act_val = None

                if exp_val != act_val:
                    # Normalize list/dict to JSON for readability
                    mismatched.append(f)
                expected_comp[f] = exp_val
                actual_comp[f] = act_val

            if mismatched:
                counts["mismatch_rows"] += 1
                counts["mismatches_total_fields"] += len(mismatched)

                if counts["mismatch_rows"] <= max_mismatches:
                    writer.writerow(
                        {
                            "master_row_index": idx,
                            "n_number": expected["n_number"],
                            "serial_number": expected["serial_number"],
                            "match_key": match.key_type,
                            "missing_in_db": False,
                            "mismatched_fields": ";".join(mismatched),
                            "expected_json": json.dumps(expected_comp, ensure_ascii=False)[:2000],
                            "actual_json": json.dumps(actual_comp, ensure_ascii=False)[:2000],
                        }
                    )

            if idx % 5000 == 0:
                logger.info(
                    "Progress: %s master rows processed, matches=%s, missing=%s, mismatches=%s",
                    idx,
                    counts["db_matches_found"],
                    counts["missing_in_db"],
                    counts["mismatch_rows"],
                )

    logger.info("Validation complete: %s", counts)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Export FAA MASTER.txt and validate vs Postgres.")
    parser.add_argument(
        "--master-path",
        type=str,
        required=True,
        help="Path to FAA MASTER.txt extracted file",
    )
    parser.add_argument(
        "--ingestion-date",
        type=str,
        required=True,
        help="Ingestion date used in DB (YYYY-MM-DD). Used to select faa_registrations rows.",
    )
    parser.add_argument(
        "--export-csv",
        type=str,
        default="",
        help="If set, export MASTER.txt raw rows to this CSV path.",
    )
    parser.add_argument(
        "--validate-db",
        action="store_true",
        help="If set, cross-reference MASTER.txt rows against faa_registrations for ingestion_date.",
    )
    parser.add_argument(
        "--report-csv",
        type=str,
        default="",
        help="Report CSV path for validation mismatches.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Process only first N MASTER rows (useful for testing).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Compare all FAA loader fields (owner+aircraft-related fields). Default is owner-only (registrant+address).",
    )
    parser.add_argument(
        "--max-mismatches",
        type=int,
        default=2000,
        help="Max mismatch rows to write into report CSV (avoid huge output).",
    )

    args = parser.parse_args()

    master_path = Path(args.master_path)
    if not master_path.exists():
        raise FileNotFoundError(f"MASTER.txt not found: {master_path}")

    if args.export_csv:
        export_master_to_csv(master_path, Path(args.export_csv), max_rows=args.max_rows)

    if args.validate_db:
        if not args.report_csv:
            raise ValueError("--report-csv is required when --validate-db is set")
        validate_master_against_db(
            master_path=master_path,
            ingestion_date=args.ingestion_date,
            report_csv=Path(args.report_csv),
            max_rows=args.max_rows,
            max_mismatches=args.max_mismatches,
            owner_only=not args.full,
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()

