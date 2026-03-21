from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2
from psycopg2 import errorcodes
from dotenv import load_dotenv


def _norm_header(s: str) -> str:
    # Normalize CSV headers from MASTER export (strip whitespace + BOM)
    return s.strip().lstrip("\ufeff")


def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip()


def parse_yyyymmdd(value: Optional[str]) -> Optional[date]:
    s = _norm(value)
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    # Best-effort: let Postgres parse if you want, but for QA we return None on unknown formats.
    return None


def parse_int(value: Optional[str]) -> Optional[int]:
    s = _norm(value)
    if not s:
        return None
    # MASTER sometimes has float-ish fields; strip to int safely.
    try:
        return int(float(s.replace(",", "")))
    except Exception:
        return None


def parse_other_names(row: Dict[str, str], get: callable, max_names: int = 5) -> list[str]:
    out: list[str] = []
    for i in range(1, max_names + 1):
        # CSV header format: OTHER NAMES(1) ... OTHER NAMES(5)
        key = f"OTHER NAMES({i})"
        val = _norm(get(key))
        if val:
            out.append(val)
    return out


def build_get_by_normalized_header(fieldnames: list[str], row: Dict[str, Any]) -> callable:
    mapping: Dict[str, str] = {_norm_header(k): k for k in fieldnames}

    def _get(header_normalized: str) -> Optional[str]:
        original_key = mapping.get(_norm_header(header_normalized))
        if not original_key:
            return None
        return row.get(original_key)

    return _get


def chunked(seq: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), chunk_size):
        yield seq[i : i + chunk_size]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill missing faa_registrations rows using exported MASTER CSV."
            " Existence check is by (serial_number, mfr_mdl_code) ONLY (no n_number)."
            " n_number is filled/updated from CSV only when inserting a missing row."
        )
    )
    parser.add_argument("--master-csv", required=True, type=str, help="Path to exported faa_master_YYYY-MM-DD.csv")
    parser.add_argument("--ingestion-date", required=True, type=str, help="faa_registrations ingestion_date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes, only report counts.")
    parser.add_argument("--progress-every", type=int, default=5000, help="Log progress every N processed CSV rows.")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level: DEBUG, INFO, WARNING, ERROR")
    parser.add_argument("--master-offset", type=int, default=0, help="Skip the first N rows from the Master CSV (for chunked backfill).")
    parser.add_argument("--max-rows", type=int, default=None, help="Limit processed CSV rows (for testing).")
    parser.add_argument("--missing-only", action="store_true", help="If set, only insert truly missing signatures.")
    parser.add_argument("--batch-commit-every", type=int, default=200, help="Commit after N inserts (non-dry-run).")
    parser.add_argument("--report-csv", type=str, default="", help="Optional path to write a backfill report (missing inserts).")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not found (expected etl-pipeline/.env)")

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("backfill_missing_faa_registrations_from_master_csv")

    logger.info(
        "Starting backfill: master_csv=%s ingestion_date=%s dry_run=%s missing_only=%s master_offset=%s max_rows=%s batch_commit_every=%s",
        args.master_csv,
        args.ingestion_date,
        args.dry_run,
        args.missing_only,
        args.master_offset,
        args.max_rows,
        args.batch_commit_every,
    )

    master_csv_path = Path(args.master_csv)
    if not master_csv_path.exists():
        raise FileNotFoundError(master_csv_path)

    ingestion_date = args.ingestion_date

    # Preload DB rows for existence check: (serial_number, mfr_mdl_code) signatures.
    # Also preload a mapping for aircraft_id by serial_number to reduce inserts.
    # Existence/signature check (NO n_number):
    # (serial_number, mfr_mdl_code, registrant_name)
    db_sig_to_row: Dict[tuple[str, str, str], dict] = {}
    aircraft_id_by_serial: Dict[str, str] = {}

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id,
                  aircraft_id,
                  serial_number,
                  mfr_mdl_code,
                  n_number,
                  registrant_name
                FROM faa_registrations
                WHERE ingestion_date = %s
                  AND serial_number IS NOT NULL
                  AND TRIM(serial_number) <> ''
                """,
                (ingestion_date,),
            )
            for rid, aircraft_id, serial_number, mfr_mdl_code, n_number, registrant_name in cur.fetchall():
                s = _norm(serial_number)
                code = _norm(mfr_mdl_code)
                if not s or not code:
                    continue
                name = _norm(registrant_name)
                sig = (s, code, name)
                db_sig_to_row[sig] = {"id": rid, "aircraft_id": aircraft_id, "n_number": n_number}

            # Preload aircraft ids (best effort): choose the newest aircraft row for that serial.
            cur.execute(
                """
                SELECT id, serial_number
                FROM aircraft
                WHERE serial_number IS NOT NULL
                  AND TRIM(serial_number) <> ''
                """
            )
            aircraft_rows = cur.fetchall()
            for aid, s in aircraft_rows:
                s_norm = _norm(s)
                if not s_norm:
                    continue
                # Keep first seen; aircraft duplicates are possible, but we minimize churn.
                aircraft_id_by_serial.setdefault(s_norm, aid)

    logger.info(
        "Preloaded signatures=%s and aircraft_by_serial=%s for ingestion_date=%s",
        len(db_sig_to_row),
        len(aircraft_id_by_serial),
        args.ingestion_date,
    )

    inserts_done = 0
    inserts_skipped_existing = 0
    inserts_new_aircraft = 0
    inserts_n_number_conflict = 0

    report_rows: list[dict] = []

    def ensure_aircraft_id(cur, serial_number: str, n_number: Optional[str], *, dry_run: bool) -> tuple[Optional[str], bool]:
        # Prefer cached aircraft_id_by_serial.
        cached = aircraft_id_by_serial.get(serial_number)
        if cached:
            return str(cached), False

        if dry_run:
            # Dry-run: do not create new aircraft rows.
            return None, False

        # Create a new aircraft row if not found.
        # Note: we only set minimal fields to avoid relying on ACFTREF decoding.
        savepoint_name = "sp_aircraft_insert"
        cur.execute(f"SAVEPOINT {savepoint_name}")
        new_id = None
        try:
            insert_sql = """
                INSERT INTO aircraft (serial_number, registration_number)
                VALUES (%s, %s)
                RETURNING id
            """
            cur.execute(insert_sql, (serial_number, n_number if n_number else None))
            new_id = cur.fetchone()[0]
        except psycopg2.Error as e:
            if e.pgcode == errorcodes.UNIQUE_VIOLATION:
                # Common case: registration_number already exists in aircraft.
                # Retry with NULL registration_number so backfill can continue.
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                cur.execute(
                    """
                    INSERT INTO aircraft (serial_number, registration_number)
                    VALUES (%s, NULL)
                    RETURNING id
                    """,
                    (serial_number,),
                )
                new_id = cur.fetchone()[0]
            else:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                raise
        finally:
            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")

        if new_id is None:
            raise RuntimeError("Failed to create/find aircraft_id during backfill.")
        aircraft_id_by_serial[serial_number] = new_id
        return str(new_id), True

    # Backfill loop
    # Unique constraint protection:
    # faa_registrations has UNIQUE(n_number, ingestion_date).
    # n_number is not part of our "missing signature" match, but we must avoid violating the constraint.
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            processed = 0
            master_seen = 0
            inserts_since_commit = 0

            # Preload existing n_number values for this ingestion snapshot.
            cur.execute(
                """
                SELECT n_number
                FROM faa_registrations
                WHERE ingestion_date = %s
                  AND n_number IS NOT NULL
                  AND TRIM(n_number) <> ''
                """,
                (ingestion_date,),
            )
            db_n_numbers_set = {_norm(r[0]) for r in cur.fetchall()}

            with open(master_csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise RuntimeError(f"MASTER CSV has no header: {master_csv_path}")

                get = None  # set per-row
                try:
                    for row in reader:
                        master_seen += 1
                        if master_seen <= int(args.master_offset):
                            continue
                        processed += 1
                        if args.max_rows is not None and processed > args.max_rows:
                            break

                        if processed % max(1, int(args.progress_every)) == 0:
                            logger.info(
                                "Progress: processed=%s (master_seen=%s offset=%s max_rows=%s) inserted=%s skipped_existing=%s new_aircraft=%s n_number_conflict=%s dry_run=%s",
                                processed,
                                master_seen,
                                args.master_offset,
                                args.max_rows,
                                inserts_done,
                                inserts_skipped_existing,
                                inserts_new_aircraft,
                                inserts_n_number_conflict,
                                args.dry_run,
                            )

                        if not get:
                            get = build_get_by_normalized_header(reader.fieldnames, row)

                        # Normalize per-row getter because row changes.
                        get = build_get_by_normalized_header(reader.fieldnames, row)

                        serial_number = _norm(get("SERIAL NUMBER"))
                        mfr_mdl_code = _norm(get("MFR MDL CODE"))
                        if not serial_number or not mfr_mdl_code:
                            continue

                        n_number = _norm(get("N-NUMBER"))
                        # Keep n_number as None if blank (schema allows null).
                        n_number_to_store = n_number if n_number else None
                        n_number_to_store_insert = n_number_to_store
                        if n_number_to_store_insert and n_number_to_store_insert in db_n_numbers_set:
                            # Insert with n_number=NULL to satisfy unique constraint.
                            inserts_n_number_conflict += 1
                            n_number_to_store_insert = None

                        registrant_name = _norm(get("NAME"))
                        sig = (serial_number, mfr_mdl_code, registrant_name)
                        if sig in db_sig_to_row:
                            inserts_skipped_existing += 1
                            continue

                        # If user wants missing-only, this is the definition already.
                        if args.missing_only and sig not in db_sig_to_row:
                            pass

                        aircraft_id, created_new_aircraft = ensure_aircraft_id(
                            cur,
                            serial_number,
                            n_number_to_store_insert,
                            dry_run=args.dry_run,
                        )
                        if created_new_aircraft:
                            inserts_new_aircraft += 1

                        if args.dry_run and not aircraft_id:
                            # In dry-run mode we skip creating registrations when aircraft_id would require writes.
                            if args.report_csv:
                                report_rows.append(
                                    {
                                        "serial_number": serial_number,
                                        "mfr_mdl_code": mfr_mdl_code,
                                        "inserted": False,
                                        "reason": "missing_aircraft_row_in_dry_run",
                                        "n_number": n_number_to_store or "",
                                        "registrant_name": registrant_name,
                                    }
                                )
                            continue

                        # owner/registrant info from MASTER CSV
                        street = _norm(get("STREET")) or None
                        street2 = _norm(get("STREET2")) or None
                        city = _norm(get("CITY")) or None
                        state = _norm(get("STATE")) or None
                        zip_code = _norm(get("ZIP CODE")) or None
                        region = _norm(get("REGION")) or None
                        county = _norm(get("COUNTY")) or None
                        country = _norm(get("COUNTRY")) or "US"

                        type_registrant = parse_int(get("TYPE REGISTRANT"))
                        eng_mfr_mdl = _norm(get("ENG MFR MDL")) or None
                        year_mfr = parse_int(get("YEAR MFR"))

                        last_action_date = parse_yyyymmdd(get("LAST ACTION DATE"))
                        cert_issue_date = parse_yyyymmdd(get("CERT ISSUE DATE"))
                        certification = _norm(get("CERTIFICATION")) or None
                        expiration_date = parse_yyyymmdd(get("EXPIRATION DATE"))
                        air_worth_date = parse_yyyymmdd(get("AIR WORTH DATE"))

                        type_aircraft = _norm(get("TYPE AIRCRAFT")) or None
                        type_engine = _norm(get("TYPE ENGINE")) or None

                        status_code = _norm(get("STATUS CODE")) or None
                        mode_s_code = _norm(get("MODE S CODE")) or None
                        mode_s_code_hex = _norm(get("MODE S CODE HEX")) or None
                        fract_owner = _norm(get("FRACT OWNER")) or None

                        unique_id = _norm(get("UNIQUE ID")) or None
                        kit_mfr = _norm(get("KIT MFR")) or None
                        kit_model = _norm(get("KIT MODEL")) or None

                        other_names = parse_other_names(
                            row=row,
                            get=get,
                            max_names=5,
                        )
                        other_names_json = json.dumps(other_names) if other_names else None

                        # Insert FAA registration.
                        insert_sql = """
                            INSERT INTO faa_registrations (
                                aircraft_id,
                                n_number,
                                serial_number,
                                mfr_mdl_code,
                                eng_mfr_mdl,
                                year_mfr,
                                type_registrant,
                                registrant_name,
                                street,
                                street2,
                                city,
                                state,
                                zip_code,
                                region,
                                county,
                                country,
                                last_action_date,
                                cert_issue_date,
                                certification,
                                expiration_date,
                                air_worth_date,
                                type_aircraft,
                                type_engine,
                                status_code,
                                mode_s_code,
                                mode_s_code_hex,
                                fract_owner,
                                unique_id,
                                kit_mfr,
                                kit_model,
                                other_names,
                                ingestion_date
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        params = (
                            aircraft_id,
                            n_number_to_store_insert,
                            serial_number,
                            mfr_mdl_code,
                            eng_mfr_mdl,
                            year_mfr,
                            type_registrant,
                            registrant_name or None,
                            street,
                            street2,
                            city,
                            state,
                            zip_code,
                            region,
                            county,
                            country,
                            last_action_date,
                            cert_issue_date,
                            certification,
                            expiration_date,
                            air_worth_date,
                            type_aircraft,
                            type_engine,
                            status_code,
                            mode_s_code,
                            mode_s_code_hex,
                            fract_owner,
                            unique_id,
                            kit_mfr,
                            kit_model,
                            other_names_json,
                            ingestion_date,
                        )

                        if not args.dry_run:
                            cur.execute(insert_sql, params)
                            if n_number_to_store_insert:
                                db_n_numbers_set.add(n_number_to_store_insert)

                        inserts_done += 1
                        inserts_since_commit += 1

                        if args.dry_run:
                            # No commit needed, just record.
                            pass
                        else:
                            if inserts_since_commit >= args.batch_commit_every:
                                conn.commit()
                                inserts_since_commit = 0

                        db_sig_to_row[sig] = {"id": None, "aircraft_id": aircraft_id, "n_number": n_number_to_store}

                        if args.report_csv:
                            report_rows.append(
                                {
                                    "serial_number": serial_number,
                                    "mfr_mdl_code": mfr_mdl_code,
                                    "inserted": True,
                                    "dry_run": args.dry_run,
                                    "n_number": n_number_to_store or "",
                                    "registrant_name": registrant_name,
                                }
                            )
                except KeyboardInterrupt:
                    logger.warning(
                        "Interrupted by user. processed=%s inserts_done=%s skipped_existing=%s dry_run=%s. Exiting.",
                        processed,
                        inserts_done,
                        inserts_skipped_existing,
                        args.dry_run,
                    )
                    raise

            if not args.dry_run:
                conn.commit()

    if args.report_csv:
        out_path = Path(args.report_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as wf:
            w = csv.DictWriter(wf, fieldnames=list(report_rows[0].keys()) if report_rows else ["serial_number", "mfr_mdl_code"])
            w.writeheader()
            for r in report_rows:
                w.writerow(r)

    print(
        {
            "processed_csv_rows": processed,
            "db_signatures_preloaded": len(db_sig_to_row),
            "inserts_done": inserts_done,
            "skipped_existing": inserts_skipped_existing,
            "new_aircraft_created": inserts_new_aircraft,
            "n_number_conflicts_resolved": inserts_n_number_conflict,
            "dry_run": args.dry_run,
        }
    )


if __name__ == "__main__":
    main()

