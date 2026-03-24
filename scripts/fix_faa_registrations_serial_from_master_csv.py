#!/usr/bin/env python3
"""
Fix ``faa_registrations.serial_number`` when it was mistakenly set to the **N-number** (tail)
instead of the real **SERIAL NUMBER** from the FAA Releasable Aircraft Database (MASTER).

**Detection (default):** for a given ``ingestion_date``, a row is "wrong" when the stored
``serial_number`` matches the stored ``n_number`` after the same normalization used for
comparison (typical copy-paste / column swap mistake).

**Source of truth:** exported MASTER CSV (same layout as other ETL scripts: headers like
``N-NUMBER``, ``SERIAL NUMBER``, ``MFR MDL CODE``).

**Safety:** without ``--apply``, only prints a report. With ``--apply``, runs ``UPDATE`` per row.

Usage::

    cd etl-pipeline
    python scripts/fix_faa_registrations_serial_from_master_csv.py \\
        --master-csv path/to/faa_master_2025-01-15.csv \\
        --ingestion-date 2025-01-15

    # Actually write fixes:
    python scripts/fix_faa_registrations_serial_from_master_csv.py \\
        --master-csv path/to/faa_master_2025-01-15.csv \\
        --ingestion-date 2025-01-15 --apply

    # Optional: also fix rows where serial != CSV serial (even if serial != n_number)
    python scripts/... --apply --fix-when-csv-differs

Environment: ``POSTGRES_CONNECTION_STRING`` in ``etl-pipeline/.env`` (or ``backend/.env``).
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv


def _norm_header(s: str) -> str:
    return s.strip().lstrip("\ufeff")


def _norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _reg_key(registration: Optional[str]) -> str:
    """Comparable key for N-number (strip noise, upper, no hyphens)."""
    if not registration:
        return ""
    t = registration.strip().upper()
    t = re.sub(r"\([^)]*\)", "", t).strip()
    parts = t.split()
    t = parts[0] if parts else t
    return t.replace("-", "").replace(" ", "")


def _serial_wrong_equals_tail(serial: str, n_number: str) -> bool:
    """True when serial looks like the tail was stored in the serial column."""
    sk = _reg_key(serial)
    nk = _reg_key(n_number)
    if not sk or not nk:
        return False
    if sk == nk:
        return True
    # Sometimes serial stored without leading N
    if nk.startswith("N") and sk == nk[1:]:
        return True
    if sk.startswith("N") and nk == sk[1:]:
        return True
    return False


def build_get_by_normalized_header(fieldnames: List[str], row: Dict[str, Any]) -> Callable[[str], Optional[str]]:
    mapping: Dict[str, str] = {_norm_header(k): k for k in fieldnames}

    def _get(header_normalized: str) -> Optional[str]:
        original_key = mapping.get(_norm_header(header_normalized))
        if not original_key:
            return None
        return row.get(original_key)

    return _get


def _get_serial_from_row(get: Callable[[str], Optional[str]]) -> str:
    for h in ("SERIAL NUMBER", "SERIAL-NUMBER", "SERIAL_NUMBER", "serial number"):
        v = _norm(get(h))
        if v:
            return v
    return ""


def _get_n_from_row(get: Callable[[str], Optional[str]]) -> str:
    for h in ("N-NUMBER", "N-NUMBER ", "N_NUMBER", "n-number"):
        v = _norm(get(h))
        if v:
            return v
    return ""


def load_master_index(master_csv: Path) -> Tuple[Dict[str, str], int]:
    """
    Map registration key -> authoritative SERIAL NUMBER from CSV.
    If duplicate N-NUMBER rows exist, last row wins (log at debug).
    """
    by_tail: Dict[str, str] = {}
    rows_read = 0
    with open(master_csv, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"No header in {master_csv}")
        for row in reader:
            rows_read += 1
            get = build_get_by_normalized_header(reader.fieldnames, row)
            n_raw = _get_n_from_row(get)
            serial = _get_serial_from_row(get)
            if not n_raw or not serial:
                continue
            k = _reg_key(n_raw)
            if not k:
                continue
            by_tail[k] = serial
    return by_tail, rows_read


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--master-csv", required=True, type=str, help="Path to FAA MASTER export CSV")
    parser.add_argument("--ingestion-date", required=True, type=str, help="faa_registrations.ingestion_date (YYYY-MM-DD)")
    parser.add_argument("--apply", action="store_true", help="Write UPDATEs (default is dry-run report only)")
    parser.add_argument(
        "--fix-when-csv-differs",
        action="store_true",
        help="Also update when CSV serial differs from DB serial (even if serial != n_number). "
        "Use carefully; narrows to rows whose N-number exists in MASTER.",
    )
    parser.add_argument("--report-csv", type=str, default="", help="Optional path to write changed rows CSV")
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("fix_faa_serial")

    etl = Path(__file__).resolve().parents[1]
    repo = etl.parent
    for p in (etl / ".env", repo / "backend" / ".env", repo / ".env"):
        if p.exists():
            load_dotenv(p, override=False)

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        log.error("POSTGRES_CONNECTION_STRING not set")
        return 1

    master_path = Path(args.master_csv)
    if not master_path.exists():
        log.error("File not found: %s", master_path)
        return 1

    log.info("Loading MASTER index from %s", master_path)
    by_tail, n_csv = load_master_index(master_path)
    log.info("MASTER rows read=%s, N-number keys with serial=%s", n_csv, len(by_tail))

    report_lines: List[Dict[str, str]] = []

    with psycopg2.connect(conn_str) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, n_number, serial_number, mfr_mdl_code
                FROM faa_registrations
                WHERE ingestion_date = %s
                  AND n_number IS NOT NULL
                  AND TRIM(n_number) <> ''
                """,
                (args.ingestion_date,),
            )
            db_rows = cur.fetchall()

        scanned = 0
        wrong_detected = 0
        no_csv = 0
        csv_empty_serial = 0
        already_ok = 0
        updated = 0

        for rid, n_number, serial_number, mfr_mdl_code in db_rows:
            scanned += 1
            sn = _norm(serial_number)
            nn = _norm(n_number)
            key = _reg_key(nn)
            official = by_tail.get(key) if key else ""

            wrong_tail = _serial_wrong_equals_tail(sn, nn)
            should_fix = False
            reason = ""

            if args.fix_when_csv_differs and key and official:
                if sn != official:
                    should_fix = True
                    reason = "csv_differs"
            elif wrong_tail:
                should_fix = True
                reason = "serial_equals_tail"
            else:
                already_ok += 1
                continue

            if not key:
                no_csv += 1
                continue
            if not official:
                if wrong_tail:
                    no_csv += 1
                else:
                    csv_empty_serial += 1
                continue

            if sn == official:
                already_ok += 1
                continue

            wrong_detected += 1
            log.info(
                "Fix id=%s n_number=%r serial_db=%r -> serial_csv=%r (%s)",
                str(rid),
                nn,
                sn,
                official,
                reason,
            )
            report_lines.append(
                {
                    "id": str(rid),
                    "n_number": nn,
                    "serial_before": sn,
                    "serial_after": official,
                    "mfr_mdl_code": _norm(mfr_mdl_code),
                    "reason": reason,
                }
            )

            if args.apply:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE faa_registrations
                        SET serial_number = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (official, rid),
                    )
                    updated += cur.rowcount
                conn.commit()

        log.info(
            "Done apply=%s: scanned=%s wrong_or_diff=%s updated=%s no_master_match=%s csv_empty_serial=%s skipped_already_ok=%s",
            args.apply,
            scanned,
            wrong_detected,
            updated if args.apply else 0,
            no_csv,
            csv_empty_serial,
            already_ok,
        )

    if args.report_csv and report_lines:
        out_path = Path(args.report_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(report_lines[0].keys()))
            w.writeheader()
            w.writerows(report_lines)
        log.info("Wrote report %s", out_path)

    if not args.apply and report_lines:
        print("\nDry-run only. Re-run with --apply to write", len(report_lines), "UPDATE(s).")
    elif args.apply:
        print("\nApplied", len(report_lines), "serial_number fix(es).")
    else:
        print("\nNo rows matched fix criteria.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
