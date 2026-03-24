#!/usr/bin/env python3
"""
Fix ``faa_registrations.n_number`` when it does not match the FAA MASTER export, for rows
where **serial and N-number were mistakenly duplicated** (detection: ``serial_number`` equals
``n_number`` after the same normalization as the serial-fix script).

**Assumption:** ``serial_number`` in the database is already the real airframe serial from MASTER;
only ``n_number`` needs to be replaced with the **N-NUMBER** from the CSV row whose
**SERIAL NUMBER** matches.

**Source of truth:** exported MASTER CSV (headers ``N-NUMBER``, ``SERIAL NUMBER``, optional
``MFR MDL CODE``).

**Safety:** default is dry-run. ``--apply`` runs ``UPDATE`` per row.

**Unique constraint:** ``faa_registrations`` has ``UNIQUE(n_number, ingestion_date)``. If the
correct N-number is already taken by another row for the same ingestion date, the script
logs a conflict and skips (no update).

**Serial matching (default):** **Exact** string after trim + upper only — ``00174`` and ``174``
are **different** keys. Optional ``--allow-leading-zero-alias`` treats digit serials as equal
when one is the other with leading zeros stripped (legacy behavior).

Usage::

    cd etl-pipeline
    python scripts/fix_faa_registrations_n_number_from_master_csv.py \\
        --master-csv path/to/faa_master.csv \\
        --ingestion-date 2025-01-15

    python scripts/fix_faa_registrations_n_number_from_master_csv.py \\
        --master-csv path/to/faa_master.csv \\
        --ingestion-date 2025-01-15 --apply

Environment: ``POSTGRES_CONNECTION_STRING`` in ``etl-pipeline/.env`` (or ``backend/.env``).
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
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
    if not registration:
        return ""
    t = registration.strip().upper()
    t = re.sub(r"\([^)]*\)", "", t).strip()
    parts = t.split()
    t = parts[0] if parts else t
    return t.replace("-", "").replace(" ", "")


def _serial_wrong_equals_tail(serial: str, n_number: str) -> bool:
    """Same detection as fix_faa_registrations_serial_from_master_csv.py."""
    sk = _reg_key(serial)
    nk = _reg_key(n_number)
    if not sk or not nk:
        return False
    if sk == nk:
        return True
    if nk.startswith("N") and sk == nk[1:]:
        return True
    if sk.startswith("N") and nk == sk[1:]:
        return True
    return False


def _serial_index_keys(serial: str, *, allow_leading_zero_alias: bool = False) -> List[str]:
    """
    Keys to match DB serial to CSV SERIAL NUMBER (trim + upper).

    By default **only** the exact normalized form — ``00174`` ≠ ``174``.
    With ``allow_leading_zero_alias=True``, also index a second key for digit-only serials
    with leading zeros stripped (optional legacy behavior).
    """
    s = _norm(serial).upper()
    if not s:
        return []
    out: List[str] = [s]
    if allow_leading_zero_alias and s.isdigit():
        t = s.lstrip("0") or "0"
        if t != s and t not in out:
            out.append(t)
    return out


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


def _get_mfr_from_row(get: Callable[[str], Optional[str]]) -> str:
    for h in ("MFR MDL CODE", "MFR-MDL-CODE", "MFR_MDL_CODE", "mfr mdl code"):
        v = _norm(get(h))
        if v:
            return v
    return ""


def load_master_serial_to_n(
    master_csv: Path,
    *,
    max_sample_warnings: int = 12,
    allow_leading_zero_alias: bool = False,
) -> Tuple[Dict[str, Tuple[str, str]], int, int, List[str]]:
    """
    Map serial lookup key -> (authoritative N-NUMBER as in CSV, mfr_mdl_code).
    Last CSV row wins per key.

    Returns (index, rows_read, duplicate_key_collision_count, sample_warning_lines).
    """
    by_serial_key: Dict[str, Tuple[str, str]] = {}
    rows_read = 0
    dup_collisions = 0
    samples: List[str] = []

    with open(master_csv, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"No header in {master_csv}")
        for row in reader:
            rows_read += 1
            get = build_get_by_normalized_header(reader.fieldnames, row)
            n_raw = _get_n_from_row(get)
            serial = _get_serial_from_row(get)
            mfr = _get_mfr_from_row(get)
            if not n_raw or not serial:
                continue
            for key in _serial_index_keys(serial, allow_leading_zero_alias=allow_leading_zero_alias):
                prev = by_serial_key.get(key)
                if prev and prev[0] != n_raw:
                    dup_collisions += 1
                    if len(samples) < max_sample_warnings:
                        samples.append(
                            f"key {key!r}: N {prev[0]!r} vs {n_raw!r} (last row wins for that key)"
                        )
                by_serial_key[key] = (n_raw, mfr)

    return by_serial_key, rows_read, dup_collisions, samples


def _lookup_n_for_db_serial(
    by_serial_key: Dict[str, Tuple[str, str]],
    db_serial: str,
    db_mfr: Optional[str],
    require_mfr: bool,
    *,
    allow_leading_zero_alias: bool = False,
) -> Optional[Tuple[str, str]]:
    """Return (n_from_csv, mfr_from_csv) or None."""
    db_mfr_n = _norm(db_mfr).upper()
    candidates: List[Tuple[str, str]] = []
    for key in _serial_index_keys(db_serial, allow_leading_zero_alias=allow_leading_zero_alias):
        hit = by_serial_key.get(key)
        if hit:
            candidates.append(hit)
            break
    if not candidates:
        return None
    n_csv, mfr_csv = candidates[0]
    if require_mfr and db_mfr_n and _norm(mfr_csv).upper() != db_mfr_n:
        return None
    return (n_csv, mfr_csv)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--master-csv", required=True, type=str, help="Path to FAA MASTER export CSV")
    parser.add_argument("--ingestion-date", required=True, type=str, help="faa_registrations.ingestion_date (YYYY-MM-DD)")
    parser.add_argument("--apply", action="store_true", help="Write UPDATEs (default dry-run)")
    parser.add_argument(
        "--require-mfr-match",
        action="store_true",
        help="Only apply when CSV MFR MDL CODE equals DB mfr_mdl_code (after trim/upper).",
    )
    parser.add_argument(
        "--allow-leading-zero-alias",
        action="store_true",
        help="Also match digit serials when one form has leading zeros (00174 vs 174). Default is exact match only.",
    )
    parser.add_argument("--report-csv", type=str, default="", help="Optional path to write report CSV")
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("fix_faa_n_number")

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

    log.info("Loading MASTER serial -> N-number index from %s", master_path)
    by_serial_key, n_csv, dup_collisions, sample_warns = load_master_serial_to_n(
        master_path,
        allow_leading_zero_alias=bool(args.allow_leading_zero_alias),
    )
    if dup_collisions:
        log.warning(
            "MASTER index: %s key collisions (different N-numbers mapped to same lookup key; last CSV row wins). "
            "Common if --allow-leading-zero-alias merges 00174/174. Sample:",
            dup_collisions,
        )
        for w in sample_warns:
            log.warning("  %s", w)
        if dup_collisions > len(sample_warns):
            log.warning("  ... and %s more collisions not shown", dup_collisions - len(sample_warns))
    log.info(
        "MASTER rows read=%s, serial lookup keys=%s (serial match: %s)",
        n_csv,
        len(by_serial_key),
        "exact trim+upper + leading-zero alias" if args.allow_leading_zero_alias else "exact trim+upper only",
    )

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
                  AND serial_number IS NOT NULL
                  AND TRIM(serial_number) <> ''
                """,
                (args.ingestion_date,),
            )
            db_rows = cur.fetchall()

        scanned = 0
        fixed = 0
        skipped_not_bad_pattern = 0
        no_master = 0
        already_correct = 0
        conflict = 0

        for rid, n_number, serial_number, mfr_mdl_code in db_rows:
            scanned += 1
            sn = _norm(serial_number)
            nn = _norm(n_number)
            if not _serial_wrong_equals_tail(sn, nn):
                skipped_not_bad_pattern += 1
                continue

            hit = _lookup_n_for_db_serial(
                by_serial_key,
                sn,
                mfr_mdl_code,
                args.require_mfr_match,
                allow_leading_zero_alias=bool(args.allow_leading_zero_alias),
            )
            if not hit:
                no_master += 1
                log.debug("No MASTER row for serial=%r mfr=%r", sn, mfr_mdl_code)
                continue

            official_n, _mfr_csv = hit
            if _reg_key(official_n) == _reg_key(nn):
                already_correct += 1
                continue

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM faa_registrations
                    WHERE ingestion_date = %s
                      AND id <> %s
                      AND REPLACE(REPLACE(UPPER(TRIM(COALESCE(n_number, ''))), '-', ''), ' ', '')
                          = %s
                    """,
                    (args.ingestion_date, rid, _reg_key(official_n)),
                )
                if cur.fetchone():
                    conflict += 1
                    log.warning(
                        "Skip id=%s: target n_number %r already used on another row (ingestion_date=%s)",
                        str(rid),
                        official_n,
                        args.ingestion_date,
                    )
                    continue

            log.info(
                "Fix id=%s serial=%r n_db=%r -> n_csv=%r",
                str(rid),
                sn,
                nn,
                official_n,
            )
            report_lines.append(
                {
                    "id": str(rid),
                    "serial_number": sn,
                    "n_number_before": nn,
                    "n_number_after": official_n,
                    "mfr_mdl_code": _norm(mfr_mdl_code),
                }
            )

            if args.apply:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE faa_registrations
                        SET n_number = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (official_n, rid),
                    )
                    rc = cur.rowcount
                conn.commit()
                fixed += rc

        log.info(
            "Done apply=%s: scanned=%s bad_pattern_fixed_candidates=%s applied=%s "
            "skipped_not_serial_eq_n=%s no_master_serial=%s already_correct_n=%s unique_conflict=%s",
            args.apply,
            scanned,
            len(report_lines),
            fixed if args.apply else 0,
            skipped_not_bad_pattern,
            no_master,
            already_correct,
            conflict,
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
    elif args.apply and report_lines:
        print("\nApplied", fixed, "n_number fix(es).")
    elif not report_lines:
        print("\nNo rows matched (serial==n_number pattern + MASTER lookup).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
