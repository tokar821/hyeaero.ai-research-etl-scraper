#!/usr/bin/env python3
"""
Test ZoomInfo GTM **contact** flow three ways (same stack as ``backend/services/zoominfo_client.py``):

1. **Contact Search** — ``POST .../data/v1/contacts/search`` (find person IDs).
2. **Contact Enrich (by ID)** — ``POST .../data/v1/contacts/enrich`` with ``personId`` from step 1.
3. **Contact Enrich (name-only)** — same enrich endpoint with ``matchPersonInput`` from first/last name only
   (no prior search).

Requires ZoomInfo credentials (same as backend):

- ``ZOOMINFO_ACCESS_TOKEN`` **or** ``ZOOMINFO_CLIENT_ID`` + ``ZOOMINFO_CLIENT_SECRET`` +
  ``ZOOMINFO_REFRESH_TOKEN``
- Optional: ``ZOOMINFO_BASE_URL`` (default ``https://api.zoominfo.com/gtm``)

Loads env from ``etl-pipeline/.env`` then ``backend/.env`` (repo root relative to this file).

Usage::

    cd etl-pipeline
    pip install requests python-dotenv
    python scripts/test_zoominfo_contact_three_way.py --full-name "Jane Doe"

    python scripts/test_zoominfo_contact_three_way.py --full-name "Jane Doe" --json

Optional: skip search and only test enrich by explicit ID::

    python scripts/test_zoominfo_contact_three_way.py --person-id 123456789 --skip-name-enrich

References: `ZoomInfo contact enrich <https://docs.zoominfo.com/reference/enrichinterface_enrichcontact>`_,
general Python + OAuth overview `Endgrate blog <https://endgrate.com/blog/using-the-zoominfo-api-to-get-contacts-in-python>`_.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, List, Optional, Set

from dotenv import load_dotenv

# ZoomInfo JSON:API error codes (403/401/429/400) — align messages with official docs.
_GUIDANCE: dict[str, str] = {
    "ZI0001": (
        "401 Unauthorized — Invalid or expired token. Regenerate the access token; ensure "
        "`Authorization: Bearer …` is sent. With refresh flow: check ZOOMINFO_CLIENT_ID / "
        "ZOOMINFO_CLIENT_SECRET / ZOOMINFO_REFRESH_TOKEN and that refresh succeeds."
    ),
    "ZI0002": (
        "403 Forbidden — Missing OAuth scope for this endpoint. Ask your ZoomInfo Administrator "
        "to grant the application access to the requested resource (e.g. "
        "`/gtm/data/v1/contacts/search` and `/gtm/data/v1/contacts/enrich`). "
        "After scopes change, you may need a new refresh token (re-consent) per your org policy. "
        "HyeAero docs often refer to contact data as `api:data:contact`-class scopes — confirm "
        "exact scope names in the ZoomInfo Developer Portal for your app."
    ),
    "ZI0003": (
        "403 Forbidden — Endpoint not enabled for your subscription. Contact your ZoomInfo "
        "Account Manager to purchase or enable GTM Data Contact Search / Contact Enrich APIs."
    ),
    "ZI0004": (
        "429 Too Many Requests — Back off with exponential retry; see ZoomInfo rate-limit docs."
    ),
    "PFAPI0001": (
        "400 — Disallowed output/input field. Check entitlements; remove disallowed fields; "
        "use Lookup Enrich to list allowed fields for your account."
    ),
    "PFAPI0002": (
        "400 — Validation or missing parameters. Fix request body and required fields per schema."
    ),
    "PFAPI0003": (
        "400 — Invalid request body or field. Match JSON:API structure and field names to the spec."
    ),
    "PFAPI0004": (
        "400 — Insufficient match input. Provide at least one valid field in matchPersonInput / search."
    ),
    "PFAPI0005": (
        "400 — Invalid field name spelling or not valid for this endpoint."
    ),
    "PFAPI0006": (
        "400 — Invalid parameter value or range; use lookup endpoints for allowed enumerations."
    ),
    "PFAPI0008": (
        "400 — Invalid field type; coerce types to match the API specification."
    ),
    "PFAPI0009": (
        "400 — Invalid query or outputFields entry; trim to fields your plan allows."
    ),
}


def _extract_error_codes(message: str) -> Set[str]:
    if not message:
        return set()
    return set(m.upper() for m in re.findall(r"\b(ZI\d{4}|PFAPI\d{4})\b", message, flags=re.I))


def _print_recommended_actions(message: str, *, way_label: str) -> None:
    """Print parsed ZoomInfo error codes and official-style recommended actions."""
    codes = _extract_error_codes(message)
    print(f"  ── Recommended actions ({way_label}) ──")
    if codes:
        for code in sorted(codes):
            hint = _GUIDANCE.get(code)
            if hint:
                print(f"  • [{code}] {hint}")
            else:
                print(f"  • [{code}] See ZoomInfo API error reference for this code.")
    else:
        low = message.lower()
        if "403" in message or "forbidden" in low or "access denied" in low:
            print("  • [403] Treat as ZI0002/ZI0003: expand OAuth app scopes (Admin) and/or "
                  "enable Contact APIs on the contract (Account Manager).")
        elif "401" in message or "unauthorized" in low:
            print("  • [401] Treat as ZI0001: fix or refresh the bearer token.")
        elif "429" in low:
            print("  • [429] Treat as ZI0004: rate limit — retry with backoff.")
        elif "400" in message or "bad request" in low:
            print("  • [400] Check PFAPI* codes in the response body; validate fields and body shape.")
        else:
            print("  • Review the full error payload above; compare with ZoomInfo GTM Data API docs.")
    print()


def _repo_roots() -> tuple[Path, Path]:
    etl = Path(__file__).resolve().parents[1]
    repo = etl.parent
    return etl, repo


def _setup_path_and_env() -> None:
    _, repo = _repo_roots()
    backend = repo / "backend"
    if backend.is_dir():
        sys.path.insert(0, str(backend))
    etl, repo2 = _repo_roots()
    for p in (etl / ".env", repo2 / "backend" / ".env", repo2 / ".env"):
        if p.exists():
            load_dotenv(p, override=False)


def _attrs(record: Any) -> dict:
    if not record or not isinstance(record, dict):
        return {}
    return record.get("attributes") or {}


def _person_id_from_search_row(row: Any) -> Optional[int]:
    """JSON:API resource id or attributes.personId."""
    if not row or not isinstance(row, dict):
        return None
    rid = row.get("id")
    if rid is not None and str(rid).strip().isdigit():
        try:
            return int(rid)
        except ValueError:
            pass
    pid = (_attrs(row).get("personId") or _attrs(row).get("id"))
    if pid is None:
        return None
    try:
        return int(pid)
    except (TypeError, ValueError):
        return None


def _summarize_contact(record: Any) -> dict:
    a = _attrs(record)
    return {
        "matchStatus": (record.get("meta") or {}).get("matchStatus"),
        "id": record.get("id"),
        "fullName": a.get("fullName"),
        "email": a.get("email"),
        "companyName": a.get("companyName"),
        "phone": a.get("phone") or a.get("directPhone") or a.get("mobilePhone"),
        "city": a.get("city"),
        "state": a.get("state"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--full-name",
        default="",
        help="Name for contact search and name-only enrich (default: env ZI_TEST_FULL_NAME or 'John Smith').",
    )
    parser.add_argument(
        "--person-id",
        type=int,
        default=None,
        help="If set, Way 2 uses this personId instead of the first search hit.",
    )
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="Skip Way 1 (contact search).",
    )
    parser.add_argument(
        "--skip-name-enrich",
        action="store_true",
        help="Skip Way 3 (enrich by name only).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print raw JSON snippets for debugging.",
    )
    args = parser.parse_args()

    _setup_path_and_env()

    full_name = (args.full_name or os.getenv("ZI_TEST_FULL_NAME") or "John Smith").strip()
    if not args.skip_search and not full_name:
        print("ERROR: --full-name is required unless --skip-search.", file=sys.stderr)
        return 1

    try:
        from services.zoominfo_client import enrich_contact, search_contacts
    except ImportError as e:
        print(
            "ERROR: Could not import backend services.zoominfo_client.\n"
            f"  {e}\n"
            "Ensure this repo layout has backend/services/zoominfo_client.py next to etl-pipeline/.",
            file=sys.stderr,
        )
        return 1

    token = (os.getenv("ZOOMINFO_ACCESS_TOKEN") or "").strip()
    if not token and not all(
        os.getenv(k, "").strip()
        for k in ("ZOOMINFO_CLIENT_ID", "ZOOMINFO_CLIENT_SECRET", "ZOOMINFO_REFRESH_TOKEN")
    ):
        print(
            "ERROR: Set ZOOMINFO_ACCESS_TOKEN or CLIENT_ID/SECRET/REFRESH_TOKEN in backend/.env or etl-pipeline/.env.",
            file=sys.stderr,
        )
        return 1

    print("ZoomInfo contact three-way test")
    print(f"  full_name={full_name!r}")
    print(f"  base_url={os.getenv('ZOOMINFO_BASE_URL') or 'https://api.zoominfo.com/gtm (default)'}")
    print()

    collected_errors: List[str] = []

    # --- Way 1: Contact Search ---
    search_rows: list = []
    if not args.skip_search:
        print("=== Way 1: Contact Search (POST /data/v1/contacts/search) ===")
        search_rows, err = search_contacts(full_name, page_size=10)
        if err:
            print(f"  FAIL: {err}")
            collected_errors.append(err)
            _print_recommended_actions(err, way_label="Way 1 — Contact Search")
        else:
            print(f"  OK: {len(search_rows)} result(s)")
            if args.json and search_rows:
                print(json.dumps(search_rows[0], indent=2, default=str)[:4000])
            elif search_rows:
                pid = _person_id_from_search_row(search_rows[0])
                print(f"  First resource id: {search_rows[0].get('id')!r}  -> parsed personId: {pid}")
        print()
    else:
        print("=== Way 1: skipped (--skip-search) ===\n")

    # --- Way 2: Enrich by personId ---
    print("=== Way 2: Contact Enrich by personId (POST /data/v1/contacts/enrich) ===")
    pid_use = args.person_id
    if pid_use is None and search_rows:
        pid_use = _person_id_from_search_row(search_rows[0])
    if pid_use is None:
        print("  SKIP: No personId (no search results or --skip-search without --person-id).")
    else:
        rec, err = enrich_contact(person_id=pid_use, full_name=None)
        if err:
            print(f"  FAIL: {err}")
        elif rec is None:
            print("  NO_MATCH (or empty response).")
        else:
            print(f"  OK: {_summarize_contact(rec)}")
            if args.json:
                print(json.dumps(rec, indent=2, default=str)[:4000])
    print()

    # --- Way 3: Enrich by name only ---
    if args.skip_name_enrich:
        print("=== Way 3: skipped (--skip-name-enrich) ===")
        return 0

    print("=== Way 3: Contact Enrich by full name only (no personId) ===")
    rec3, err3 = enrich_contact(person_id=None, full_name=full_name)
    if err3:
        print(f"  FAIL: {err3}")
        collected_errors.append(err3)
        _print_recommended_actions(err3, way_label="Way 3 — Contact Enrich (name only)")
    elif rec3 is None:
        print("  NO_MATCH (or empty response).")
    else:
        print(f"  OK: {_summarize_contact(rec3)}")
        if args.json:
            print(json.dumps(rec3, indent=2, default=str)[:4000])

    print()
    print("── Summary ──")
    all_codes: Set[str] = set()
    for e in collected_errors:
        all_codes |= _extract_error_codes(e)
    if "ZI0002" in all_codes:
        print(
            "Your OAuth token is valid enough to reach ZoomInfo, but the application lacks "
            "scope (ZI0002) for **both** Contact Search and Contact Enrich in this run.\n"
            "  1. ZoomInfo Developer Portal → your app → enable GTM Data **contact** APIs / scopes.\n"
            "  2. ZoomInfo Administrator approves the scope change.\n"
            "  3. Re-run OAuth (e.g. phlydata-zoominfo/oauth_capture_refresh_token.py) if a new "
            "refresh token is required.\n"
            "  4. If scopes are correct but error persists, treat as ZI0003 — Account Manager "
            "must enable the product on the contract."
        )
    elif collected_errors:
        print("Some steps failed; see recommended actions above per way.")
    else:
        print("All executed contact steps completed without transport/API errors (OK or NO_MATCH).")

    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
