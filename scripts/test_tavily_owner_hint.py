#!/usr/bin/env python3
"""
Smoke-test Tavily owner hints (same logic as ``backend/services/tavily_owner_hint.py``).

Uses ``TAVILY_API_KEY`` from ``backend/.env`` or ``etl-pipeline/.env``.

**Built-in sample rows** (``--sample kb|other|minimal``):

- ``kb`` — Kenmore-style (matches curated KB if present; Tavily skipped in full pipeline).
- ``other`` — Trustee + address **not** in default KB → good for proving Tavily runs.
- ``minimal`` — TRUSTEE + street only.

Custom row::

    python scripts/test_tavily_owner_hint.py --registrant "MY SHELL LLC TRUSTEE" --street "100 Main" --city Dallas --state TX --zip 75201

Does **not** call Postgres; only Tavily + local heuristics.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict


def _setup() -> None:
    etl = Path(__file__).resolve().parents[1]
    repo = etl.parent
    backend = repo / "backend"
    if backend.is_dir():
        sys.path.insert(0, str(backend))
    from dotenv import load_dotenv

    for p in (etl / ".env", backend / ".env", repo / ".env"):
        if p.exists():
            load_dotenv(p, override=False)


SAMPLES: Dict[str, Dict[str, Any]] = {
    # Mirrors public Kenmore / Shepherd pattern (curated KB usually wins in /api/phlydata/owners).
    "kb": {
        "registrant_name": "KENMORE CREW LEASING INC TRUSTEE",
        "street": "4340 PACIFIC HWY UNIT 203",
        "street2": "",
        "city": "BELLINGHAM",
        "state": "WA",
        "zip_code": "98226",
        "country": "US",
    },
    # Fictional trustee + address → Tavily fetch if key set.
    "other": {
        "registrant_name": "SKYLINE AIRCRAFT TITLE TRUSTEE",
        "street": "5000 LEGACY DRIVE STE 100",
        "street2": "",
        "city": "PLANO",
        "state": "TX",
        "zip_code": "75024",
        "country": "US",
    },
    "minimal": {
        "registrant_name": "EXAMPLE HOLDINGS INC TRUSTEE",
        "street": "1 FINANCIAL PLAZA",
        "street2": None,
        "city": None,
        "state": "DE",
        "zip_code": "19801",
        "country": "US",
    },
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sample", choices=list(SAMPLES.keys()), default="other", help="Built-in FAA-style row")
    parser.add_argument("--registrant", default="", help="Override registrant_name")
    parser.add_argument("--street", default="", help="Override street")
    parser.add_argument("--street2", default="", help="Override street2")
    parser.add_argument("--city", default="", help="Override city")
    parser.add_argument("--state", default="", help="Override state")
    parser.add_argument("--zip", default="", dest="zip_code", help="Override zip_code")
    parser.add_argument("--json-out", action="store_true", help="Print full Tavily payload JSON")
    args = parser.parse_args()

    _setup()

    try:
        from services.tavily_owner_hint import (
            build_owner_search_query,
            enrich_faa_owners_with_tavily_hints,
            fetch_tavily_hints_for_query,
            has_address_context_for_search,
            is_trustee_like_registrant,
            should_run_tavily_for_registrant,
        )
    except ImportError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    row = dict(SAMPLES[args.sample])
    if args.registrant:
        row["registrant_name"] = args.registrant
    if args.street:
        row["street"] = args.street
    if args.street2:
        row["street2"] = args.street2
    if args.city:
        row["city"] = args.city
    if args.state:
        row["state"] = args.state
    if args.zip_code:
        row["zip_code"] = args.zip_code

    name = row.get("registrant_name") or ""
    print("Row:", json.dumps(row, indent=2))
    print(f"  trustee_like={is_trustee_like_registrant(name)}")
    print(f"  should_run_tavily={should_run_tavily_for_registrant(name)}")
    print(f"  has_address={has_address_context_for_search(row)}")
    q = build_owner_search_query(row)
    print(f"  query={q!r}")
    print()

    if not should_run_tavily_for_registrant(name):
        print("SKIP: Registrant does not match trustee/corp rules (see TAVILY_WHEN_CORP_AND_ADDRESS).")
        return 0
    if not has_address_context_for_search(row):
        print("SKIP: Need street or city+state/zip for search context.")
        return 0

    if not (os.getenv("TAVILY_API_KEY") or "").strip():
        print("SKIP: Set TAVILY_API_KEY in backend/.env")
        return 1

    payload = fetch_tavily_hints_for_query(q)
    if args.json_out:
        print(json.dumps(payload, indent=2, default=str))
    else:
        err = payload.get("error")
        results = payload.get("results") or []
        print(f"Tavily: error={err!r}, results={len(results)}")
        for i, hit in enumerate(results[:5], 1):
            print(f"  {i}. {hit.get('title')}")
            print(f"     {hit.get('url')}")
            if hit.get("content"):
                c = str(hit["content"])[:200].replace("\n", " ")
                print(f"     {c}...")

    print()
    print("--- enrich_faa_owners_with_tavily_hints (no KB on row) ---")
    enriched = enrich_faa_owners_with_tavily_hints([dict(row)])
    tw = enriched[0].get("tavily_web_hints")
    print("tavily_web_hints present:", bool(tw and (tw.get("results") or tw.get("error"))))
    if tw and not args.json_out:
        print(f"  keys: {list(tw.keys())}")
        if tw.get("error"):
            print(f"  error: {tw['error']}")
        if tw.get("results"):
            print(f"  result count: {len(tw['results'])}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
