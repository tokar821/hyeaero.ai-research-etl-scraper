# Scraper re-run behavior (same date)

**Goal:** Re-running on the same date keeps updating JSON **continuously**, without removing or duplicating original data, by skipping what’s already there. If we have scraped HTML but didn’t save JSON yet (e.g. crash), we **backfill from HTML** and save to JSON. Same date only.

## Summary

| Module | Skip-if-exists | Backfill from HTML | Re-run same date |
|--------|----------------|--------------------|------------------|
| **Controller index** | **Yes** (by page) | **Yes** | Safe. Skips pages with HTML; appends only new. No overwrite/delete. |
| **Controller detail** | **Yes** (by `listing_url`) | **Yes** | Safe. Skips existing; appends only new. |
| **AircraftExchange index** | **Yes** (by page) | **Yes** | Safe. Skips pages with HTML; appends only new. |
| **AircraftExchange detail** | **Yes** (by `listing_url`) | **Yes** | Safe. Skips existing; appends only new. |
| **AircraftExchange manufacturer-detail** | **Yes** (by `listing_url`) | **Yes** | Safe per manufacturer. Skips existing; appends only new. |
| AircraftExchange manufacturer | Partial | — | Overwrites per manufacturer (listings). |

## Same-date guarantees

- **No re-scrape** of data we already have (skip existing).
- **No overwrite or delete** of previous data.
- **Append only** new data.
- **No existing JSON:** rebuild from scraped HTML (backfill) when possible.

## Backfill from HTML

If we have **HTML** (e.g. `page_*.html`, `listing_*.html`) but **no JSON** (or missing records) because we didn’t save yet (crash, interrupt):

1. Load existing JSON when present.
2. Scan output dir for saved HTML.
3. Extract data from HTML for any item we don’t yet have in JSON.
4. Append those records to JSON and save.
5. Then run the normal scrape loop; skip existing, fetch only missing, append, save.

## Per-module behavior

- **Index (Controller, AircraftExchange):** Discover `page_*.html` → done pages. Load `listings_metadata.json`. Backfill from HTML (merge by `listing_url`). Skip pages with HTML; fetch only new pages. Append new listings, save JSON.
- **Detail (Controller, AircraftExchange, manufacturer-detail):** Load `details_metadata.json`. Build `done_urls` from `listing_url`. Backfill from `listing_*.html` (we have HTML but no record). Skip URLs in `done_urls`; fetch only new. Append, save JSON after each new scrape.

## Resume args

- **Controller index:** `--start-page` ignored (skip-if-exists). Use `--date`, `--max-pages` as needed.
- **Controller detail:** `--start-from` ignored (skip-if-exists). Use `--date`, `--max-listings`.
- **AircraftExchange:** Use `--date`, `--start-from` (detail), `--max-pages` (index), etc. where supported.
