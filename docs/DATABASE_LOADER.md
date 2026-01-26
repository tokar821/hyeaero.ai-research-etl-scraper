# Database Loader - Complete Guide

## Overview

Loads ALL scraped data from Controller, AircraftExchange, FAA, and Internal DB into PostgreSQL. Captures every field from every source - structured in normalized tables, original data in `raw_data_store`, documents in `documents` table.

## Features

- ✅ **Complete Data Capture**: All fields from all sources stored
- ✅ **Latest Date Only**: Processes only latest date data
- ✅ **Smart Upsert**: Inserts new or updates existing records
- ✅ **Change Tracking**: Records all field changes in history table
- ✅ **Engine/APU Tables**: Structured engine and APU data
- ✅ **FAA Tables**: Complete FAA registration and reference data
- ✅ **RAG-Ready**: Structured data optimized for embeddings

## Database Schema

See `database/schema.sql` for complete schema definition.

### Key Tables

**Core:**
- **aircraft** - Master aircraft records (serial/registration, manufacturer, model, condition, based_at, FAA fields)
- **aircraft_listings** - Current listings (all fields: engines, APU, seller contact, condition, etc.)
- **aircraft_engines** - Engine data (position 1/2, hours, cycles, TBO, notes)
- **aircraft_apus** - APU data (hours, maintenance programs, notes)
- **aircraft_sales** - Historical sales (with deferment flags, feature_source)
- **aircraft_listing_history** - Change tracking

**FAA:**
- **faa_registrations** - Complete MASTER.txt data (owner, address, certification, mode S codes)
- **faa_aircraft_reference** - ACFTREF.txt (manufacturer/model codes with specs)
- **faa_engine_reference** - ENGINE.txt (engine specifications)
- **faa_dealers** - DEALER.txt (dealer certificates)
- **faa_deregistered** - DEREG.txt (deregistered aircraft)
- **faa_document_index** - DOCINDEX.txt (document index)
- **faa_reserved** - RESERVED.txt (reserved registrations)

**Storage:**
- **raw_data_store** - All original JSON/CSV/TXT data (append-only)
- **documents** - PDF/TXT extracted text for RAG
- **ingestion_runs** - ETL execution tracking

## Usage

### Basic Command

```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

### What It Does

1. **Connects to PostgreSQL** using connection string
2. **Finds Latest Dates**:
   - Controller: Latest date in `store/raw/controller/YYYY-MM-DD/`
   - AircraftExchange: Latest date in `store/raw/aircraftexchange/YYYY-MM-DD/`
   - FAA: Latest date in `store/raw/faa/YYYY-MM-DD/`
   - Internal DB: Always loads (no date-based filtering)
3. **Loads Data**:
   - **Controller**: `index/listings_metadata.json`, `details/details_metadata.json` (all 50+ fields)
   - **AircraftExchange**: `index/listings_metadata.json`, `details/details_metadata.json`, `manufacturers/*/` (all 40+ fields)
   - **FAA**: All 7 TXT files (MASTER, ACFTREF, DEALER, DEREG, ENGINE, DOCINDEX, RESERVED) + PDFs
   - **Internal DB**: `aircraft.csv`, `recent_sales.csv` (all 35+ fields)
4. **Upserts Records**:
   - Checks if listing exists (by `listing_url`)
   - If exists and newer/same date: Updates and tracks changes
   - If exists but older date: Skips (already processed)
   - If not exists: Inserts new record
5. **Tracks Changes**: Records all field changes in `aircraft_listing_history`

## Data Flow

```
store/raw/
├── controller/2026-01-23/
│   ├── index/listings_metadata.json  → aircraft_listings
│   └── details/details_metadata.json → aircraft_listings (enriched)
├── aircraftexchange/2026-01-23/
│   ├── index/listings_metadata.json  → aircraft_listings
│   └── details/details_metadata.json → aircraft_listings (enriched)
├── faa/2026-01-23/
│   └── extracted/
│       └── MASTER.txt                 → aircraft (registration data)
└── internaldb/
    ├── aircraft.csv                   → aircraft (master data)
    └── recent_sales.csv               → aircraft_sales (append-only)
```

## Update Logic

### For Listings (Controller/AircraftExchange)

1. **Check Existence**: Query by `listing_url` (get latest record)
2. **Compare Dates**:
   - If new data date > existing date: Update with new date
   - If new data date == existing date: Update fields (re-run)
   - If new data date < existing date: Skip (older data)
3. **Track Changes**: Compare all fields, record changes in history table
4. **Update Aircraft**: Link to or create `aircraft` record

### For Internal DB

- **aircraft.csv**: Updates `aircraft` master table (by serial number)
- **recent_sales.csv**: Inserts into `aircraft_sales` (append-only, no updates)

## Change Tracking

All field changes are recorded in `aircraft_listing_history`:

```sql
SELECT * FROM aircraft_listing_history
WHERE listing_id = '...'
ORDER BY changed_at DESC;
```

Tracks:
- Price changes (`ask_price`, `sold_price`)
- Status changes (`listing_status`)
- Location changes
- Description changes
- Time changes (`airframe_total_time`)

## Output

The loader prints a summary:

```
============================================================
Database Loader Completed!
============================================================
Controller: {'date': '2026-01-23', 'listings': 5072, 'details': 907, 'inserted': 100, 'updated': 800, 'skipped': 0}
AircraftExchange: {'date': '2026-01-23', 'listings': 1000, 'details': 500, 'inserted': 200, 'updated': 300, 'skipped': 0}
FAA: {'date': '2026-01-23', 'aircraft': 350000, 'inserted': 10000, 'updated': 340000, 'skipped': 0}
Internal DB: {'aircraft': 5000, 'sales': 10000, 'inserted': 5000, 'updated': 0, 'skipped': 0}
Total Inserted: 5300
Total Updated: 1100
Total Skipped: 0
============================================================
```

## Re-running

When you run the loader again:

1. **Same Date**: If latest date hasn't changed, it will:
   - Update existing records with any changes
   - Track changes in history
   - Skip records that haven't changed

2. **New Date**: If a newer date exists, it will:
   - Process only the newer date
   - Update records with newer data
   - Skip older date data

## Commands

### Test (10 records each)
```powershell
python runners/run_database_loader.py --test
```

### Load All Data
```powershell
python runners/run_database_loader.py
```

### Load Specific Source Only
```powershell
# FAA only
python runners/run_database_loader.py --faa-only

# Internal DB only  
python runners/run_database_loader.py --internal-only
```

### Custom Limits
```powershell
python runners/run_database_loader.py --limit-controller 100 --limit-faa 1000
```

## PostgreSQL Connection

Uses environment variables (see `docs/ENVIRONMENT_SETUP.md`) or falls back to hardcoded connection string.

## Troubleshooting

### "ModuleNotFoundError: No module named 'psycopg2'"
```powershell
pip install psycopg2-binary
```

### "Connection refused"
- Check PostgreSQL connection string
- Verify database is accessible
- Check firewall/network settings

### "Table does not exist"
- Run schema creation: The loader will auto-create schema if tables don't exist
- Or manually run: `database/schema.sql`

### "No data found"
- Check `store/raw/` directory structure
- Verify date directories exist (YYYY-MM-DD format)
- Check JSON/CSV files exist

## Next Steps

- Add environment variable support for connection string
- Add progress bars for large datasets
- Add dry-run mode
- Add data validation before insert
- Add error recovery/resume capability
