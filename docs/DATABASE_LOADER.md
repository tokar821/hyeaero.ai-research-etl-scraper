# Database Loader Module

## Overview

The database loader module (`database/data_loader.py`) loads scraped data from the `store/` directory into PostgreSQL database. It only processes the **latest date** data for each source to avoid duplicate processing.

## Features

- ✅ **Latest Date Only**: Automatically finds and processes only the latest date data
- ✅ **Smart Upsert**: Inserts new records or updates existing ones
- ✅ **Change Tracking**: Records all field-level changes in `aircraft_listing_history` table
- ✅ **Deduplication**: Skips records that already exist with same or newer data
- ✅ **Multi-Source**: Supports Controller, AircraftExchange, and Internal DB (CSV)

## Database Schema

See `database/schema.sql` for complete schema definition.

### Key Tables

- **aircraft**: Canonical aircraft master data (by serial number)
- **aircraft_listings**: Current/latest snapshot of listings
- **aircraft_listing_history**: Change history (price, status, etc.)
- **aircraft_sales**: Historical sales data (append-only)
- **raw_data_store**: Original raw data (append-only)

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
   - Internal DB: Always loads (no date-based filtering)
3. **Loads Data**:
   - Controller: `index/listings_metadata.json` and `details/details_metadata.json`
   - AircraftExchange: `index/listings_metadata.json` and `details/details_metadata.json`
   - Internal DB: `aircraft.csv` and `recent_sales.csv`
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

## PostgreSQL Connection

Connection string is hardcoded in `runners/run_database_loader.py`:

```python
connection_string = (
    "postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@"
    "pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/"
    "defaultdb?sslmode=require"
)
```

**Note**: Consider moving to environment variables for production.

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
