# All Test Commands - Complete Reference

Quick reference for testing all ETL pipeline modules.

## Scraper Modules (Small Data Tests)

### Controller Modules

#### 1. Controller Index (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23
```

#### 2. Controller Detail (2 listings, quick)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23
```

### AircraftExchange Modules

#### 3. AircraftExchange Index (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23
```

#### 4. AircraftExchange Manufacturer List
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23
```

#### 5. AircraftExchange Manufacturer Detail (1 manufacturer, 2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23
```

#### 6. AircraftExchange Detail (2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23
```

### FAA Module

#### 7. FAA Scraper
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_faa_scraper.py
```

## Database Loader

### 8. Load Latest Data to PostgreSQL
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

This will:
- Find latest date for each source (controller, aircraftexchange, internaldb)
- Load data from `store/raw/` into PostgreSQL
- Insert new records or update existing ones
- Track changes in history table

## Test All Modules (Sequential)

Run all modules in sequence:

```powershell
# Controller Index (1 page)
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23

# Controller Detail (2 listings)
cd D:\HyeAero\etl-pipeline; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23

# AircraftExchange Index (1 page)
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23

# AircraftExchange Manufacturer
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23

# AircraftExchange Manufacturer Detail (1 manufacturer, 2 listings)
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23

# AircraftExchange Detail (2 listings)
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23

# Database Loader (Load all latest data)
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

## Quick One-Liner Tests

### Controller Only
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23
```

### AircraftExchange Only
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23
```

## Expected Results

After running tests, check:

1. **Log files** in `logs/`:
   - `controller_log.txt`
   - `aircraftexchange_log.txt`
   - `faa_log.txt`
   - `database_loader_log.txt`

2. **Data files** in `store/raw/`:
   - `controller/2026-01-23/index/`
   - `controller/2026-01-23/details/`
   - `aircraftexchange/2026-01-23/index/`
   - `aircraftexchange/2026-01-23/manufacturers/`
   - `aircraftexchange/2026-01-23/details/`
   - `internaldb/aircraft.csv`
   - `internaldb/recent_sales.csv`

3. **Database** (after running loader):
   - `aircraft` table: Master aircraft data
   - `aircraft_listings` table: Current listings
   - `aircraft_listing_history` table: Change history
   - `aircraft_sales` table: Sales history
   - `raw_data_store` table: Original raw data

## Notes

- All scraper commands use `--date 2026-01-23` to test with existing data (skip/backfill logic)
- `--cooldown-every 0` disables cooldown for quick testing
- Small limits (1-2 pages/listings) for fast verification
- Database loader processes only the **LATEST date** for each source
- Database loader will update existing records if data changed
- Database loader tracks all changes in `aircraft_listing_history` table
