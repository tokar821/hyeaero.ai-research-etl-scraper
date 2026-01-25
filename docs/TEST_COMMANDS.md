# Test Commands for All Scraper Modules

Quick test commands with small limits to verify all modules work after refactoring.

## Controller Modules

### 1. Controller Index Scraper (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23
```

### 2. Controller Detail Scraper (2 listings, quick test)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23
```

## AircraftExchange Modules

### 3. AircraftExchange Index Scraper (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23
```

### 4. AircraftExchange Manufacturer List Scraper
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23
```

### 5. AircraftExchange Manufacturer Detail Scraper (1 manufacturer, 2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23
```

### 6. AircraftExchange Detail Scraper (from index, 2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23
```

## FAA Module

### 7. FAA Scraper
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_faa_scraper.py
```

## All Modules Test (Sequential)

Run all modules in sequence with small limits:

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

# FAA
cd D:\HyeAero\etl-pipeline; python runners/run_faa_scraper.py
```

## Quick Test One-Liners

### Controller Modules
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23
```

### AircraftExchange Modules
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23
```

## Expected Results

After running tests, check:

1. **Log files** in `logs/`:
   - `controller_log.txt`
   - `aircraftexchange_log.txt`
   - `faa_log.txt`

2. **Data files** in `store/raw/`:
   - `controller/2026-01-23/index/`
   - `controller/2026-01-23/details/`
   - `aircraftexchange/2026-01-23/index/`
   - `aircraftexchange/2026-01-23/manufacturers/`
   - `aircraftexchange/2026-01-23/details/`

## Database Loader

### Test Database Loader (Load Latest Data to PostgreSQL)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

This will:
- Connect to PostgreSQL database
- Find latest date data for each source (controller, aircraftexchange, internaldb)
- Load/update data in database (only latest date, not all historical)
- Track changes in history table
- Skip duplicates, update existing records

## Notes

- All commands use `--date 2026-01-23` to test with existing data (skip/backfill logic)
- `--cooldown-every 0` disables cooldown for quick testing
- Small limits (1-2 pages/listings) for fast verification
- Check logs for any import or path errors
- Database loader processes only the LATEST date for each source
