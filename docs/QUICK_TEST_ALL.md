# Quick Test All Modules

## One-Command Test (All Scrapers + Database)

```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23; python runners/run_database_loader.py
```

## Individual Module Tests

### Controller Index (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_scraper.py --max-pages 1 --date 2026-01-23
```

### Controller Detail (2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0 --date 2026-01-23
```

### AircraftExchange Index (1 page)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --index --max-pages 1 --date 2026-01-23
```

### AircraftExchange Manufacturer
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer --date 2026-01-23
```

### AircraftExchange Manufacturer Detail (1 manufacturer, 2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 1 --max-listings 2 --date 2026-01-23
```

### AircraftExchange Detail (2 listings)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_aircraftexchange_scraper.py --detail --max-listings 2 --date 2026-01-23
```

### Database Loader (Load Latest Data)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

## What Each Command Does

- **Scrapers**: Scrape data and save to `store/raw/` (only latest date, skip existing)
- **Database Loader**: Loads latest date data from `store/raw/` into PostgreSQL (insert/update)

## Check Results

1. **Logs**: `logs/*.txt`
2. **Data**: `store/raw/controller/2026-01-23/` and `store/raw/aircraftexchange/2026-01-23/`
3. **Database**: Query PostgreSQL tables after running loader
