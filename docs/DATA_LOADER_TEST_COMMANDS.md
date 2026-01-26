# Data Loader Test Commands

Complete guide for testing the `data_loader` module with all data sources.

## Overview

The data loader loads scraped data from `store/raw/` into PostgreSQL database. It supports 4 data sources:
1. **Controller** - Controller.com aircraft listings
2. **AircraftExchange** - AircraftExchange.com listings (index, details, manufacturer listings, manufacturer details)
3. **FAA** - FAA registration database (MASTER, ACFTREF, DEALER, DEREG, ENGINE, DOCINDEX, RESERVED, PDFs)
4. **Internal DB** - Internal database records

## Prerequisites

1. **PostgreSQL Database**: Ensure your database is running and accessible
2. **Environment Variables**: Set PostgreSQL connection in `.env` or environment:
   ```env
   POSTGRES_CONNECTION_STRING=postgres://user:password@host:port/database?sslmode=require
   ```
   OR use individual components:
   ```env
   POSTGRES_HOST=your-host
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=your-database
   POSTGRES_USER=your-user
   POSTGRES_PASSWORD=your-password
   ```

3. **Scraped Data**: Ensure you have scraped data in `store/raw/` directory:
   ```
   store/raw/
   ├── controller/YYYY-MM-DD/
   ├── aircraftexchange/YYYY-MM-DD/
   ├── faa/YYYY-MM-DD/
   └── internaldb/ (if applicable)
   ```

## Basic Test Commands

**⚠️ Important: All commands must be run from the `etl-pipeline` directory!**

### Quick Test (10 records from each source)
```powershell
# Navigate to etl-pipeline directory first
cd D:\HyeAero\etl-pipeline

# Then run the test
python runners/run_database_loader.py --test
```

This processes:
- 10 Controller listings
- 10 AircraftExchange listings
- 10 FAA records
- 10 Internal DB records

### Load All Data (No Limits)
```bash
python runners/run_database_loader.py
```

This processes **all** data from the latest date for each source.

## Test Individual Data Sources

**Note**: When you specify a limit for only one source, the loader automatically skips all other sources. This makes testing individual sources easier!

### Test Controller Only
```bash
# Test with 10 records (automatically skips other sources)
python runners/run_database_loader.py --limit-controller 10

# Test with 50 records
python runners/run_database_loader.py --limit-controller 50

# Process all Controller data (skip others) - Use --controller-only flag
python runners/run_database_loader.py --controller-only
```

### Test AircraftExchange Only
```bash
# Test with 10 records (automatically skips other sources)
python runners/run_database_loader.py --limit-aircraftexchange 10

# Test with 50 records
python runners/run_database_loader.py --limit-aircraftexchange 50

# Process all AircraftExchange data (skip others) - Use --aircraftexchange-only flag
python runners/run_database_loader.py --aircraftexchange-only
```

### Test FAA Only
```bash
# Test with 10 records (automatically skips other sources)
python runners/run_database_loader.py --limit-faa 10

# Test with 100 records
python runners/run_database_loader.py --limit-faa 100

# Process all FAA data (skip others) - Use --faa-only flag
python runners/run_database_loader.py --faa-only
```

### Test Internal DB Only
```bash
# Test with 10 records (automatically skips other sources)
python runners/run_database_loader.py --limit-internal 10

# Test with 50 records
python runners/run_database_loader.py --limit-internal 50

# Process all Internal DB data (skip others) - Use --internal-only flag
python runners/run_database_loader.py --internal-only
```

## Test Multiple Sources (Combined)

### Test Controller + AircraftExchange
```bash
# 10 records from each
python runners/run_database_loader.py --limit-controller 10 --limit-aircraftexchange 10 --limit-faa -1 --limit-internal -1
```

### Test Controller + FAA
```bash
# 10 Controller, 100 FAA
python runners/run_database_loader.py --limit-controller 10 --limit-faa 100 --limit-aircraftexchange -1 --limit-internal -1
```

### Test AircraftExchange + FAA
```bash
# 20 AircraftExchange, 50 FAA
python runners/run_database_loader.py --limit-aircraftexchange 20 --limit-faa 50 --limit-controller -1 --limit-internal -1
```

## Advanced Test Scenarios

### Test with Custom Limits
```bash
# Controller: 25, AircraftExchange: 30, FAA: 200, Internal: 15
python runners/run_database_loader.py --limit-controller 25 --limit-aircraftexchange 30 --limit-faa 200 --limit-internal 15
```

### Test Specific Source with Others Skipped
```bash
# Only Controller (others automatically skipped when only one limit is specified)
python runners/run_database_loader.py --limit-controller 50

# Only AircraftExchange (others automatically skipped)
python runners/run_database_loader.py --limit-aircraftexchange 50

# Or use explicit flags for clarity:
python runners/run_database_loader.py --controller-only
python runners/run_database_loader.py --aircraftexchange-only
python runners/run_database_loader.py --faa-only
python runners/run_database_loader.py --internal-only
```

## Understanding Limit Values

- **Positive number** (e.g., `10`, `50`, `100`): Process up to N records
- **`-1`**: Skip this source entirely
- **Not specified or `None`**: Process all records (no limit)

## Expected Output

The loader will output:
1. Connection status
2. Schema check/creation
3. Progress for each source
4. Summary statistics:

```
============================================================
Database Loader Completed!
============================================================
Controller: {'date': '2026-01-23', 'listings': 100, 'details': 95, 'inserted': 50, 'updated': 45, 'skipped': 5}
AircraftExchange: {'date': '2026-01-23', 'listings': 200, 'details': 180, 'manufacturer_listings': 50, 'manufacturer_details': 45, 'inserted': 100, 'updated': 80, 'skipped': 20}
FAA: {'date': '2026-01-23', 'master': {'inserted': 1000, 'updated': 0}, 'acftref': {'inserted': 500, 'updated': 0}, ...}
Internal DB: {'inserted': 10, 'updated': 5, 'skipped': 0}
Total Inserted: 1560
Total Updated: 130
Total Skipped: 25
============================================================
```

## Log Files

All logs are saved to:
```
etl-pipeline/logs/database_loader_log.txt
```

Check logs for detailed information about:
- Data processing progress
- Errors or warnings
- Database operations
- Record counts

## Troubleshooting

### No Data Found
If you see "No date directories found for {source}":
- Ensure scrapers have been run first
- Check that `store/raw/{source}/YYYY-MM-DD/` directories exist

### Database Connection Errors
- Verify PostgreSQL is running
- Check connection string/credentials in `.env`
- Test connection: `python -c "from database.postgres_client import PostgresClient; PostgresClient('your-connection-string').test_connection()"`

### Schema Issues
- The loader will auto-create schema if it doesn't exist
- Check `database/schema.sql` for table definitions
- Schema migrations are handled automatically

## Quick Reference

| Command | Description |
|---------|-------------|
| `--test` | Quick test: 10 records from each source |
| `--limit-controller N` | Limit Controller to N records (auto-skips others if only this is specified) |
| `--limit-aircraftexchange N` | Limit AircraftExchange to N records (auto-skips others if only this is specified) |
| `--limit-faa N` | Limit FAA to N records (auto-skips others if only this is specified) |
| `--limit-internal N` | Limit Internal DB to N records (auto-skips others if only this is specified) |
| `--controller-only` | Process only Controller data (all records) |
| `--aircraftexchange-only` | Process only AircraftExchange data (all records) |
| `--faa-only` | Process only FAA data (all records) |
| `--internal-only` | Process only Internal DB data (all records) |
| No flags | Process all data from all sources |

## Example Test Workflow

1. **Quick sanity check** (10 records each):
   ```bash
   python runners/run_database_loader.py --test
   ```

2. **Test individual sources** (one at a time - others automatically skipped):
   ```bash
   python runners/run_database_loader.py --limit-controller 20
   python runners/run_database_loader.py --limit-aircraftexchange 20
   python runners/run_database_loader.py --limit-faa 100
   ```

3. **Test specific combinations**:
   ```bash
   python runners/run_database_loader.py --limit-controller 50 --limit-aircraftexchange 50
   ```

4. **Full production load** (when ready):
   ```bash
   python runners/run_database_loader.py
   ```
