# Enhanced Logging for Database Loader

The database loader now includes comprehensive detailed logging to help you track exactly what's happening during data processing.

## Log Levels

**Default is DEBUG** – detailed logs for all loaders (Controller, AircraftExchange, FAA, Internal DB) are shown by default.

You can change verbosity with `--log-level`:

```bash
# Default (DEBUG level) - Detailed logs for all loaders
python runners/run_database_loader.py

# INFO level - Progress and summaries only (less verbose)
python runners/run_database_loader.py --log-level INFO

# WARNING level - Only warnings and errors
python runners/run_database_loader.py --log-level WARNING
```

## What You'll See in Logs

### Controller.com Data Loading

**INFO Level:**
- File loading status
- Total records found
- Progress every 10 records
- Summary statistics

**DEBUG Level (use `--log-level DEBUG`):**
- Each record being processed
- Extracted fields (URL, ID, Price, Model, etc.)
- Insert/Update/Skip decisions
- Field changes detected
- Engine/APU storage
- JSON parsing details

**Example:**
```
2026-01-25 19:20:10 | loaders.controller_loader | INFO | Loading Controller index data from store/raw/controller/2026-01-23/index/listings_metadata.json
2026-01-25 19:20:11 | loaders.controller_loader | INFO | Successfully loaded 150 Controller index listings from JSON
2026-01-25 19:20:11 | loaders.controller_loader | INFO | Processing 150 Controller index listings...
2026-01-25 19:20:11 | loaders.controller_loader | INFO | Processing Controller index listing 1/150: https://...
2026-01-25 19:20:11 | loaders.controller_loader | DEBUG | Processing listing: URL=https://..., ID=12345, Price=$2,500,000, Model=Cessna Citation CJ3+
2026-01-25 19:20:12 | loaders.controller_loader | DEBUG | ✓ Inserted Controller listing: https://...
```

### FAA Data Loading

**INFO Level:**
- File loading status
- Field names detected
- Total rows found
- Progress every 1000 records (MASTER/DEREG) or 100 records (DEALER)
- Summary statistics

**DEBUG Level:**
- Each record being processed
- N-Number, Serial Number, decoded manufacturer/model
- Insert/Update/Skip decisions
- ACFTREF code lookups
- CSV parsing details

**Example:**
```
2026-01-25 19:20:15 | loaders.faa_loader | INFO | Loading FAA MASTER data from store/raw/faa/2026-01-23/extracted/MASTER.txt
2026-01-25 19:20:16 | loaders.faa_loader | INFO | Found 63837 rows in MASTER.txt (processing up to all)
2026-01-25 19:20:16 | loaders.faa_loader | INFO | Processing FAA MASTER row 1/63837: N-Number=N123AB
2026-01-20 19:20:16 | loaders.faa_loader | DEBUG | Decoded MFR/MDL code C25A → CESSNA 525A
2026-01-25 19:20:17 | loaders.faa_loader | DEBUG | ✓ Inserted FAA MASTER: N-Number=N123AB, Serial=CE-525A-0123
```

### Internal DB Data Loading

**INFO Level:**
- File loading status
- Total rows found
- Progress every 10 records
- Summary statistics

**DEBUG Level:**
- Each record being processed
- Extracted fields (Serial, Registration, Make, Model, Price, etc.)
- Insert/Update/Skip decisions

**Example:**
```
2026-01-25 19:20:20 | loaders.internal_loader | INFO | Loading aircraft data from store/raw/internaldb/aircraft.csv
2026-01-25 19:20:20 | loaders.internal_loader | INFO | Found 500 rows in aircraft.csv (processing up to all)
2026-01-25 19:20:20 | loaders.internal_loader | INFO | Processing Internal DB aircraft 1/500: Serial=CE-525A-0123, Reg=N123AB
2026-01-25 19:20:20 | loaders.controller_loader | DEBUG | Processing aircraft: Serial=CE-525A-0123, Reg=N123AB, Make=Cessna, Model=Citation CJ3+, Year=2020
2026-01-25 19:20:21 | loaders.internal_loader | DEBUG | ✓ Inserted Internal DB aircraft: Serial=CE-525A-0123
```

### AircraftExchange.com Data Loading

Similar detailed logging as Controller.com:
- JSON file loading status
- Progress indicators
- Field extraction details
- Insert/Update/Skip decisions

## Log File Location

All logs are saved to:
```
etl-pipeline/logs/database_loader_log.txt
```

## Recommended Usage

### Default (detailed logs)
```bash
# Detailed logs for all loaders (Controller, AircraftExchange, FAA, Internal DB)
python runners/run_database_loader.py
```

### For Production (less verbose)
```bash
# Progress and summaries only
python runners/run_database_loader.py --log-level INFO
```

### For Testing with limits
```bash
# See all details with limited records
python runners/run_database_loader.py --limit-controller 10
```

## What Each Log Level Shows

| Level | What You See |
|-------|-------------|
| **DEBUG** | Everything: every record, every field extracted, every decision, parsing details |
| **INFO** | Progress indicators, file loading, summaries, important events |
| **WARNING** | Only warnings and potential issues |
| **ERROR** | Only errors and exceptions |

## Key Log Messages

### Success Indicators
- `✓ Inserted` - New record added
- `✓ Updated` - Existing record updated
- `⊘ Skipped` - Record skipped (already exists, missing data, etc.)

### Progress Indicators
- `Processing X/Y` - Shows current progress
- `Found X rows` - Total records found in file
- `Processing complete` - Summary of what was done

### Error Indicators
- `Failed to parse JSON` - JSON parsing error
- `Error loading` - File reading error
- `Missing listing_url` - Required field missing

## Tips

1. **Use DEBUG for detailed investigation**: When you need to see exactly what data is being extracted and why records are being skipped.

2. **Use INFO for normal operation**: Shows you progress and summaries without overwhelming detail.

3. **Check logs after each run**: Review the log file to see what was processed and identify any issues.

4. **Look for patterns**: If many records are skipped, check the DEBUG logs to see why.

5. **Monitor file sizes**: DEBUG logs can be large. Consider archiving old logs periodically.

## Example: Full Controller Detail Processing

With `--log-level DEBUG`, you'll see:
```
2026-01-25 19:20:30 | loaders.controller_loader | INFO | Loading Controller detail data from store/raw/controller/2026-01-23/details/details_metadata.json
2026-01-25 19:20:31 | loaders.controller_loader | INFO | Successfully loaded 50 Controller detail records from JSON
2026-01-25 19:20:31 | loaders.controller_loader | INFO | Processing 50 Controller detail records...
2026-01-25 19:20:31 | loaders.controller_loader | INFO | Processing Controller detail 1/50: https://...
2026-01-25 19:20:31 | loaders.controller_loader | DEBUG | Processing detail: URL=https://..., Serial=CE-525A-0123, Reg=N123AB, Make=Cessna, Model=Citation CJ3+, Year=2020, Price=$2,500,000
2026-01-25 19:20:32 | loaders.controller_loader | DEBUG | Extracted Controller detail fields: Serial=CE-525A-0123, Reg=N123AB, Make=Cessna, Model=Citation CJ3+, Year=2020, Price=2500000.0, Time=1250.5, Location=KJFK
2026-01-25 19:20:32 | loaders.controller_loader | DEBUG | Found existing Controller detail listing (ID: abc-123, existing_date: 2026-01-22, new_date: 2026-01-23)
2026-01-25 19:20:32 | loaders.controller_loader | DEBUG | Price change: 2400000.0 → 2500000.0
2026-01-25 19:20:32 | loaders.controller_loader | DEBUG | Updating Controller detail with newer ingestion_date (ID: abc-123)
2026-01-25 19:20:33 | loaders.controller_loader | DEBUG | Storing engine 1: Pratt & Whitney PW535A, Time=1250.5, Cycles=850
2026-01-25 19:20:33 | loaders.controller_loader | DEBUG | Recording history change: ask_price = 2400000.0 → 2500000.0
2026-01-25 19:20:33 | loaders.controller_loader | DEBUG | Successfully updated Controller detail (URL: https://..., 1 changes)
2026-01-25 19:20:33 | loaders.controller_loader | DEBUG | ✓ Updated Controller detail: https://...
```

This gives you complete visibility into what's happening with each record!
