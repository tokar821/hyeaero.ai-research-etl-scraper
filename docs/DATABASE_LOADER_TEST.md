# Database Loader Test Commands

## Quick Test (10 records from each source)

```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --test
```

This will process:
- 10 Controller listings
- 10 AircraftExchange listings
- 10 FAA records
- 10 Internal DB records

## Custom Limits

### Test with specific limits for each source:

```powershell
# Test with 5 records from each source
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --limit-controller 5 --limit-aircraftexchange 5 --limit-faa 5 --limit-internal 5

# Test Controller only (10 records)
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --limit-controller 10

# Test FAA only (20 records)
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --limit-faa 20

# Test AircraftExchange only (15 records)
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --limit-aircraftexchange 15

# Test Internal DB only (10 records)
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py --limit-internal 10
```

## What Gets Tested

### Controller
- ✅ JSON files: `index/listings_metadata.json`, `details/details_metadata.json`
- ✅ Data saved to: `aircraft_listings`, `aircraft`, `raw_data_store`

### AircraftExchange
- ✅ JSON files: `index/listings_metadata.json`, `details/details_metadata.json`
- ✅ Manufacturer JSON: `manufacturers/*/manufacturer_listings_metadata.json`, `manufacturers/*/details/details_metadata.json`
- ✅ Data saved to: `aircraft_listings`, `aircraft`, `raw_data_store`

### FAA
- ✅ TXT files: `MASTER.txt`, `ACFTREF.txt`, `DEALER.txt`, `DEREG.txt`, `ENGINE.txt`, `DOCINDEX.txt`, `RESERVED.txt`
- ✅ PDF files: `ardata.pdf`, `FAA_Database_Documentation_*.pdf`
- ✅ Data saved to: `aircraft`, `raw_data_store`, `documents` (for PDFs)

### Internal DB
- ✅ CSV files: `aircraft.csv`, `recent_sales.csv`
- ✅ Data saved to: `aircraft`, `aircraft_sales`, `raw_data_store`

## Verify in Database

After running test, check:

```sql
-- Check aircraft table
SELECT COUNT(*) FROM aircraft;

-- Check listings
SELECT source_platform, COUNT(*) 
FROM aircraft_listings 
GROUP BY source_platform;

-- Check raw data store
SELECT source_platform, source_type, COUNT(*) 
FROM raw_data_store 
GROUP BY source_platform, source_type
ORDER BY source_platform, source_type;

-- Check documents (PDFs)
SELECT source_platform, document_type, COUNT(*), SUM(file_size)
FROM documents
GROUP BY source_platform, document_type;

-- Check sales
SELECT COUNT(*) FROM aircraft_sales;
```

## Expected Results

With `--test` (10 records each):
- Controller: ~10 listings in `aircraft_listings`
- AircraftExchange: ~10 listings in `aircraft_listings`
- FAA: ~10 aircraft in `aircraft`, ~7 TXT files in `raw_data_store`, ~2 PDFs in `documents`
- Internal DB: ~10 aircraft in `aircraft`, ~10 sales in `aircraft_sales`

## Notes

- Limits apply to processing, not storage
- ACFTREF.txt is always fully loaded (needed for MASTER.txt decoding) but storage may be limited
- PDF extraction may take a moment even with limits
- All raw data is preserved in `raw_data_store` for verification
