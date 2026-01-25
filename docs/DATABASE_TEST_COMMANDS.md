# Database Loader Test Commands

## Quick Test Commands

### 1. Test Database Connection
```powershell
cd D:\HyeAero\etl-pipeline; python -c "from database.postgres_client import PostgresClient; client = PostgresClient(connection_string='postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/defaultdb?sslmode=require'); print('OK' if client.test_connection() else 'FAILED')"
```

### 2. Create Database Schema
```powershell
cd D:\HyeAero\etl-pipeline; python -c "from database.postgres_client import PostgresClient; client = PostgresClient(connection_string='postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/defaultdb?sslmode=require'); client.create_schema()"
```

### 3. Run Database Loader (Load Latest Data)
```powershell
cd D:\HyeAero\etl-pipeline; python runners/run_database_loader.py
```

## Expected Output

```
============================================================
Database Loader - Loading latest scraped data to PostgreSQL
============================================================
Connecting to PostgreSQL...
Database connection test successful
Database schema already exists
Store base path: D:\HyeAero\etl-pipeline\store\raw
Latest date for controller: 2026-01-23
Loading Controller index data from ...
Loading Controller detail data from ...
Latest date for aircraftexchange: 2026-01-23
Loading AircraftExchange index data from ...
Loading AircraftExchange detail data from ...
Loading aircraft data from ...
Loading sales data from ...
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

## Verify Data in Database

### Check Aircraft Count
```sql
SELECT COUNT(*) FROM aircraft;
```

### Check Listings Count
```sql
SELECT source_platform, COUNT(*) 
FROM aircraft_listings 
GROUP BY source_platform;
```

### Check Latest Ingestion Date
```sql
SELECT source_platform, MAX(ingestion_date) as latest_date
FROM aircraft_listings
GROUP BY source_platform;
```

### Check Change History
```sql
SELECT field_name, COUNT(*) as change_count
FROM aircraft_listing_history
GROUP BY field_name
ORDER BY change_count DESC;
```

## Troubleshooting

### Import Errors
If you see `ModuleNotFoundError: No module named 'psycopg2'`:
```powershell
pip install psycopg2-binary
```

### Connection Errors
- Verify PostgreSQL connection string is correct
- Check database is accessible
- Verify SSL mode is set to 'require'

### Schema Errors
- Run schema creation manually if needed
- Check `database/schema.sql` for table definitions
