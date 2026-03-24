# PhlyData aircraft CSV → PostgreSQL

Loads `store/raw/internaldb/aircraft.csv` into `public.phlydata_aircraft`.

## Prerequisites

1. **Python 3.10+** with ETL deps: from `etl-pipeline` run `pip install -r requirements.txt` (needs `psycopg2-binary`, `python-dotenv`).
2. **`POSTGRES_CONNECTION_STRING`** in `etl-pipeline/.env` or `backend/.env` (same variable the API uses).

## Run (Windows)

**CMD** (from anywhere):

```bat
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd
```

**PowerShell**:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.ps1
```

**Full reload** (truncate table, then insert — use when schema/columns changed or you want a clean slate):

```bat
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd --reset
```

**Custom CSV path**:

```bat
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd --internal-csv "D:\path\to\aircraft.csv"
```

**Dry run** (counts only, no DB writes):

```bat
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd --dry-run
```

**Drop old JSON column** (if you previously used `csv_extra`):

```bat
D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd --drop-legacy-csv-extra
```

## Manual (no wrapper)

```bat
cd /d D:\HyeAero\etl-pipeline
pip install -r requirements.txt
python scripts\build_phlydata_aircraft_table.py
```

## Columns (1:1 with CSV)

- **Canonical** headers (standard internal export) map to **typed** Postgres columns (`serial_number`, `ask_price`, dates, numerics, etc.).
- **Any other** CSV column gets its **own** `TEXT` column named `csv_<slug>` (derived from the header). Nothing is merged into a single JSON field.
- The API uses a **dynamic** `SELECT` so every column on the table is returned.

The script **adds missing columns** on existing databases (`ALTER TABLE`) before upserting. Re-run after you add new columns to the CSV.
