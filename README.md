# HyeAero ETL Pipeline

ETL pipeline for data ingestion and normalization from aircraft market data sources.

## Overview

This module handles:
- Scraping data from sources (Controller, AircraftExchange, FAA)
- Raw data storage in local file system
- Data normalization
- Loading into downstream systems (PostgreSQL)

## Architecture

- **Config Module**: Central configuration loader with environment variable support
- **Utils Module**: Logging and other utilities
- **Scrapers Module**: Web scrapers for aircraft market data
- **Database Module**: PostgreSQL database operations (schema, client, data loader)

## Setup

### Prerequisites

- Python 3.12
- PostgreSQL database (optional, for database loader)

### Installation

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment (optional):
```bash
# Copy the example env file if it exists
# cp env/.env.example .env

# Edit .env with your settings
```

### Environment Variables

Optional environment variables (see `.env.example` if exists):

**General:**
- `ENVIRONMENT`: `dev`, `prod`, or `local` (defaults to `local`)
- `DRY_RUN`: `true` or `false` (local defaults to true)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

**PostgreSQL Database:**
- `POSTGRES_CONNECTION_STRING`: Full PostgreSQL connection URI (recommended)
  - Format: `postgres://user:password@host:port/database?sslmode=require`
- OR use individual components:
  - `POSTGRES_HOST`: Database hostname
  - `POSTGRES_PORT`: Database port (default: 5432)
  - `POSTGRES_DATABASE`: Database name
  - `POSTGRES_USER`: Database username
  - `POSTGRES_PASSWORD`: Database password

**Example `.env` file:**
```env
ENVIRONMENT=local
LOG_LEVEL=INFO
POSTGRES_CONNECTION_STRING=postgres://user:password@host:port/database?sslmode=require
```

## Usage

### Running Scrapers

```bash
# Controller index scraper
python runners/run_controller_scraper.py

# Controller detail scraper
python runners/run_controller_detail_scraper.py --max-listings 10

# AircraftExchange all modules
python runners/run_aircraftexchange_scraper.py --all

# FAA scraper
python runners/run_faa_scraper.py
```

### Basic Example

```python
from config import get_config
from utils.logger import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Load config
config = get_config()
logger.info(f"Environment: {config.environment}, Dry-run: {config.is_dry_run()}")
```

### Data Storage

All scraped data is stored locally in the `store/` directory:
- **Raw data**: `store/raw/{source}/{YYYY-MM-DD}/`
  - Example: `store/raw/controller/2024-01-15/index/`
  - Example: `store/raw/controller/2024-01-15/details/`

## Logging

Logging includes:
- Timestamps (YYYY-MM-DD HH:MM:SS)
- Module names
- Log levels
- Log files saved to `logs/` directory

Example log output:
```
2024-01-15 14:30:45 | scrapers.controller_scraper_undetected | INFO | Starting Controller.com scraper
2024-01-15 14:30:46 | scrapers.controller_scraper_undetected | INFO | Scraped page 1/182
```

## Development

### Project Structure

```
etl-pipeline/
├── config/              # Configuration management
│   ├── __init__.py
│   └── config_loader.py
├── utils/               # Utility functions
│   ├── __init__.py
│   ├── logger.py
│   └── chrome_utils.py
├── scrapers/            # Scraper modules
│   ├── __init__.py
│   ├── controller_*.py
│   ├── aircraftexchange_*.py
│   └── faa_scraper.py
├── runners/             # Runner scripts
│   ├── __init__.py
│   ├── run_controller_scraper.py
│   ├── run_controller_detail_scraper.py
│   ├── run_aircraftexchange_scraper.py
│   └── run_faa_scraper.py
├── database/            # Database operations (PostgreSQL)
│   └── __init__.py
├── store/               # Local raw data storage
│   └── raw/
│       ├── controller/
│       └── aircraftexchange/
├── logs/                # Log files
│   ├── controller_log.txt
│   ├── aircraftexchange_log.txt
│   └── faa_log.txt
├── docs/                # Documentation
│   ├── SCRAPER_RE-RUN_BEHAVIOR.md
│   ├── CONTROLLER_DETAIL_PARSING_PATTERN.md
│   ├── QUICK_START.md
│   ├── TEST_COMMANDS.md
│   └── REFACTORING_PLAN.md
├── scripts/             # Utility and test scripts
│   ├── __init__.py
│   ├── verify_*.py
│   ├── check_*.py
│   └── test_*.py
├── env/                 # Environment files
│   └── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Database Integration

The pipeline includes PostgreSQL database integration:

- **Schema**: `database/schema.sql` - Complete database schema with tables for aircraft, listings, sales, history, and raw data
- **Client**: `database/postgres_client.py` - PostgreSQL connection and query utilities
- **Data Loader**: `database/data_loader.py` - Loads scraped data from `store/` into PostgreSQL
- **Runner**: `runners/run_database_loader.py` - Command to load latest data

### Running Database Loader

```bash
# Load latest scraped data to PostgreSQL
python runners/run_database_loader.py
```

The loader will:
- Find the latest date data for each source (controller, aircraftexchange, internaldb)
- Insert new records or update existing ones
- Track changes in history table
- Store raw data in append-only table

See [DATABASE_LOADER.md](./docs/DATABASE_LOADER.md) for detailed documentation.

## Next Steps

- ✅ Scrapers implemented for Controller, AircraftExchange, and FAA
- ✅ PostgreSQL database integration
- Implement data normalization pipelines
- Add data validation and quality checks
