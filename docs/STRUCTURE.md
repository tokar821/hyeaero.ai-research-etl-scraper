# ETL Pipeline Directory Structure

## Overview

This document describes the clean, organized structure of the ETL pipeline codebase.

## Directory Structure

```
etl-pipeline/
├── config/              # Configuration management
│   ├── __init__.py
│   └── config_loader.py
│
├── utils/               # Utility functions
│   ├── __init__.py
│   ├── logger.py        # Logging setup and utilities
│   └── chrome_utils.py  # Chrome/browser utilities
│
├── scrapers/            # Scraper modules
│   ├── __init__.py
│   ├── controller_*.py           # Controller.com scrapers
│   ├── aircraftexchange_*.py     # AircraftExchange.com scrapers
│   └── faa_scraper.py            # FAA database scraper
│
├── runners/             # Runner scripts (entry points)
│   ├── __init__.py
│   ├── run_controller_scraper.py
│   ├── run_controller_detail_scraper.py
│   ├── run_aircraftexchange_scraper.py
│   └── run_faa_scraper.py
│
├── database/            # Database operations (PostgreSQL)
│   └── __init__.py
│   # Future: PostgreSQL client, models, migrations
│
├── store/               # Local raw data storage
│   └── raw/
│       ├── controller/
│       │   └── YYYY-MM-DD/
│       │       ├── index/
│       │       └── details/
│       └── aircraftexchange/
│           └── YYYY-MM-DD/
│               ├── index/
│               ├── manufacturers/
│               └── details/
│
├── logs/                # Log files (auto-generated)
│   ├── controller_log.txt
│   ├── aircraftexchange_log.txt
│   └── faa_log.txt
│
├── docs/                # Documentation
│   ├── SCRAPER_RE-RUN_BEHAVIOR.md
│   ├── CONTROLLER_DETAIL_PARSING_PATTERN.md
│   ├── QUICK_START.md
│   ├── TEST_COMMANDS.md
│   ├── REFACTORING_PLAN.md
│   └── STRUCTURE.md (this file)
│
├── scripts/             # Utility and test scripts
│   ├── __init__.py
│   ├── verify_*.py      # Verification scripts
│   ├── check_*.py       # Check/validation scripts
│   └── test_*.py        # Test scripts
│
├── env/                 # Environment files
│   └── .env.example     # Example environment variables
│
├── .gitignore
├── requirements.txt
└── README.md
```

## Directory Purposes

### `config/`
Centralized configuration management. Loads settings from environment variables and provides a unified config interface.

### `utils/`
Reusable utility functions:
- **logger.py**: Logging setup, formatters, and logger getters
- **chrome_utils.py**: Chrome version detection, driver utilities

### `scrapers/`
Scraper implementations for each data source:
- **Controller.com**: Index and detail scrapers (undetected Chrome)
- **AircraftExchange.com**: Index, manufacturer, manufacturer-detail, and detail scrapers
- **FAA**: Aircraft registration database downloader

### `runners/`
Entry point scripts to run scrapers. These handle:
- Command-line argument parsing
- Logging setup
- Scraper initialization and execution
- Error handling and reporting

### `database/`
Future PostgreSQL database operations:
- Database client
- Models/schemas
- Migrations
- Query utilities

### `store/`
Local file system storage for raw scraped data:
- Organized by source (controller, aircraftexchange)
- Organized by date (YYYY-MM-DD)
- Contains HTML files and JSON metadata

### `logs/`
Auto-generated log files from scraper runs. Each scraper writes to its own log file.

### `docs/`
All documentation:
- Setup guides
- Behavior specifications
- Test commands
- Architecture documentation

### `scripts/`
Utility scripts for:
- Testing scrapers
- Verifying data
- Checking configurations
- Development tools

### `env/`
Environment variable templates and examples.

## Running Scrapers

From the project root:

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

## Import Patterns

All imports use relative paths from the project root:

```python
# From scrapers
from scrapers.controller_scraper_undetected import ControllerScraperUndetected

# From utils
from utils.logger import setup_logging, get_logger
from utils.chrome_utils import get_chrome_version

# From config
from config.config_loader import get_config

```

## Path References

When referencing directories from scripts:

- **From root scripts**: `Path(__file__).parent / "logs"`
- **From runners/**: `Path(__file__).parent.parent / "logs"`
- **From scripts/**: `Path(__file__).parent.parent / "logs"`

This ensures paths work regardless of where the script is located.
