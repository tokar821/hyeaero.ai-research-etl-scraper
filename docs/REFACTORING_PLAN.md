# ETL Pipeline Refactoring Plan

## Current Structure Issues
- Runner scripts (`run_*.py`) scattered in root
- Documentation files mixed in root
- Test/verify scripts in root
- No clear separation for database operations
- No dedicated `runners/` or `env/` directories

## Target Structure

```
etl-pipeline/
├── config/              # Configuration management (keep)
│   ├── __init__.py
│   └── config_loader.py
├── utils/               # Utility functions (keep)
│   ├── __init__.py
│   ├── logger.py
│   └── chrome_utils.py
├── scrapers/            # Scraper modules (keep)
│   ├── __init__.py
│   ├── controller_*.py
│   ├── aircraftexchange_*.py
│   └── faa_scraper.py
├── runners/             # NEW: Runner scripts
│   ├── __init__.py
│   ├── run_controller_scraper.py
│   ├── run_controller_detail_scraper.py
│   ├── run_aircraftexchange_scraper.py
│   └── run_faa_scraper.py
├── database/            # NEW: Database operations (PostgreSQL)
│   ├── __init__.py
│   └── (future: PostgreSQL client, models, migrations)
├── store/               # Raw data storage (keep, already exists)
│   └── raw/
│       ├── controller/
│       └── aircraftexchange/
├── logs/                # Log files (create if doesn't exist)
│   ├── controller_log.txt
│   ├── aircraftexchange_log.txt
│   └── faa_log.txt
├── docs/                # All documentation
│   ├── SCRAPER_RE-RUN_BEHAVIOR.md
│   ├── CONTROLLER_DETAIL_PARSING_PATTERN.md
│   ├── QUICK_START.md
│   ├── EXPLANATION.md
│   └── TEST_COMMANDS.md
├── scripts/             # NEW: Utility/test scripts
│   ├── verify_*.py
│   ├── check_*.py
│   └── test_*.py
├── env/                 # NEW: Environment files
│   ├── .env.example
│   └── .env (gitignored)
├── __init__.py
├── .gitignore
├── requirements.txt
└── README.md
```

## Migration Steps

1. Create new directories: `runners/`, `database/`, `scripts/`, `env/`
2. Move runner scripts: `run_*.py` → `runners/`
3. Move documentation: `*.md` (except README.md) → `docs/`
4. Move test/verify scripts: `*_*.py` (test, verify, check) → `scripts/`
5. Create `env/` directory and move `.env.example` there
6. Update all import statements
7. Update path references in scripts (logs/, store/, etc.)
8. Update README.md with new structure

## Import Path Changes

### Before:
```python
from utils.logger import setup_logging
from scrapers.controller_scraper import ControllerScraper
```

### After:
```python
from utils.logger import setup_logging
from scrapers.controller_scraper import ControllerScraper
# (No change - relative imports work the same)
```

### Runner Scripts:
```python
# Before: run_controller_scraper.py (in root)
from scrapers.controller_scraper_undetected import ControllerScraperUndetected

# After: runners/run_controller_scraper.py
from scrapers.controller_scraper_undetected import ControllerScraperUndetected
# (No change - Python path resolution works)
```

## Path References to Update

- Log file paths: `Path(__file__).parent / "logs"` → `Path(__file__).parent.parent / "logs"`
- Store paths: Already using relative paths, should work
- Config paths: Already using relative paths
