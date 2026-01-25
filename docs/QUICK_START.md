# Quick Start Guide

Get up and running with the ETL pipeline in 5 minutes.

## Prerequisites Check

Before starting, make sure you have:

- ✅ Python 3.12 installed (`python --version`)
- ✅ Chrome browser installed (for web scraping)

## Step 1: Install Dependencies (1 minute)

```bash
# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Step 2: Run a Test Scrape (2 minutes)

```bash
# Test Controller index scraper (1 page)
python runners/run_controller_scraper.py --max-pages 1

# Test Controller detail scraper (2 listings)
python runners/run_controller_detail_scraper.py --max-listings 2 --cooldown-every 0

# Test AircraftExchange index scraper (1 page)
python runners/run_aircraftexchange_scraper.py --index --max-pages 1
```

## Step 3: Check Results

After running, check:

1. **Log files**: `logs/controller_log.txt` or `logs/aircraftexchange_log.txt`
2. **Scraped data**: `store/raw/controller/YYYY-MM-DD/` or `store/raw/aircraftexchange/YYYY-MM-DD/`

## Step 4: Run Full Scrapes

Once testing works, run full scrapes:

```bash
# Controller index (all pages)
python runners/run_controller_scraper.py

# Controller detail (all listings)
python runners/run_controller_detail_scraper.py

# AircraftExchange (all modules)
python runners/run_aircraftexchange_scraper.py --all

# FAA database
python runners/run_faa_scraper.py
```

## Configuration (Optional)

Environment variables (optional, defaults work for local):

- `ENVIRONMENT`: `dev`, `prod`, or `local` (default: `local`)
- `LOG_LEVEL`: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)

Create `.env` file in project root if needed:

```env
ENVIRONMENT=local
LOG_LEVEL=INFO
```

## Troubleshooting

1. **Chrome not found**: Install Chrome browser
2. **Import errors**: Make sure virtual environment is activated
3. **Permission errors**: Check write permissions for `store/` and `logs/` directories

## Next Steps

- 📖 Read [TEST_COMMANDS.md](./TEST_COMMANDS.md) for more test commands
- 📖 Read [SCRAPER_RE-RUN_BEHAVIOR.md](./SCRAPER_RE-RUN_BEHAVIOR.md) to understand re-run behavior
- 📖 Read [STRUCTURE.md](./STRUCTURE.md) for project structure details

**Time to complete**: ~5 minutes
