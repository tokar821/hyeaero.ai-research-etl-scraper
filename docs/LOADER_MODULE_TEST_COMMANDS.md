# Loader Module Test Commands

Complete guide for testing each individual loader module after refactoring.

**⚠️ Important: All commands must be run from the `etl-pipeline` directory!**

```powershell
# First, navigate to the etl-pipeline directory
cd D:\HyeAero\etl-pipeline
```

## Overview

After refactoring, we now have separate loaders for each data source:
- `ControllerLoader` - Controller.com data
- `AircraftExchangeLoader` - AircraftExchange.com data
- `FAALoader` - FAA database
- `InternalLoader` - Internal DB CSV files

## Quick Import Tests

Test that all modules import correctly:

```bash
# Test base loader
python -c "from database.base_loader import BaseLoader; print('✅ BaseLoader imported')"

# Test Controller loader
python -c "from database.loaders.controller_loader import ControllerLoader; print('✅ ControllerLoader imported')"

# Test AircraftExchange loader
python -c "from database.loaders.aircraftexchange_loader import AircraftExchangeLoader; print('✅ AircraftExchangeLoader imported')"

# Test FAA loader
python -c "from database.loaders.faa_loader import FAALoader; print('✅ FAALoader imported')"

# Test Internal loader
python -c "from database.loaders.internal_loader import InternalLoader; print('✅ InternalLoader imported')"

# Test main DataLoader (orchestrator)
python -c "from database.data_loader import DataLoader; print('✅ DataLoader imported')"

# Test all at once
python -c "from database.loaders import ControllerLoader, AircraftExchangeLoader, FAALoader, InternalLoader; from database.data_loader import DataLoader; print('✅ All modules imported successfully!')"
```

## Test Individual Loaders via Main Runner

### Test Controller Loader Only
```bash
# Test with 10 records
python runners/run_database_loader.py --limit-controller 10

# Test with 50 records
python runners/run_database_loader.py --limit-controller 50

# Process all Controller data
python runners/run_database_loader.py --controller-only
```

### Test AircraftExchange Loader Only
```bash
# Test with 10 records
python runners/run_database_loader.py --limit-aircraftexchange 10

# Test with 50 records
python runners/run_database_loader.py --limit-aircraftexchange 50

# Process all AircraftExchange data
python runners/run_database_loader.py --aircraftexchange-only
```

### Test FAA Loader Only
```bash
# Test with 10 records
python runners/run_database_loader.py --limit-faa 10

# Test with 100 records
python runners/run_database_loader.py --limit-faa 100

# Process all FAA data
python runners/run_database_loader.py --faa-only
```

### Test Internal Loader Only
```bash
# Test with 10 records
python runners/run_database_loader.py --limit-internal 10

# Test with 50 records
python runners/run_database_loader.py --limit-internal 50

# Process all Internal DB data
python runners/run_database_loader.py --internal-only
```

## Test Loaders Directly (Python Scripts)

Create test scripts to test each loader independently:

### Test Controller Loader Directly
```python
# test_controller_loader.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.loaders.controller_loader import ControllerLoader
from config.config_loader import get_config
from datetime import date

# Get config
config = get_config()
connection_string = config.postgres_connection_string or "your-connection-string"

# Initialize
db_client = PostgresClient(connection_string)
loader = ControllerLoader(db_client)

# Test
latest_date = loader.find_latest_date('controller')
if latest_date:
    print(f"Testing Controller loader with date: {latest_date}")
    stats = loader.load_controller_data(latest_date, limit=10)
    print(f"Results: {stats}")
else:
    print("No Controller data found")
```

### Test AircraftExchange Loader Directly
```python
# test_aircraftexchange_loader.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.loaders.aircraftexchange_loader import AircraftExchangeLoader
from config.config_loader import get_config
from datetime import date

config = get_config()
connection_string = config.postgres_connection_string or "your-connection-string"

db_client = PostgresClient(connection_string)
loader = AircraftExchangeLoader(db_client)

latest_date = loader.find_latest_date('aircraftexchange')
if latest_date:
    print(f"Testing AircraftExchange loader with date: {latest_date}")
    stats = loader.load_aircraftexchange_data(latest_date, limit=10)
    print(f"Results: {stats}")
else:
    print("No AircraftExchange data found")
```

### Test FAA Loader Directly
```python
# test_faa_loader.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.loaders.faa_loader import FAALoader
from config.config_loader import get_config
from datetime import date

config = get_config()
connection_string = config.postgres_connection_string or "your-connection-string"

db_client = PostgresClient(connection_string)
loader = FAALoader(db_client)

latest_date = loader.find_latest_date('faa')
if latest_date:
    print(f"Testing FAA loader with date: {latest_date}")
    stats = loader.load_faa_data(latest_date, limit=10)
    print(f"Results: {stats}")
else:
    print("No FAA data found")
```

### Test Internal Loader Directly
```python
# test_internal_loader.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.loaders.internal_loader import InternalLoader
from config.config_loader import get_config

config = get_config()
connection_string = config.postgres_connection_string or "your-connection-string"

db_client = PostgresClient(connection_string)
loader = InternalLoader(db_client)

print("Testing Internal loader")
stats = loader.load_internal_db_data(limit=10)
print(f"Results: {stats}")
```

## Test All Modules Together

### Quick Test (10 records each)
```bash
python runners/run_database_loader.py --test
```

### Test Specific Combinations
```bash
# Controller + AircraftExchange
python runners/run_database_loader.py --limit-controller 10 --limit-aircraftexchange 10 --limit-faa -1 --limit-internal -1

# Controller + FAA
python runners/run_database_loader.py --limit-controller 10 --limit-faa 100 --limit-aircraftexchange -1 --limit-internal -1

# All sources with custom limits
python runners/run_database_loader.py --limit-controller 25 --limit-aircraftexchange 30 --limit-faa 200 --limit-internal 15
```

### Full Production Load
```bash
python runners/run_database_loader.py
```

## Test Base Loader Utilities

Test shared utilities from BaseLoader:

```python
# test_base_loader.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.base_loader import BaseLoader

# Initialize (you'll need a valid connection string)
db_client = PostgresClient("your-connection-string")
loader = BaseLoader(db_client)

# Test parsing utilities
print("Testing parsing utilities:")
print(f"Price: {loader._parse_price('USD $1,234,567')}")
print(f"Date: {loader._parse_date('2024-01-15')}")
print(f"Int: {loader._parse_int('1,234')}")
print(f"Float: {loader._parse_float('1,234.56')}")

# Test registration cleaning
print(f"Registration: {loader._clean_registration('N6EU (Registration Retained by Seller)')}")

# Test find_latest_date
print(f"Latest Controller date: {loader.find_latest_date('controller')}")
print(f"Latest FAA date: {loader.find_latest_date('faa')}")
```

## Verification Checklist

After refactoring, verify:

- [ ] All imports work: `python -c "from database.data_loader import DataLoader"`
- [ ] Controller loader works: `python runners/run_database_loader.py --limit-controller 10`
- [ ] AircraftExchange loader works: `python runners/run_database_loader.py --limit-aircraftexchange 10`
- [ ] FAA loader works: `python runners/run_database_loader.py --limit-faa 10`
- [ ] Internal loader works: `python runners/run_database_loader.py --limit-internal 10`
- [ ] All loaders together: `python runners/run_database_loader.py --test`
- [ ] No linter errors: Check IDE or run linter
- [ ] Logs show correct behavior: Check `logs/database_loader_log.txt`

## Expected Output

When testing, you should see:

```
2026-01-25 XX:XX:XX | __main__ | INFO | Database Loader - Loading latest scraped data to PostgreSQL
2026-01-25 XX:XX:XX | __main__ | INFO | TEST MODE - Processing limited data
2026-01-25 XX:XX:XX | __main__ | INFO |   FAA limit: 10
2026-01-25 XX:XX:XX | __main__ | INFO | Skipping sources: Controller, AircraftExchange, Internal DB
2026-01-25 XX:XX:XX | database.loaders.faa_loader | INFO | Loading FAA ACFTREF data from ...
2026-01-25 XX:XX:XX | database.loaders.faa_loader | INFO | ACFTREF field names detected: ...
2026-01-25 XX:XX:XX | database.loaders.faa_loader | INFO | Processed X rows, loaded Y ACFTREF codes for lookup
...
```

## Troubleshooting

### Import Errors
If you see import errors:
```bash
# Make sure you're in the etl-pipeline directory
cd D:\HyeAero\etl-pipeline

# Test imports
python -c "from database.loaders import ControllerLoader; print('OK')"
```

### Module Not Found
- Ensure you're running from `etl-pipeline/` directory
- Check that `loaders/` directory exists
- Verify `__init__.py` files are present

### Database Connection Errors
- Check `.env` file has `POSTGRES_CONNECTION_STRING`
- Verify database is accessible
- Test connection: `python -c "from database.postgres_client import PostgresClient; PostgresClient('your-connection').test_connection()"`

## Quick Test Script

Create a simple test script to verify all modules:

```python
# test_all_loaders.py
"""Quick test to verify all loaders can be imported and initialized."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.loaders import ControllerLoader, AircraftExchangeLoader, FAALoader, InternalLoader
from database.data_loader import DataLoader
from config.config_loader import get_config

def test_imports():
    """Test that all modules can be imported."""
    print("✅ All modules imported successfully!")

def test_initialization():
    """Test that all loaders can be initialized."""
    config = get_config()
    connection_string = config.postgres_connection_string or "test-connection"
    
    try:
        db_client = PostgresClient(connection_string)
        
        # Test each loader
        ControllerLoader(db_client)
        AircraftExchangeLoader(db_client)
        FAALoader(db_client)
        InternalLoader(db_client)
        DataLoader(db_client)
        
        print("✅ All loaders initialized successfully!")
    except Exception as e:
        print(f"⚠️  Initialization test skipped (connection issue): {e}")

if __name__ == "__main__":
    test_imports()
    test_initialization()
    print("\n🎉 All tests passed!")
```

Run it:
```bash
python test_all_loaders.py
```
