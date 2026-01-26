# Quick Test Reference - Loader Modules

Quick copy-paste commands for testing each loader module.

**⚠️ Important: All commands must be run from the `etl-pipeline` directory!**

```powershell
# First, navigate to the etl-pipeline directory
cd D:\HyeAero\etl-pipeline
```

## 🧪 Import Tests (Verify Modules Load)

```bash
# Test all imports
python -c "from database.loaders import ControllerLoader, AircraftExchangeLoader, FAALoader, InternalLoader; from database.data_loader import DataLoader; print('✅ All modules OK')"
```

## 📦 Test Individual Loaders

### Controller Loader
```bash
# 10 records
python runners/run_database_loader.py --limit-controller 10

# 50 records
python runners/run_database_loader.py --limit-controller 50

# All records
python runners/run_database_loader.py --controller-only
```

### AircraftExchange Loader
```bash
# 10 records
python runners/run_database_loader.py --limit-aircraftexchange 10

# 50 records
python runners/run_database_loader.py --limit-aircraftexchange 50

# All records
python runners/run_database_loader.py --aircraftexchange-only
```

### FAA Loader
```bash
# 10 records
python runners/run_database_loader.py --limit-faa 10

# 100 records
python runners/run_database_loader.py --limit-faa 100

# All records
python runners/run_database_loader.py --faa-only
```

### Internal Loader
```bash
# 10 records
python runners/run_database_loader.py --limit-internal 10

# 50 records
python runners/run_database_loader.py --limit-internal 50

# All records
python runners/run_database_loader.py --internal-only
```

## 🔄 Test All Together

```bash
# Quick test (10 each)
python runners/run_database_loader.py --test

# All data (no limits)
python runners/run_database_loader.py
```

## 🎯 Test Specific Combinations

```bash
# Controller + AircraftExchange
python runners/run_database_loader.py --limit-controller 10 --limit-aircraftexchange 10 --limit-faa -1 --limit-internal -1

# Controller + FAA
python runners/run_database_loader.py --limit-controller 10 --limit-faa 100 --limit-aircraftexchange -1 --limit-internal -1

# Custom limits
python runners/run_database_loader.py --limit-controller 25 --limit-aircraftexchange 30 --limit-faa 200 --limit-internal 15
```

## 📝 Quick Verification Checklist

Run these in order:

```bash
# 1. Test imports
python -c "from database.data_loader import DataLoader; print('✅ Import OK')"

# 2. Test Controller (10 records)
python runners/run_database_loader.py --limit-controller 10

# 3. Test AircraftExchange (10 records)
python runners/run_database_loader.py --limit-aircraftexchange 10

# 4. Test FAA (10 records)
python runners/run_database_loader.py --limit-faa 10

# 5. Test Internal (10 records)
python runners/run_database_loader.py --limit-internal 10

# 6. Test all together
python runners/run_database_loader.py --test
```

## 🔍 Check Logs

After running tests, check the logs:
```bash
# View latest log entries
Get-Content D:\HyeAero\etl-pipeline\logs\database_loader_log.txt -Tail 50
```
