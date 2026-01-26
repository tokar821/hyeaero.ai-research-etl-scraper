# Data Loader Refactoring Plan

## Current State
- `data_loader.py`: 2774 lines - monolithic file with all loaders
- All data sources (Controller, AircraftExchange, FAA, Internal) in one file
- Hard to maintain and test

## Target Architecture

```
database/
├── base_loader.py          # Base class with shared utilities
├── data_loader.py          # Main orchestrator (simplified)
├── loaders/
│   ├── __init__.py
│   ├── controller_loader.py
│   ├── aircraftexchange_loader.py
│   ├── faa_loader.py
│   └── internal_loader.py
└── utils.py                # Shared utility functions (if needed)
```

## Refactoring Steps

1. ✅ Create `base_loader.py` with:
   - `find_latest_date()`
   - `_parse_price()`, `_parse_date()`, `_parse_int()`, `_parse_float()`
   - `_clean_registration()`
   - `_get_or_create_aircraft()`
   - `_store_engine()`, `_store_apu()`
   - `_store_raw_data()`

2. Create individual loaders (inherit from BaseLoader):
   - `ControllerLoader`: `load_controller_data()`, `_upsert_controller_listing()`, `_upsert_controller_detail()`
   - `AircraftExchangeLoader`: `load_aircraftexchange_data()`, `_upsert_aircraftexchange_listing()`, `_upsert_aircraftexchange_detail()`
   - `FAALoader`: `load_faa_data()`, all `_store_faa_*()` methods, `_upsert_faa_aircraft()`
   - `InternalLoader`: `load_internal_db_data()`, `_upsert_internal_aircraft()`, `_upsert_internal_sale()`

3. Refactor `data_loader.py` to:
   - Import all loaders
   - Create instances
   - Orchestrate via `load_all_latest()`

## Benefits

- **Separation of Concerns**: Each loader handles one data source
- **Maintainability**: Easier to find and fix issues
- **Testability**: Can test each loader independently
- **Scalability**: Easy to add new data sources
- **Code Organization**: Clear structure, easier navigation
