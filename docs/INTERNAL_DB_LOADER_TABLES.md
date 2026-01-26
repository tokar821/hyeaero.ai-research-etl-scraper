# Internal DB Loader - Expected Database Tables

When you run **only** the Internal DB loader module, here are the tables that will be populated:

## Command

```powershell
cd D:\HyeAero\etl-pipeline
python runners/run_database_loader.py --internal-only
```

Or with a limit:
```powershell
python runners/run_database_loader.py --limit-internal 10
```

## Tables Populated

### 1. `aircraft` (Master Aircraft Table)

**Source:** `store/raw/internaldb/aircraft.csv`

**Operations:**
- **INSERT**: Creates new aircraft records if they don't exist (by serial_number or registration_number)
- **UPDATE**: Updates existing aircraft records with data from CSV

**Fields Updated:**
- `manufacturer` (from 'Make')
- `model` (from 'Model')
- `manufacturer_year` (from 'Manufacturer Year')
- `delivery_year` (from 'Delivery Year')
- `category` (from 'Category')
- `aircraft_status` (from 'Aircraft Status')
- `registration_number` (from 'Registration Number')
- `number_of_passengers` (from 'Number of Passengers')
- `registration_country` (from 'Registration Country')
- `based_country` (from 'Based Country')
- `updated_at` (automatically set to CURRENT_TIMESTAMP)

**Key Logic:**
- Uses `_get_or_create_aircraft()` to find existing aircraft by serial_number or registration_number
- If not found, creates new aircraft record
- If found, updates existing record with COALESCE (only updates non-NULL values)

**Example Query to Check:**
```sql
SELECT 
    id, serial_number, registration_number, 
    manufacturer, model, manufacturer_year,
    category, aircraft_status, updated_at
FROM aircraft
WHERE source_metadata IS NULL OR source_metadata->>'source' = 'internal'
ORDER BY updated_at DESC
LIMIT 10;
```

---

### 2. `aircraft_sales` (Historical Sales Data)

**Source:** `store/raw/internaldb/recent_sales.csv`

**Operations:**
- **INSERT**: Appends new sales records (append-only, no updates)
- **SKIP**: If sale already exists (by serial_number + date_sold)

**Fields Inserted:**
- `aircraft_id` (linked to aircraft table if found)
- `serial_number`
- `manufacturer` (from 'Make')
- `model` (from 'Model')
- `manufacturer_year` (from 'Manufacturer Year')
- `delivery_year` (from 'Delivery Year')
- `category` (from 'Category')
- `transaction_status` (from 'Transaction Status')
- `sold_price` (from 'Sold Price')
- `ask_price` (from 'Ask Price')
- `take_price` (from 'Take Price')
- `date_sold` (from 'Date Sold')
- `days_on_market` (from 'Days on market')
- `airframe_total_time` (from 'Airframe Total Time')
- `apu_total_time` (from 'APU Total Time')
- `prop_total_time` (from 'Prop Total Time')
- `engine_program` (from 'Engine Program')
- `engine_program_deferment` (BOOLEAN - True if deferment amount > 0)
- `engine_program_deferment_amount` (from 'Engine Program Deferment Amount')
- `apu_program` (from 'APU Program')
- `apu_program_deferment` (BOOLEAN - True if deferment amount > 0)
- `apu_program_deferment_amount` (from 'APU Program Deferment Amount')
- `airframe_program` (from 'Airframe Program')
- `registration_country` (from 'Registration Country')
- `based_country` (from 'Based Country')
- `number_of_passengers` (from 'Number of Passengers')
- `interior_year` (from 'Interior Year')
- `exterior_year` (from 'Exterior Year')
- `seller` (from 'Seller')
- `buyer` (from 'Buyer')
- `seller_broker` (from 'Seller Broker')
- `buyer_broker` (from 'Buyer Broker')
- `has_damage` (from 'Has Damage' - converted to BOOLEAN)
- `percent_of_book` (from '% of Book')
- `features` (from 'Features' - JSON array of comma-separated features)
- `source_platform` (always set to 'internal')
- `source_data` (full CSV row as JSONB)
- `created_at` (automatically set to CURRENT_TIMESTAMP)

**Key Logic:**
- Checks for duplicates: `WHERE serial_number = X AND date_sold = Y`
- Links to `aircraft` table if aircraft_id found (by serial_number or registration_number)
- Features are parsed from comma-separated string and stored as JSONB array
- Deferment amounts are converted to booleans (True if amount > 0)

**Example Query to Check:**
```sql
SELECT 
    id, serial_number, manufacturer, model,
    date_sold, sold_price, transaction_status,
    source_platform, created_at
FROM aircraft_sales
WHERE source_platform = 'internal'
ORDER BY created_at DESC
LIMIT 10;
```

---

## Summary Table

| Table | Operation | Source File | Key Fields |
|-------|-----------|-------------|------------|
| `aircraft` | INSERT/UPDATE | `aircraft.csv` | serial_number, registration_number, manufacturer, model |
| `aircraft_sales` | INSERT (append-only) | `recent_sales.csv` | serial_number, date_sold, sold_price |

## Expected Results

After running the internal loader, you should see:

1. **Aircraft Records:**
   - New aircraft created OR existing aircraft updated
   - All fields from `aircraft.csv` populated

2. **Sales Records:**
   - New sales records inserted into `aircraft_sales`
   - Each sale linked to aircraft (if aircraft_id found)
   - Full CSV row stored in `source_data` JSONB field

## Statistics Returned

The loader returns a dictionary with:
```python
{
    'aircraft': <number of aircraft rows processed>,
    'sales': <number of sales rows processed>,
    'inserted': <total new records inserted>,
    'updated': <total existing records updated>,
    'skipped': <total records skipped (duplicates or invalid)>
}
```

## Notes

- **No Raw Data Storage**: Unlike other loaders, the internal loader does NOT store data in `raw_data_store` table
- **Append-Only Sales**: Sales records are never updated, only inserted (historical data)
- **Aircraft Updates**: Aircraft records are updated if they already exist (master data)
- **Duplicate Prevention**: Sales are skipped if `serial_number + date_sold` combination already exists
- **Aircraft Linking**: Sales try to link to `aircraft` table, but can exist independently if aircraft not found

## Verification Queries

### Count records by source:
```sql
-- Count aircraft records (internal loader doesn't set source_metadata, so check by updated_at)
SELECT COUNT(*) FROM aircraft;

-- Count internal sales
SELECT COUNT(*) FROM aircraft_sales WHERE source_platform = 'internal';
```

### Check latest internal sales:
```sql
SELECT 
    s.serial_number,
    s.manufacturer,
    s.model,
    s.date_sold,
    s.sold_price,
    s.transaction_status,
    a.registration_number
FROM aircraft_sales s
LEFT JOIN aircraft a ON s.aircraft_id = a.id
WHERE s.source_platform = 'internal'
ORDER BY s.created_at DESC
LIMIT 20;
```

### Check aircraft updates:
```sql
SELECT 
    serial_number,
    registration_number,
    manufacturer,
    model,
    manufacturer_year,
    category,
    aircraft_status,
    updated_at
FROM aircraft
ORDER BY updated_at DESC
LIMIT 20;
```
