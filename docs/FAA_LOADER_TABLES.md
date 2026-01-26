# FAA Loader - Expected Database Tables

When you run **only** the FAA loader module, here are the tables that will be populated:

## Command

```powershell
cd D:\HyeAero\etl-pipeline
python runners/run_database_loader.py --faa-only
```

Or with a limit:
```powershell
python runners/run_database_loader.py --limit-faa 10
```

## Tables Populated

### 1. `aircraft` (Master Aircraft Table)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/MASTER.txt`

**Operations:**
- **INSERT**: Creates new aircraft records if they don't exist (by serial_number or registration_number)
- **UPDATE**: Updates existing aircraft records with FAA data

**Fields Updated:**
- `serial_number` (from MASTER.txt)
- `registration_number` (N-number from MASTER.txt)
- `manufacturer` (decoded from MFR_MDL_CODE using ACFTREF lookup)
- `model` (decoded from MFR_MDL_CODE using ACFTREF lookup)
- `manufacturer_year` (from YEAR_MFR)
- `airworthiness_date` (from AIR_WORTH_DATE)
- `certification` (from CERTIFICATION)
- `type_aircraft` (from TYPE_AIRCRAFT)
- `type_engine` (from TYPE_ENGINE)
- `mode_s_code` (from MODE_S_CODE)
- `mode_s_code_hex` (from MODE_S_CODE_HEX)
- `updated_at` (automatically set to CURRENT_TIMESTAMP)

**Key Logic:**
- Uses `_get_or_create_aircraft()` to find/create aircraft
- Decodes manufacturer/model from MFR_MDL_CODE using ACFTREF lookup table
- Links to `faa_registrations` table via aircraft_id

---

### 2. `faa_registrations` (FAA Aircraft Registrations)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/MASTER.txt`

**Operations:**
- **INSERT**: New registration records
- **UPDATE**: Updates existing registrations (by n_number + ingestion_date)

**Fields Inserted:**
- `aircraft_id` (linked to aircraft table)
- `n_number` (N-number, e.g., "N12345")
- `serial_number`
- `mfr_mdl_code` (manufacturer/model code)
- `eng_mfr_mdl` (engine manufacturer/model)
- `year_mfr` (year manufactured)
- `type_registrant` (registrant type code)
- `registrant_name` (owner name)
- `street`, `street2`, `city`, `state`, `zip_code`, `region`, `county`, `country` (address fields)
- `last_action_date`
- `cert_issue_date`
- `certification`
- `expiration_date`
- `air_worth_date`
- `type_aircraft`
- `type_engine`
- `status_code`
- `mode_s_code`, `mode_s_code_hex`
- `fract_owner` (fractional owner)
- `unique_id`
- `kit_mfr`, `kit_model` (kit manufacturer/model)
- `other_names` (JSONB array of alternative names)
- `ingestion_date`
- `created_at`, `updated_at`

**Unique Constraint:** `(n_number, ingestion_date)` - allows same N-number on different dates

---

### 3. `faa_aircraft_reference` (Aircraft Type Reference Codes)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/ACFTREF.txt`

**Operations:**
- **INSERT**: New reference codes
- **UPDATE**: Updates existing codes (by code)

**Fields:**
- `code` (PRIMARY KEY - manufacturer/model code, e.g., "A1")
- `manufacturer` (decoded manufacturer name)
- `model` (decoded model name)
- `type_aircraft` (aircraft type)
- `type_engine` (engine type)
- `ac_category` (aircraft category)
- `build_cert_ind` (build certification indicator)
- `number_of_engines`
- `number_of_seats`
- `aircraft_weight`
- `speed`
- `tc_data_sheet` (type certificate data sheet)
- `tc_data_holder` (type certificate holder)
- `ingestion_date`
- `created_at`, `updated_at`

**Purpose:** Lookup table to decode MFR_MDL_CODE from MASTER.txt into manufacturer/model names

---

### 4. `faa_engine_reference` (Engine Type Reference Codes)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/ENGINE.txt`

**Operations:**
- **INSERT**: New engine reference codes
- **UPDATE**: Updates existing codes (by code)

**Fields:**
- `code` (PRIMARY KEY - engine code)
- `manufacturer` (engine manufacturer)
- `model` (engine model)
- `type` (engine type)
- `horsepower`
- `thrust`
- `ingestion_date`
- `created_at`, `updated_at`

---

### 5. `faa_dealers` (FAA Aircraft Dealers)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/DEALER.txt`

**Operations:**
- **INSERT**: New dealer records
- **UPDATE**: Updates existing dealers (by certificate_number)

**Fields:**
- `certificate_number` (dealer certificate number)
- `ownership` (ownership type code)
- `certificate_date`
- `expiration_date`
- `expiration_flag`
- `certificate_issue_count`
- `name` (dealer name)
- `street`, `street2`, `city`, `state`, `zip_code` (address)
- `other_names` (JSONB array of alternative names)
- `ingestion_date`
- `created_at`, `updated_at`

---

### 6. `faa_deregistered` (Deregistered Aircraft)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/DEREG.txt`

**Operations:**
- **INSERT**: New deregistration records (append-only)

**Fields:**
- `n_number`
- `serial_number`
- `mfr_mdl_code`
- `status_code`
- `name` (registrant name)
- `street_mail`, `street2_mail`, `city_mail`, `state_mail`, `zip_code_mail`, `county_mail`, `country_mail` (mailing address)
- `street_physical`, `street2_physical`, `city_physical`, `state_physical`, `zip_code_physical`, `county_physical`, `country_physical` (physical address)
- `eng_mfr_mdl`
- `year_mfr`
- `certification`
- `region`
- `air_worth_date`
- `cancel_date`
- `mode_s_code`
- `indicator_group`
- `exp_country`
- `last_act_date`
- `cert_issue_date`
- `other_names` (JSONB)
- `kit_mfr`, `kit_model`
- `mode_s_code_hex`
- `ingestion_date`
- `created_at`

---

### 7. `faa_document_index` (FAA Document Index)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/DOCINDEX.txt`

**Operations:**
- **INSERT**: New document index records (append-only)

**Fields:**
- `type_collateral` (collateral type code)
- `collateral` (collateral description)
- `party` (party name)
- `doc_id` (document ID)
- `drdate` (document date)
- `processing_date`
- `corr_date` (correction date)
- `corr_id` (correction ID)
- `serial_id` (serial number/ID)
- `doc_type` (document type)
- `ingestion_date`
- `created_at`

---

### 8. `faa_reserved` (Reserved N-Numbers)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/RESERVED.txt`

**Operations:**
- **INSERT**: New reserved registration records (append-only)

**Fields:**
- `n_number` (reserved N-number)
- `registrant` (reservation holder)
- `street`, `street2`, `city`, `state`, `zip_code` (address)
- `rsv_date` (reservation date)
- `tr` (transfer code)
- `exp_date` (expiration date)
- `n_num_chg` (N-number change indicator)
- `purge_date`
- `ingestion_date`
- `created_at`

---

### 9. `documents` (PDF Documents)

**Source:** `store/raw/faa/YYYY-MM-DD/extracted/*.pdf`

**Operations:**
- **INSERT**: New document records (by file_hash to prevent duplicates)

**Files Processed:**
- `ardata.pdf` (FAA database documentation)
- `FAA_Database_Documentation_YYYY-MM-DD.pdf` (if exists)

**Fields:**
- `source_platform` (always 'faa')
- `document_type` (e.g., 'database_documentation')
- `file_path` (full path to PDF)
- `file_name` (PDF filename)
- `file_size` (file size in bytes)
- `file_hash` (SHA-256 hash of file content)
- `extracted_text` (text extracted from PDF using PyPDF2)
- `metadata` (JSONB with additional metadata)
- `ingestion_date`
- `created_at`, `updated_at`

**Note:** Requires PyPDF2 library. If not installed, PDFs are skipped.

---

### 10. `raw_data_store` (Raw Data Storage)

**Source:** All FAA CSV files and PDFs

**Operations:**
- **INSERT**: Stores raw CSV rows and PDF metadata (append-only, never overwritten)

**Data Stored:**
- Raw CSV rows from: MASTER, ACFTREF, DEALER, DEREG, ENGINE, DOCINDEX, RESERVED
- PDF metadata (not full PDF content, just metadata)

**Fields:**
- `source_platform` (always 'faa')
- `source_type` (e.g., 'master', 'acftref', 'dealer', 'dereg', 'engine', 'docindex', 'reserved', 'pdf')
- `ingestion_date`
- `file_path` (path to source file)
- `raw_data` (JSONB - full CSV row or PDF metadata)
- `metadata` (JSONB - additional metadata)
- `created_at`

**Purpose:** Complete audit trail of all ingested data, never deleted or modified

---

## Summary Table

| Table | Operation | Source File | Key Fields | Notes |
|-------|-----------|-------------|------------|-------|
| `aircraft` | INSERT/UPDATE | MASTER.txt | serial_number, registration_number | Master aircraft data |
| `faa_registrations` | INSERT/UPDATE | MASTER.txt | n_number, ingestion_date | Full registration details |
| `faa_aircraft_reference` | INSERT/UPDATE | ACFTREF.txt | code (PK) | Lookup for manufacturer/model codes |
| `faa_engine_reference` | INSERT/UPDATE | ENGINE.txt | code (PK) | Engine type reference |
| `faa_dealers` | INSERT/UPDATE | DEALER.txt | certificate_number | Dealer information |
| `faa_deregistered` | INSERT | DEREG.txt | n_number, serial_number | Deregistered aircraft (append-only) |
| `faa_document_index` | INSERT | DOCINDEX.txt | doc_id, serial_id | Document index (append-only) |
| `faa_reserved` | INSERT | RESERVED.txt | n_number | Reserved N-numbers (append-only) |
| `documents` | INSERT | *.pdf | file_hash | PDF documents with extracted text |
| `raw_data_store` | INSERT | All files | source_type, ingestion_date | Raw data audit trail |

## Processing Order

1. **ACFTREF.txt** - Loaded first (needed for MASTER.txt decoding)
2. **MASTER.txt** - Main aircraft registration data
3. **DEALER.txt** - Dealer information
4. **DEREG.txt** - Deregistered aircraft
5. **ENGINE.txt** - Engine reference codes
6. **DOCINDEX.txt** - Document index
7. **RESERVED.txt** - Reserved registrations
8. **PDF files** - Documentation files

## Expected Results

After running the FAA loader, you should see:

1. **Aircraft Records:**
   - New aircraft created OR existing aircraft updated with FAA data
   - All fields from MASTER.txt populated
   - Manufacturer/model decoded from ACFTREF lookup

2. **FAA Registration Records:**
   - Full registration details in `faa_registrations`
   - Linked to `aircraft` table via `aircraft_id`
   - One record per N-number per ingestion date

3. **Reference Tables:**
   - ACFTREF codes for decoding manufacturer/model
   - Engine reference codes

4. **Supporting Data:**
   - Dealers, deregistered aircraft, document index, reserved N-numbers

5. **Documents:**
   - PDF files with extracted text (if PyPDF2 available)

6. **Raw Data:**
   - Complete audit trail in `raw_data_store`

## Statistics Returned

The loader returns a dictionary with:
```python
{
    'date': 'YYYY-MM-DD',
    'master': {
        'records': <total processed>,
        'inserted': <new records>,
        'updated': <updated records>,
        'skipped': <skipped records>
    },
    'acftref': {
        'records': <total>,
        'inserted': <new>,
        'skipped': <skipped>
    },
    'dealer': {...},
    'dereg': {...},
    'engine': {...},
    'docindex': {...},
    'reserved': {...},
    'pdfs': {
        'files': <total PDFs>,
        'inserted': <new>,
        'skipped': <duplicates>
    },
    'total_inserted': <sum of all inserted>,
    'total_updated': <sum of all updated>,
    'total_skipped': <sum of all skipped>
}
```

## Verification Queries

### Count records by type:
```sql
-- Count FAA registrations
SELECT COUNT(*) FROM faa_registrations;

-- Count aircraft reference codes
SELECT COUNT(*) FROM faa_aircraft_reference;

-- Count engine reference codes
SELECT COUNT(*) FROM faa_engine_reference;

-- Count dealers
SELECT COUNT(*) FROM faa_dealers;

-- Count deregistered
SELECT COUNT(*) FROM faa_deregistered;

-- Count documents
SELECT COUNT(*) FROM documents WHERE source_platform = 'faa';

-- Count raw data records
SELECT source_type, COUNT(*) 
FROM raw_data_store 
WHERE source_platform = 'faa' 
GROUP BY source_type;
```

### Check latest FAA registrations:
```sql
SELECT 
    r.n_number,
    r.registrant_name,
    r.city,
    r.state,
    a.manufacturer,
    a.model,
    r.ingestion_date
FROM faa_registrations r
LEFT JOIN aircraft a ON r.aircraft_id = a.id
ORDER BY r.created_at DESC
LIMIT 20;
```

### Check ACFTREF lookup:
```sql
SELECT 
    code,
    manufacturer,
    model,
    type_aircraft,
    number_of_engines,
    ingestion_date
FROM faa_aircraft_reference
ORDER BY ingestion_date DESC, code
LIMIT 20;
```

### Check PDF documents:
```sql
SELECT 
    file_name,
    file_size,
    LENGTH(extracted_text) as text_length,
    ingestion_date,
    created_at
FROM documents
WHERE source_platform = 'faa'
ORDER BY created_at DESC;
```

## Notes

- **ACFTREF is loaded first** - Required for decoding MFR_MDL_CODE in MASTER.txt
- **Aircraft linking** - FAA registrations link to `aircraft` table when serial_number or registration_number matches
- **Duplicate prevention** - Uses unique constraints and checks to prevent duplicate inserts
- **Append-only tables** - DEREG, DOCINDEX, RESERVED are append-only (never updated)
- **PDF extraction** - Requires PyPDF2 library. If not available, PDFs are skipped with a warning
- **Raw data storage** - All CSV rows stored in `raw_data_store` for complete audit trail
- **Date-based ingestion** - Each ingestion date creates separate records (allows historical tracking)
