# Aviacost Data: Database Architecture & Structure

This document describes how Aviacost aircraft data (from `https://aviacost.com/api/home/GetAircraftDetails`) is stored in **PostgreSQL** and **Pinecone** within the HyeAero ETL and backend.

---

## 1. Data flow

```
Aviacost API (GET GetAircraftDetails)
        │
        ▼
store/raw/aviacost/<YYYY-MM-DD>/aircraft_details.json   (scrape output)
        │
        ▼
ETL: aviacost_loader.py
        │
        ├──► PostgreSQL: aviacost_aircraft_details (structured table)
        └──► PostgreSQL: raw_data_store (append-only raw JSON, optional)
        │
        ▼
Backend RAG pipeline (run_rag_pipeline.py)
        │
        ▼
Pinecone: vectors from aviacost_aircraft_details (entity_type: aviacost_aircraft_detail)
```

---

## 2. PostgreSQL

### 2.1 Table: `aviacost_aircraft_details`

One row per **aircraft type** from Aviacost (reference/cost data, not listings).

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (auto-generated). |
| `aircraft_detail_id` | INTEGER UNIQUE NOT NULL | Source ID from Aviacost API. |
| `name` | TEXT | Aircraft type name (e.g. "Daher-Socata TBM 940"). |
| `description` | TEXT | Long description. |
| `manufacturer_id` | INTEGER | Aviacost manufacturer ID. |
| `manufacturer_name` | VARCHAR(255) | e.g. "Daher-Socata". |
| `category_id` | INTEGER | Aviacost category ID. |
| `category_name` | VARCHAR(100) | e.g. "Turboprops", "Large Jet". |
| `avionics` | TEXT | Avionics description. |
| `years_in_production` | VARCHAR(50) | e.g. "2019-Present". |
| `average_pre_owned_price` | DECIMAL(15,2) | Average pre-owned price. |
| `variable_cost_per_hour` | DECIMAL(12,2) | Total variable cost per flight hour. |
| `fuel_gallons_per_hour` | DECIMAL(10,2) | Fuel burn (gal/hr). |
| `normal_cruise_speed_kts` | DECIMAL(10,2) | Normal cruise speed (knots). |
| `seats_full_range_nm` | DECIMAL(10,2) | Range with full seats (nm). |
| `typical_passenger_capacity_max` | INTEGER | Max passengers. |
| `max_takeoff_weight` | INTEGER | MTOW. |
| `powerplant` | VARCHAR(255) | Engine manufacturer. |
| `engine_model` | VARCHAR(100) | Engine model. |
| `last_updated_on` | TIMESTAMPTZ | Last update from Aviacost. |
| `raw_data` | JSONB | Full API object for the row. |
| `ingestion_date` | DATE NOT NULL | ETL run date (from scrape folder). |
| `created_at` | TIMESTAMPTZ | Row creation time. |
| `updated_at` | TIMESTAMPTZ | Row last update time. |

**Indexes:** `aircraft_detail_id`, `manufacturer_name`, `category_name`, `ingestion_date`.

### 2.2 Raw storage: `raw_data_store` (optional)

If the loader is run with `store_raw=True` (default), each Aviacost record is also appended to `raw_data_store`:

- `source_platform` = `'aviacost'`
- `source_type` = `'aircraft_detail'`
- `ingestion_date` = scrape date
- `file_path` = path to `aircraft_details.json`
- `raw_data` = full JSON object (JSONB)

This keeps an append-only copy of the raw API payload for traceability and reprocessing.

---

## 3. Pinecone (RAG)

- **Source table:** `aviacost_aircraft_details`
- **Entity type:** `aviacost_aircraft_detail`
- **Text for embedding:** Built from name, manufacturer, category, description, avionics, years in production, average pre-owned price, variable cost per hour, fuel, cruise speed, range, passengers, powerplant, engine model (see `AviacostAircraftDetailExtractor` in `backend/rag/entity_extractors.py`).
- **Metadata stored in Pinecone:** `entity_type`, `entity_id` (UUID), `source_platform` (`aviacost`), `name`, `manufacturer_name`, `category_name`, `aircraft_detail_id`, `ingestion_date`.
- **Tracking:** `embeddings_metadata` in PostgreSQL stores `entity_type`, `entity_id`, `embedding_model`, `chunk_count`, `vector_store`, `vector_store_id` for each embedded row.

**Run RAG sync for Aviacost:**

```bash
cd backend
python runners/run_rag_pipeline.py --entities aviacost_aircraft_detail
```

Or include with all entities:

```bash
python runners/run_rag_pipeline.py --entities all
```

---

## 4. ETL usage

1. **Scrape** (writes JSON under `store/raw/aviacost/<date>/`):

   ```bash
   cd etl-pipeline
   python runners/run_aviacost_scraper.py
   ```

2. **Create table** (if not already applied):

   - Table is defined in `etl-pipeline/database/schema.sql` as `aviacost_aircraft_details`. Apply your usual schema migration or run schema creation so this table exists in the same database used by the ETL and backend.

3. **Load into PostgreSQL** (from latest `store/raw/aviacost/<date>/`):

   ```bash
   python runners/run_database_loader.py --aviacost-only
   ```

   Or load all sources (including Aviacost):

   ```bash
   python runners/run_database_loader.py
   ```

   Optional: `--limit-aviacost N` to process only N records (e.g. for testing).

4. **Sync to Pinecone** (from backend):

   ```bash
   cd backend
   python runners/run_rag_pipeline.py --entities aviacost_aircraft_detail
   ```

---

## 5. Design choices

- **Single table:** One normalized table `aviacost_aircraft_details` with flat columns for common filters (name, category, cost, specs) and `raw_data` JSONB for the full payload.
- **Upsert by `aircraft_detail_id`:** Re-runs of the loader update existing rows by `aircraft_detail_id` and set `ingestion_date` to the current run.
- **RAG:** Same pattern as other entities: extract text from the row, chunk, embed, upsert to Pinecone, and record in `embeddings_metadata` so the Consultant can search Aviacost cost/spec content.

This gives you a clear database architecture and structure for Aviacost in both PostgreSQL and Pinecone.
