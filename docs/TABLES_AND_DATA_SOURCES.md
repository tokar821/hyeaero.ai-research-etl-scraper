# Tables and Data Sources

Each database table and the raw data source(s) that populate it.

| Table | Data source(s) | Raw file / location |
|-------|----------------|---------------------|
| **aircraft** | Internal DB, Controller, AircraftExchange, FAA | internaldb: `aircraft.csv`; Controller: `index/listings_metadata.json`, `details/details_metadata.json`; AircraftExchange: same; FAA: `MASTER.txt` (via registration link) |
| **aircraft_listings** | Controller, AircraftExchange | Controller: `store/raw/controller/<date>/index/listings_metadata.json`, `details/details_metadata.json`; AircraftExchange: `store/raw/aircraftexchange/<date>/index/listings_metadata.json`, `details/details_metadata.json` |
| **aircraft_engines** | Controller, AircraftExchange | From listing details (Controller/AircraftExchange `details/details_metadata.json`) |
| **aircraft_apus** | Controller, AircraftExchange | From listing details (Controller/AircraftExchange `details/details_metadata.json`) |
| **aircraft_sales** | Internal DB | internaldb: `recent_sales.csv` |
| **aircraft_listing_history** | Controller, AircraftExchange | Derived when loader updates listings (tracks field changes) |
| **faa_registrations** | FAA | FAA: `MASTER.txt` (from extracted ZIP) |
| **faa_aircraft_reference** | FAA | FAA: `ACFTREF.txt` |
| **faa_engine_reference** | FAA | FAA: `ENGINE.txt` |
| **faa_dealers** | FAA | FAA: `DEALER.txt` |
| **faa_deregistered** | FAA | FAA: `DEREG.txt` |
| **faa_document_index** | FAA | FAA: `DOCINDEX.txt` |
| **faa_reserved** | FAA | FAA: `RESERVED.txt` |
| **raw_data_store** | All sources | All loaders write raw JSON/CSV/TXT per run (append-only) |
| **documents** | FAA (and any PDF/TXT ingest) | FAA PDFs under `store/raw/faa/<date>/`; other document paths as configured |
| **embeddings_metadata** | Backend / RAG pipeline | Populated when generating embeddings (references documents, listings, etc.) |
| **ingestion_runs** | ETL pipeline | Created by database loader on each run |

## Short reference (table → primary source file)

| Table | Source | File(s) |
|-------|--------|--------|
| aircraft | internaldb | `aircraft.csv` |
| aircraft | Controller | `listings_metadata.json`, `details_metadata.json` |
| aircraft | AircraftExchange | `listings_metadata.json`, `details_metadata.json` |
| aircraft_listings | Controller | `listings_metadata.json`, `details_metadata.json` |
| aircraft_listings | AircraftExchange | `listings_metadata.json`, `details_metadata.json` |
| aircraft_engines | Controller | from details |
| aircraft_engines | AircraftExchange | from details |
| aircraft_apus | Controller | from details |
| aircraft_apus | AircraftExchange | from details |
| aircraft_sales | internaldb | `recent_sales.csv` |
| aircraft_listing_history | Controller, AircraftExchange | derived from listing updates |
| faa_registrations | FAA | `MASTER.txt` |
| faa_aircraft_reference | FAA | `ACFTREF.txt` |
| faa_engine_reference | FAA | `ENGINE.txt` |
| faa_dealers | FAA | `DEALER.txt` |
| faa_deregistered | FAA | `DEREG.txt` |
| faa_document_index | FAA | `DOCINDEX.txt` |
| faa_reserved | FAA | `RESERVED.txt` |
| raw_data_store | all | (raw payloads from each loader) |
| documents | FAA / docs | PDFs, extracted text |
| embeddings_metadata | backend | (RAG/embedding pipeline) |
| ingestion_runs | ETL | (run metadata) |
