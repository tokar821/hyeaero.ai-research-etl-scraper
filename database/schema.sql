-- HyeAero ETL Pipeline - PostgreSQL Database Schema
-- Production-grade schema for aviation market data with full traceability

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE ENTITIES
-- ============================================================================

-- Aircraft master table (canonical aircraft entities)
CREATE TABLE aircraft (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    serial_number VARCHAR(100) UNIQUE NOT NULL,
    registration_number VARCHAR(20) UNIQUE,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    manufacturer_year INTEGER,
    delivery_year INTEGER,
    category VARCHAR(50),
    aircraft_status VARCHAR(50), -- Active, Retired, etc.
    number_of_passengers INTEGER,
    registration_country VARCHAR(100),
    based_country VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_serial_reg UNIQUE (serial_number, registration_number)
);

-- Aircraft listings (current/latest snapshot - for sale or off-market)
CREATE TABLE aircraft_listings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE SET NULL,
    listing_url TEXT NOT NULL,
    source_platform VARCHAR(50) NOT NULL, -- 'controller', 'aircraftexchange', 'internal'
    source_listing_id VARCHAR(100), -- Original listing ID from source
    listing_status VARCHAR(50) NOT NULL, -- 'for_sale', 'sold', 'off_market', 'pending'
    ask_price DECIMAL(15, 2),
    take_price DECIMAL(15, 2),
    sold_price DECIMAL(15, 2),
    date_listed DATE,
    date_sold DATE,
    days_on_market INTEGER,
    location TEXT,
    description TEXT,
    seller VARCHAR(255),
    buyer VARCHAR(255),
    seller_broker VARCHAR(255),
    buyer_broker VARCHAR(255),
    has_damage BOOLEAN DEFAULT FALSE,
    airframe_total_time DECIMAL(10, 1),
    apu_total_time DECIMAL(10, 1),
    prop_total_time DECIMAL(10, 1),
    engine_program VARCHAR(255),
    engine_program_deferment DECIMAL(15, 2),
    apu_program VARCHAR(255),
    apu_program_deferment DECIMAL(15, 2),
    airframe_program VARCHAR(255),
    maintenance_tracking_program VARCHAR(255),
    interior_year INTEGER,
    exterior_year INTEGER,
    features JSONB, -- Array of feature strings
    next_inspections JSONB, -- Array of inspection dates/types
    props_notes TEXT,
    additional_equipment TEXT,
    exterior_notes TEXT,
    interior_notes TEXT,
    raw_data JSONB, -- Full original JSON payload
    ingestion_date DATE NOT NULL, -- Date when this data was scraped
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft sales history (historical sales data)
CREATE TABLE aircraft_sales (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE SET NULL,
    serial_number VARCHAR(100),
    registration_number VARCHAR(20),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    manufacturer_year INTEGER,
    delivery_year INTEGER,
    category VARCHAR(50),
    transaction_status VARCHAR(50), -- 'sold', 'pending', etc.
    sold_price DECIMAL(15, 2),
    ask_price DECIMAL(15, 2),
    take_price DECIMAL(15, 2),
    date_sold DATE,
    days_on_market INTEGER,
    airframe_total_time DECIMAL(10, 1),
    apu_total_time DECIMAL(10, 1),
    prop_total_time DECIMAL(10, 1),
    engine_program VARCHAR(255),
    engine_program_deferment DECIMAL(15, 2),
    apu_program VARCHAR(255),
    apu_program_deferment DECIMAL(15, 2),
    airframe_program VARCHAR(255),
    registration_country VARCHAR(100),
    based_country VARCHAR(100),
    number_of_passengers INTEGER,
    interior_year INTEGER,
    exterior_year INTEGER,
    seller VARCHAR(255),
    buyer VARCHAR(255),
    seller_broker VARCHAR(255),
    buyer_broker VARCHAR(255),
    has_damage BOOLEAN DEFAULT FALSE,
    percent_of_book DECIMAL(5, 2),
    features JSONB,
    source_platform VARCHAR(50), -- 'internal', 'controller', etc.
    source_data JSONB, -- Original CSV row as JSON
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft listing change history (tracks price/status changes)
CREATE TABLE aircraft_listing_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id UUID REFERENCES aircraft_listings(id) ON DELETE CASCADE,
    field_name VARCHAR(100) NOT NULL, -- 'ask_price', 'listing_status', etc.
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_date DATE NOT NULL -- Date when change was detected
);

-- ============================================================================
-- RAW DATA STORAGE (Append-only)
-- ============================================================================

-- Raw ingested data store (never overwritten, append-only)
CREATE TABLE raw_data_store (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_platform VARCHAR(50) NOT NULL, -- 'controller', 'aircraftexchange', 'faa', 'internal'
    source_type VARCHAR(50) NOT NULL, -- 'index', 'detail', 'manufacturer', 'csv', 'json', 'pdf', 'txt'
    ingestion_date DATE NOT NULL,
    ingestion_run_id UUID, -- Links to ingestion_runs
    file_path TEXT, -- Original file path in store/
    listing_url TEXT, -- If applicable
    raw_data JSONB NOT NULL, -- Original data as received
    metadata JSONB, -- Additional metadata (file size, hash, etc.)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DOCUMENTS (For RAG/ML)
-- ============================================================================

-- Documents table (PDF/TXT extracted text for RAG)
CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_platform VARCHAR(50),
    document_type VARCHAR(50), -- 'pdf', 'txt', 'html'
    file_path TEXT NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size BIGINT,
    file_hash VARCHAR(64), -- SHA-256 hash
    extracted_text TEXT, -- Full extracted text content
    metadata JSONB, -- Document metadata (pages, encoding, etc.)
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Embedding metadata (tracks which documents have embeddings, no vectors stored here)
CREATE TABLE embeddings_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    embedding_model VARCHAR(100) NOT NULL, -- e.g., 'text-embedding-ada-002'
    embedding_dimension INTEGER,
    chunk_count INTEGER, -- Number of chunks this document was split into
    vector_store VARCHAR(100), -- Where vectors are stored (e.g., 'pinecone', 'pgvector')
    vector_store_id VARCHAR(255), -- ID in vector store
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ETL TRACKING
-- ============================================================================

-- Ingestion runs (tracks each ETL execution)
CREATE TABLE ingestion_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_date DATE NOT NULL,
    source_platform VARCHAR(50) NOT NULL,
    run_type VARCHAR(50) NOT NULL, -- 'full', 'incremental', 'backfill'
    status VARCHAR(50) NOT NULL, -- 'running', 'completed', 'failed', 'partial'
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    errors JSONB, -- Array of error messages
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER
);

-- ============================================================================
-- INDEXES (Performance optimization)
-- ============================================================================

-- Aircraft indexes
CREATE INDEX idx_aircraft_serial_number ON aircraft(serial_number);
CREATE INDEX idx_aircraft_registration ON aircraft(registration_number);
CREATE INDEX idx_aircraft_manufacturer_model ON aircraft(manufacturer, model);

-- Aircraft listings indexes
CREATE INDEX idx_listings_listing_url ON aircraft_listings(listing_url);
CREATE INDEX idx_listings_source_platform ON aircraft_listings(source_platform);
CREATE INDEX idx_listings_aircraft_id ON aircraft_listings(aircraft_id);
CREATE INDEX idx_listings_ingestion_date ON aircraft_listings(ingestion_date);
CREATE INDEX idx_listings_status ON aircraft_listings(listing_status);
CREATE INDEX idx_listings_created_at ON aircraft_listings(created_at);
CREATE INDEX idx_listings_updated_at ON aircraft_listings(updated_at);
CREATE INDEX idx_listings_source_listing_id ON aircraft_listings(source_platform, source_listing_id);
CREATE UNIQUE INDEX idx_listings_url_date ON aircraft_listings(listing_url, ingestion_date);

-- Aircraft sales indexes
CREATE INDEX idx_sales_aircraft_id ON aircraft_sales(aircraft_id);
CREATE INDEX idx_sales_serial_number ON aircraft_sales(serial_number);
CREATE INDEX idx_sales_date_sold ON aircraft_sales(date_sold);
CREATE INDEX idx_sales_source_platform ON aircraft_sales(source_platform);

-- Listing history indexes
CREATE INDEX idx_history_listing_id ON aircraft_listing_history(listing_id);
CREATE INDEX idx_history_changed_at ON aircraft_listing_history(changed_at);
CREATE INDEX idx_history_ingestion_date ON aircraft_listing_history(ingestion_date);

-- Raw data store indexes
CREATE INDEX idx_raw_source_platform ON raw_data_store(source_platform);
CREATE INDEX idx_raw_ingestion_date ON raw_data_store(ingestion_date);
CREATE INDEX idx_raw_listing_url ON raw_data_store(listing_url);
CREATE INDEX idx_raw_ingestion_run_id ON raw_data_store(ingestion_run_id);

-- Documents indexes
CREATE INDEX idx_documents_source_platform ON documents(source_platform);
CREATE INDEX idx_documents_ingestion_date ON documents(ingestion_date);
CREATE INDEX idx_documents_file_hash ON documents(file_hash);

-- Embeddings metadata indexes
CREATE INDEX idx_embeddings_document_id ON embeddings_metadata(document_id);
CREATE INDEX idx_embeddings_vector_store ON embeddings_metadata(vector_store);

-- Ingestion runs indexes
CREATE INDEX idx_runs_run_date ON ingestion_runs(run_date);
CREATE INDEX idx_runs_source_platform ON ingestion_runs(source_platform);
CREATE INDEX idx_runs_status ON ingestion_runs(status);

-- ============================================================================
-- TRIGGERS (Auto-update timestamps)
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to tables with updated_at
CREATE TRIGGER update_aircraft_updated_at BEFORE UPDATE ON aircraft
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_aircraft_listings_updated_at BEFORE UPDATE ON aircraft_listings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_documents_updated_at BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_embeddings_metadata_updated_at BEFORE UPDATE ON embeddings_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS (Documentation)
-- ============================================================================

COMMENT ON TABLE aircraft IS 'Canonical aircraft master data. One record per unique aircraft (by serial number).';
COMMENT ON TABLE aircraft_listings IS 'Current/latest snapshot of aircraft listings. Always represents the most recent state.';
COMMENT ON TABLE aircraft_sales IS 'Historical sales data from internal CSV and other sources. Append-only.';
COMMENT ON TABLE aircraft_listing_history IS 'Change history for listings. Tracks field-level changes (price, status, etc.).';
COMMENT ON TABLE raw_data_store IS 'Append-only raw data storage. Never overwritten or deleted. Original source data as received.';
COMMENT ON TABLE documents IS 'Documents (PDF/TXT) with extracted text for RAG/ML processing.';
COMMENT ON TABLE embeddings_metadata IS 'Metadata about document embeddings. Vectors stored externally (e.g., Pinecone, pgvector).';
COMMENT ON TABLE ingestion_runs IS 'Tracks each ETL ingestion run for auditing and monitoring.';
