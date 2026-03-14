-- HyeAero ETL Pipeline - PostgreSQL Database Schema
-- Production-grade schema for aviation market data with full traceability and RAG support
-- Captures ALL data from Controller, AircraftExchange, FAA, and Internal DB sources

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE ENTITIES
-- ============================================================================

-- Aircraft master table (canonical aircraft entities)
CREATE TABLE IF NOT EXISTS aircraft (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    serial_number VARCHAR(100),
    registration_number VARCHAR(50) UNIQUE,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    manufacturer_year INTEGER,
    delivery_year INTEGER,
    category VARCHAR(50),
    aircraft_status VARCHAR(50),
    condition VARCHAR(50), -- Used, New, etc.
    number_of_passengers INTEGER,
    registration_country VARCHAR(100),
    based_country VARCHAR(100),
    based_at VARCHAR(100), -- Airport code/location
    airworthiness_date DATE, -- From FAA
    certification VARCHAR(50), -- From FAA
    type_aircraft VARCHAR(50), -- From FAA
    type_engine VARCHAR(50), -- From FAA
    mode_s_code VARCHAR(20), -- From FAA
    mode_s_code_hex VARCHAR(20), -- From FAA
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source_metadata JSONB, -- Source-specific metadata
    CONSTRAINT at_least_one_identifier CHECK (serial_number IS NOT NULL OR registration_number IS NOT NULL)
);

-- Aircraft listings (current/latest snapshot - for sale or off-market)
CREATE TABLE IF NOT EXISTS aircraft_listings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE SET NULL,
    listing_url TEXT NOT NULL,
    source_platform VARCHAR(50) NOT NULL,
    source_listing_id VARCHAR(100),
    listing_status VARCHAR(50) NOT NULL,
    
    -- Pricing
    ask_price DECIMAL(15, 2),
    take_price DECIMAL(15, 2),
    sold_price DECIMAL(15, 2),
    
    -- Dates
    date_listed DATE,
    date_sold DATE,
    days_on_market INTEGER,
    
    -- Location
    location TEXT,
    based_at VARCHAR(100),
    
    -- Description
    description TEXT,
    
    -- Seller/Buyer
    seller VARCHAR(255),
    buyer VARCHAR(255),
    seller_broker VARCHAR(255),
    buyer_broker VARCHAR(255),
    seller_contact_name VARCHAR(255),
    seller_phone VARCHAR(50),
    seller_email VARCHAR(255),
    seller_location VARCHAR(255),
    buyer_contact_name VARCHAR(255),
    buyer_phone VARCHAR(50),
    buyer_email VARCHAR(255),
    
    -- Airframe
    airframe_total_time DECIMAL(10, 1),
    airframe_total_cycles INTEGER,
    airframe_total_landings INTEGER,
    airframe_notes TEXT,
    complete_logs BOOLEAN,
    maintenance_tracking_program VARCHAR(255),
    airframe_program VARCHAR(255),
    
    -- Propellers
    prop_total_time DECIMAL(10, 1),
    props_notes TEXT,
    
    -- Programs
    engine_program VARCHAR(255),
    engine_program_deferment BOOLEAN,
    engine_program_deferment_amount DECIMAL(15, 2),
    apu_program VARCHAR(255),
    apu_program_deferment BOOLEAN,
    apu_program_deferment_amount DECIMAL(15, 2),
    
    -- Interior/Exterior
    interior_year INTEGER,
    exterior_year INTEGER,
    year_painted INTEGER,
    number_of_passengers INTEGER,
    galley VARCHAR(255),
    galley_configuration TEXT,
    interior_notes TEXT,
    exterior_notes TEXT,
    
    -- Features & Equipment
    features JSONB,
    additional_equipment TEXT,
    modifications TEXT,
    avionics_description TEXT,
    avionics_list TEXT,
    
    -- Inspections
    next_inspections JSONB,
    inspection_status TEXT,
    
    -- Other
    has_damage BOOLEAN DEFAULT FALSE,
    is_premium_listing BOOLEAN,
    payment_estimate DECIMAL(15, 2),
    
    -- Metadata
    raw_data JSONB,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft engines (structured engine data)
CREATE TABLE IF NOT EXISTS aircraft_engines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE CASCADE,
    listing_id UUID REFERENCES aircraft_listings(id) ON DELETE SET NULL,
    engine_position INTEGER NOT NULL, -- 1 or 2
    make_model TEXT,
    serial_number VARCHAR(100),
    hours_since_new DECIMAL(10, 1),
    hours_since_overhaul DECIMAL(10, 1),
    hours_since_hot_section DECIMAL(10, 1),
    cycles INTEGER,
    tbo_hours INTEGER,
    tbo_years INTEGER,
    notes TEXT,
    source_platform VARCHAR(50),
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft APUs (structured APU data)
CREATE TABLE IF NOT EXISTS aircraft_apus (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE CASCADE,
    listing_id UUID REFERENCES aircraft_listings(id) ON DELETE SET NULL,
    make_model TEXT,
    hours_since_new DECIMAL(10, 1),
    hours_since_overhaul DECIMAL(10, 1),
    maintenance_program VARCHAR(255),
    notes TEXT,
    source_platform VARCHAR(50),
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft sales history (historical sales data)
CREATE TABLE IF NOT EXISTS aircraft_sales (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE SET NULL,
    serial_number VARCHAR(100),
    registration_number VARCHAR(50),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    manufacturer_year INTEGER,
    delivery_year INTEGER,
    category VARCHAR(50),
    transaction_status VARCHAR(50),
    sold_price DECIMAL(15, 2),
    ask_price DECIMAL(15, 2),
    take_price DECIMAL(15, 2),
    date_sold DATE,
    days_on_market INTEGER,
    airframe_total_time DECIMAL(10, 1),
    apu_total_time DECIMAL(10, 1),
    prop_total_time DECIMAL(10, 1),
    engine_program VARCHAR(255),
    engine_program_deferment BOOLEAN,
    engine_program_deferment_amount DECIMAL(15, 2),
    apu_program VARCHAR(255),
    apu_program_deferment BOOLEAN,
    apu_program_deferment_amount DECIMAL(15, 2),
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
    feature_source VARCHAR(50),
    source_platform VARCHAR(50),
    source_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft listing change history (tracks price/status changes)
CREATE TABLE IF NOT EXISTS aircraft_listing_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id UUID REFERENCES aircraft_listings(id) ON DELETE CASCADE,
    field_name VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_date DATE NOT NULL
);

-- ============================================================================
-- FAA DATA TABLES
-- ============================================================================

-- FAA Aircraft Registrations (from MASTER.txt)
CREATE TABLE IF NOT EXISTS faa_registrations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID REFERENCES aircraft(id) ON DELETE SET NULL,
    n_number VARCHAR(20), -- Can be NULL for records without N-number
    serial_number VARCHAR(100),
    mfr_mdl_code VARCHAR(20),
    eng_mfr_mdl VARCHAR(20),
    year_mfr INTEGER,
    
    -- Registrant Info
    type_registrant INTEGER,
    registrant_name VARCHAR(255),
    street VARCHAR(255),
    street2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(10),
    county VARCHAR(100),
    country VARCHAR(100),
    
    -- Certification
    last_action_date DATE,
    cert_issue_date DATE,
    certification VARCHAR(50),
    expiration_date DATE,
    air_worth_date DATE,
    
    -- Aircraft Type
    type_aircraft VARCHAR(50),
    type_engine VARCHAR(50),
    status_code VARCHAR(10),
    
    -- Other
    mode_s_code VARCHAR(20),
    mode_s_code_hex VARCHAR(20),
    fract_owner VARCHAR(10),
    unique_id VARCHAR(50),
    kit_mfr VARCHAR(100),
    kit_model VARCHAR(100),
    other_names JSONB,
    
    -- Metadata
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(n_number, ingestion_date)
);

-- FAA Aircraft Reference (from ACFTREF.txt)
CREATE TABLE IF NOT EXISTS faa_aircraft_reference (
    code VARCHAR(20) PRIMARY KEY,
    manufacturer VARCHAR(255),
    model VARCHAR(255),
    type_aircraft VARCHAR(50),
    type_engine VARCHAR(50),
    ac_category VARCHAR(50),
    build_cert_ind VARCHAR(10),
    number_of_engines INTEGER,
    number_of_seats INTEGER,
    aircraft_weight VARCHAR(50),
    speed VARCHAR(50),
    tc_data_sheet VARCHAR(255),
    tc_data_holder VARCHAR(255),
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FAA Engine Reference (from ENGINE.txt)
CREATE TABLE IF NOT EXISTS faa_engine_reference (
    code VARCHAR(20) PRIMARY KEY,
    manufacturer VARCHAR(255),
    model VARCHAR(255),
    type VARCHAR(50),
    horsepower INTEGER,
    thrust INTEGER,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FAA Dealers (from DEALER.txt)
CREATE TABLE IF NOT EXISTS faa_dealers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    certificate_number VARCHAR(50),
    ownership INTEGER,
    certificate_date DATE,
    expiration_date DATE,
    expiration_flag VARCHAR(10),
    certificate_issue_count INTEGER,
    name VARCHAR(255),
    street VARCHAR(255),
    street2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    other_names JSONB,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FAA Deregistered Aircraft (from DEREG.txt)
CREATE TABLE IF NOT EXISTS faa_deregistered (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    n_number VARCHAR(20),
    serial_number VARCHAR(100),
    mfr_mdl_code VARCHAR(20),
    status_code VARCHAR(10),
    name VARCHAR(255),
    street_mail VARCHAR(255),
    street2_mail VARCHAR(255),
    city_mail VARCHAR(100),
    state_mail VARCHAR(10),
    zip_code_mail VARCHAR(20),
    county_mail VARCHAR(100),
    country_mail VARCHAR(100),
    street_physical VARCHAR(255),
    street2_physical VARCHAR(255),
    city_physical VARCHAR(100),
    state_physical VARCHAR(10),
    zip_code_physical VARCHAR(20),
    county_physical VARCHAR(100),
    country_physical VARCHAR(100),
    eng_mfr_mdl VARCHAR(20),
    year_mfr INTEGER,
    certification VARCHAR(50),
    region VARCHAR(10),
    air_worth_date DATE,
    cancel_date DATE,
    mode_s_code VARCHAR(20),
    indicator_group VARCHAR(10),
    exp_country VARCHAR(100),
    last_act_date DATE,
    cert_issue_date DATE,
    other_names JSONB,
    kit_mfr VARCHAR(100),
    kit_model VARCHAR(100),
    mode_s_code_hex VARCHAR(20),
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FAA Document Index (from DOCINDEX.txt)
CREATE TABLE IF NOT EXISTS faa_document_index (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type_collateral INTEGER,
    collateral VARCHAR(100),
    party VARCHAR(255),
    doc_id VARCHAR(50),
    drdate DATE,
    processing_date DATE,
    corr_date DATE,
    corr_id VARCHAR(50),
    serial_id VARCHAR(100),
    doc_type VARCHAR(10),
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FAA Reserved Registrations (from RESERVED.txt)
CREATE TABLE IF NOT EXISTS faa_reserved (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    n_number VARCHAR(20),
    registrant VARCHAR(255),
    street VARCHAR(255),
    street2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    rsv_date DATE,
    tr VARCHAR(10),
    exp_date DATE,
    n_num_chg VARCHAR(10),
    purge_date DATE,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- AVIACOST (Operating cost & specs by aircraft type)
-- ============================================================================
-- One row per aircraft type from Aviacost GetAircraftDetails API.
-- Used for cost/spec reference and RAG (Pinecone) search.

CREATE TABLE IF NOT EXISTS aviacost_aircraft_details (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_detail_id INTEGER UNIQUE NOT NULL,
    name TEXT,
    description TEXT,
    manufacturer_id INTEGER,
    manufacturer_name VARCHAR(255),
    category_id INTEGER,
    category_name VARCHAR(100),
    avionics TEXT,
    years_in_production VARCHAR(50),
    average_pre_owned_price DECIMAL(15, 2),
    variable_cost_per_hour DECIMAL(12, 2),
    fuel_gallons_per_hour DECIMAL(10, 2),
    normal_cruise_speed_kts DECIMAL(10, 2),
    seats_full_range_nm DECIMAL(10, 2),
    typical_passenger_capacity_max INTEGER,
    max_takeoff_weight INTEGER,
    powerplant VARCHAR(255),
    engine_model VARCHAR(100),
    last_updated_on TIMESTAMP WITH TIME ZONE,
    raw_data JSONB,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aviacost_aircraft_detail_id ON aviacost_aircraft_details(aircraft_detail_id);
CREATE INDEX IF NOT EXISTS idx_aviacost_manufacturer ON aviacost_aircraft_details(manufacturer_name);
CREATE INDEX IF NOT EXISTS idx_aviacost_category ON aviacost_aircraft_details(category_name);
CREATE INDEX IF NOT EXISTS idx_aviacost_ingestion_date ON aviacost_aircraft_details(ingestion_date);

COMMENT ON TABLE aviacost_aircraft_details IS 'Aircraft type reference and operating cost data from Aviacost (aviacost.com) GetAircraftDetails API.';

-- ============================================================================
-- RAW DATA STORAGE (Append-only)
-- ============================================================================

-- Raw ingested data store (never overwritten, append-only)
CREATE TABLE IF NOT EXISTS raw_data_store (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_platform VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    ingestion_date DATE NOT NULL,
    ingestion_run_id UUID,
    file_path TEXT,
    listing_url TEXT,
    raw_data JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DOCUMENTS (For RAG/ML)
-- ============================================================================

-- Documents table (PDF/TXT extracted text for RAG)
CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_platform VARCHAR(50),
    document_type VARCHAR(50),
    file_path TEXT NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size BIGINT,
    file_hash VARCHAR(64),
    extracted_text TEXT,
    metadata JSONB,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Embedding metadata (tracks which documents have embeddings)
CREATE TABLE IF NOT EXISTS embeddings_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    embedding_model VARCHAR(100) NOT NULL,
    embedding_dimension INTEGER,
    chunk_count INTEGER,
    vector_store VARCHAR(100),
    vector_store_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ETL TRACKING
-- ============================================================================

-- Ingestion runs (tracks each ETL execution)
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_date DATE NOT NULL,
    source_platform VARCHAR(50) NOT NULL,
    run_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    errors JSONB,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Aircraft indexes
CREATE INDEX IF NOT EXISTS idx_aircraft_serial_number ON aircraft(serial_number) WHERE serial_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_aircraft_registration ON aircraft(registration_number);
CREATE INDEX IF NOT EXISTS idx_aircraft_manufacturer_model ON aircraft(manufacturer, model);

-- Aircraft listings indexes
CREATE INDEX idx_listings_listing_url ON aircraft_listings(listing_url);
CREATE INDEX idx_listings_source_platform ON aircraft_listings(source_platform);
CREATE INDEX idx_listings_aircraft_id ON aircraft_listings(aircraft_id);
CREATE INDEX idx_listings_ingestion_date ON aircraft_listings(ingestion_date);
CREATE INDEX idx_listings_status ON aircraft_listings(listing_status);
CREATE UNIQUE INDEX idx_listings_url_date ON aircraft_listings(listing_url, ingestion_date);

-- Engine/APU indexes
CREATE INDEX IF NOT EXISTS idx_engines_aircraft_id ON aircraft_engines(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_engines_listing_id ON aircraft_engines(listing_id);
CREATE INDEX IF NOT EXISTS idx_apus_aircraft_id ON aircraft_apus(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_apus_listing_id ON aircraft_apus(listing_id);

-- Aircraft sales indexes
CREATE INDEX idx_sales_aircraft_id ON aircraft_sales(aircraft_id);
CREATE INDEX idx_sales_date_sold ON aircraft_sales(date_sold);
CREATE INDEX idx_sales_source_platform ON aircraft_sales(source_platform);

-- Listing history indexes
CREATE INDEX IF NOT EXISTS idx_history_listing_id ON aircraft_listing_history(listing_id);
CREATE INDEX IF NOT EXISTS idx_history_ingestion_date ON aircraft_listing_history(ingestion_date);

-- FAA indexes
CREATE INDEX idx_faa_reg_n_number ON faa_registrations(n_number);
CREATE INDEX idx_faa_reg_aircraft_id ON faa_registrations(aircraft_id);
CREATE INDEX idx_faa_reg_ingestion_date ON faa_registrations(ingestion_date);
CREATE INDEX idx_faa_dereg_n_number ON faa_deregistered(n_number);
CREATE INDEX idx_faa_reserved_n_number ON faa_reserved(n_number);

-- Raw data store indexes
CREATE INDEX IF NOT EXISTS idx_raw_source_platform ON raw_data_store(source_platform);
CREATE INDEX IF NOT EXISTS idx_raw_ingestion_date ON raw_data_store(ingestion_date);
CREATE INDEX IF NOT EXISTS idx_raw_listing_url ON raw_data_store(listing_url);

-- Documents indexes
CREATE INDEX idx_documents_source_platform ON documents(source_platform);
CREATE INDEX idx_documents_ingestion_date ON documents(ingestion_date);
CREATE INDEX idx_documents_file_hash ON documents(file_hash);

-- Ingestion runs indexes
CREATE INDEX IF NOT EXISTS idx_runs_run_date ON ingestion_runs(run_date);
CREATE INDEX IF NOT EXISTS idx_runs_source_platform ON ingestion_runs(source_platform);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers
DROP TRIGGER IF EXISTS update_aircraft_updated_at ON aircraft;
CREATE TRIGGER update_aircraft_updated_at BEFORE UPDATE ON aircraft
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_aircraft_listings_updated_at ON aircraft_listings;
CREATE TRIGGER update_aircraft_listings_updated_at BEFORE UPDATE ON aircraft_listings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_engines_updated_at ON aircraft_engines;
CREATE TRIGGER update_engines_updated_at BEFORE UPDATE ON aircraft_engines
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_apus_updated_at ON aircraft_apus;
CREATE TRIGGER update_apus_updated_at BEFORE UPDATE ON aircraft_apus
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_faa_reg_updated_at ON faa_registrations;
CREATE TRIGGER update_faa_reg_updated_at BEFORE UPDATE ON faa_registrations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_documents_updated_at ON documents;
CREATE TRIGGER update_documents_updated_at BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_embeddings_metadata_updated_at ON embeddings_metadata;
CREATE TRIGGER update_embeddings_metadata_updated_at BEFORE UPDATE ON embeddings_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE aircraft IS 'Canonical aircraft master data. One record per unique aircraft.';
COMMENT ON TABLE aircraft_listings IS 'Current/latest snapshot of aircraft listings. Always represents the most recent state.';
COMMENT ON TABLE aircraft_engines IS 'Structured engine data for aircraft. Supports multiple engines per aircraft.';
COMMENT ON TABLE aircraft_apus IS 'Structured APU data for aircraft.';
COMMENT ON TABLE aircraft_sales IS 'Historical sales data from internal CSV and other sources. Append-only.';
COMMENT ON TABLE faa_registrations IS 'FAA aircraft registration data from MASTER.txt. Complete registration details.';
COMMENT ON TABLE faa_aircraft_reference IS 'FAA aircraft reference codes from ACFTREF.txt. Used for decoding manufacturer/model.';
COMMENT ON TABLE faa_engine_reference IS 'FAA engine reference codes from ENGINE.txt.';
COMMENT ON TABLE raw_data_store IS 'Append-only raw data storage. Never overwritten or deleted. Original source data as received.';
COMMENT ON TABLE documents IS 'Documents (PDF/TXT) with extracted text for RAG/ML processing.';
