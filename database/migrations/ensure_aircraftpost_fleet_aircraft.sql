-- Ensure AircraftPost fleet extracted table exists.
-- Stores one row per AircraftPost aircraft_entity_id per ingestion_date.
-- Preserves full extracted table (fields + sections) as JSONB for flexibility.

CREATE TABLE IF NOT EXISTS aircraftpost_fleet_aircraft (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Source identifiers
    make_model_id INTEGER,
    make_model_name TEXT,
    aircraft_entity_id INTEGER NOT NULL,
    serial_number VARCHAR(100),
    registration_number VARCHAR(50),

    -- Common query fields (best-effort extracted from 'fields')
    mfr_year INTEGER,
    eis_date VARCHAR(20),
    country_code VARCHAR(10),
    base_code VARCHAR(20),
    owner_url TEXT,
    airframe_hours INTEGER,
    total_landings INTEGER,
    prior_owners INTEGER,
    for_sale BOOLEAN,
    passengers INTEGER,
    engine_program_type VARCHAR(100),
    apu_program VARCHAR(100),

    -- Full extracted data
    fields JSONB,
    sections JSONB,

    -- Lineage
    source_file_path TEXT,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(aircraft_entity_id, ingestion_date)
);

CREATE INDEX IF NOT EXISTS idx_aircraftpost_ingestion_date ON aircraftpost_fleet_aircraft(ingestion_date);
CREATE INDEX IF NOT EXISTS idx_aircraftpost_make_model_id ON aircraftpost_fleet_aircraft(make_model_id);
CREATE INDEX IF NOT EXISTS idx_aircraftpost_serial ON aircraftpost_fleet_aircraft(serial_number);
CREATE INDEX IF NOT EXISTS idx_aircraftpost_registration ON aircraftpost_fleet_aircraft(registration_number);
CREATE INDEX IF NOT EXISTS idx_aircraftpost_mfr_year ON aircraftpost_fleet_aircraft(mfr_year);

COMMENT ON TABLE aircraftpost_fleet_aircraft IS 'AircraftPost Fleet Detail extracted aircraft table (pivoted wide-table). One row per AircraftPost aircraft_entity_id per ingestion_date.';

