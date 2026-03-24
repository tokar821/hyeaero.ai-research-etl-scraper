-- FAA Releasable Aircraft (MASTER) snapshot table.
-- One row per aircraft registration row from the FAA MASTER CSV export.
-- Reload the same ingestion_date to upsert (replace) rows for that date.

CREATE TABLE IF NOT EXISTS faa_master (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    n_number VARCHAR(20) NOT NULL,
    serial_number VARCHAR(100),
    mfr_mdl_code VARCHAR(50),
    eng_mfr_mdl VARCHAR(50),
    year_mfr INTEGER,
    type_registrant SMALLINT,
    registrant_name TEXT,
    street TEXT,
    street2 TEXT,
    city VARCHAR(200),
    state VARCHAR(50),
    zip_code VARCHAR(50),
    region VARCHAR(20),
    county VARCHAR(20),
    country VARCHAR(10),

    last_action_date DATE,
    cert_issue_date DATE,
    certification VARCHAR(50),
    type_aircraft VARCHAR(20),
    type_engine VARCHAR(20),
    status_code VARCHAR(20),
    mode_s_code VARCHAR(50),
    fract_owner VARCHAR(20),
    air_worth_date DATE,

    other_name_1 TEXT,
    other_name_2 TEXT,
    other_name_3 TEXT,
    other_name_4 TEXT,
    other_name_5 TEXT,

    expiration_date DATE,
    unique_id VARCHAR(50),
    kit_mfr VARCHAR(200),
    kit_model VARCHAR(200),
    mode_s_code_hex VARCHAR(20),

    source_file TEXT,
    ingestion_date DATE NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_faa_master_n_ingestion UNIQUE (n_number, ingestion_date)
);

CREATE INDEX IF NOT EXISTS idx_faa_master_serial ON faa_master (serial_number);
CREATE INDEX IF NOT EXISTS idx_faa_master_unique_id ON faa_master (unique_id);
CREATE INDEX IF NOT EXISTS idx_faa_master_ingestion ON faa_master (ingestion_date DESC);

COMMENT ON TABLE faa_master IS 'FAA MASTER (releasable aircraft) CSV snapshot, registrant/address from source file.';
