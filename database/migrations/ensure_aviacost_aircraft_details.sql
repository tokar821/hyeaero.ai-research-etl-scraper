-- Ensure aviacost_aircraft_details table exists (for DBs created before this table was added to schema.sql).
-- Safe to run multiple times (CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT EXISTS).

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
