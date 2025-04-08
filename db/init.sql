-- Create the restaurants table
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id TEXT PRIMARY KEY,
    license_ TEXT,
    dba_name TEXT,
    aka_name TEXT,
    facility_type TEXT,
    risk TEXT,
    address TEXT,
    city TEXT,
    state CHAR(2),
    zip TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- Create the inspections table
CREATE TABLE IF NOT EXISTS inspections (
    inspection_id BIGINT PRIMARY KEY,
    restaurant_id TEXT REFERENCES restaurants(restaurant_id),
    inspection_date DATE,
    inspection_type TEXT,
    results TEXT
);

-- Create the violations table
CREATE TABLE IF NOT EXISTS violations (
    id SERIAL PRIMARY KEY,
    inspection_id BIGINT REFERENCES inspections(inspection_id) ON DELETE CASCADE,
    violation_number INT,
    violation_description TEXT,
    violation_comments TEXT
);
