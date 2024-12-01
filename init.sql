-- Define Scope Enum for emissions
CREATE TYPE scope_enum AS ENUM ('1', '2', '3');

CREATE TABLE IF NOT EXISTS emission_sources (
    id SERIAL PRIMARY KEY,
    scope scope_enum NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    description TEXT,
    UNIQUE (scope, source_name) -- Ensures each scope-source_name combination is unique
);


CREATE TABLE IF NOT EXISTS emission_factors (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES emission_sources(id),
    factor_value DECIMAL,
    unit VARCHAR(50),
    calculation_method TEXT
);

CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS emissions_logs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES emission_sources(id),
    organization_id INTEGER REFERENCES organizations(id),  -- Foreign key to organizations table
    date DATE,
    quantity DECIMAL,
    total_emissions DECIMAL
);
