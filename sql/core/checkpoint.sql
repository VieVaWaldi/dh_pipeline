-----------------------------------------------
-- Transformation Checkpoints

CREATE TABLE core.import_checkpoint (
    source_system core.source_type NOT NULL,
    table_name TEXT NOT NULL,
    last_processed_timestamp TIMESTAMP NOT NULL,
    records_processed INTEGER DEFAULT 0,
    last_run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, table_name)
);

-----------------------------------------------
-- Initialize Checkpoints for all Sources Main Tables

INSERT INTO core.import_checkpoint (source_system, table_name, last_processed_timestamp)
VALUES ('arxiv', 'entry', '1970-01-01 00:00:00')
ON CONFLICT (source_system, table_name) DO NOTHING;

INSERT INTO core.import_checkpoint (source_system, table_name, last_processed_timestamp)
VALUES ('cordis', 'researchoutput', '1970-01-01 00:00:00')
ON CONFLICT (source_system, table_name) DO NOTHING;

INSERT INTO core.import_checkpoint (source_system, table_name, last_processed_timestamp)
VALUES ('coreac', 'work', '1970-01-01 00:00:00')
ON CONFLICT (source_system, table_name) DO NOTHING;

-- MIA
-- OPEN AIRE
-- CORDIS: INST and PROJECT