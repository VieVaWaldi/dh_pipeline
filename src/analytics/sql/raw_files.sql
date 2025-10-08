SELECT * from analytics.raw_files;



-- CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.raw_files (
    id SERIAL PRIMARY KEY,
    analysis_date TIMESTAMP NOT NULL DEFAULT NOW(),
    source_query_id TEXT NOT NULL,
    total_disk_usage_gb DOUBLE PRECISION NOT NULL,
    file_types_total JSONB NOT NULL,
    checkpoints JSONB NOT NULL,
    UNIQUE(analysis_date, source_query_id)
);