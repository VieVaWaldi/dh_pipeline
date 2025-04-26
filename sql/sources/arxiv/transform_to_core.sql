-------------------------------------------------------------------
--- Arxiv Transformation to Core Script

CREATE TEMP TABLE arxiv_batch_tracker (
    cp_timestamp TIMESTAMP, -- checkpoint
    batch_size INTEGER DEFAULT 1000,
    processed INTEGER DEFAULT 0
);

INSERT INTO arxiv_batch_tracker (cp_timestamp)
SELECT COALESCE(
    (SELECT last_processed_timestamp FROM core.import_checkpoint
     WHERE source_system = 'arxiv' AND table_name = 'entry'),
    '1970-01-01 00:00:00'
);

CREATE OR REPLACE FUNCTION process_arxiv_updates() RETURNS void AS $$
DECLARE
    max_timestamp TIMESTAMP;
    cp_timestamp TIMESTAMP;
    batch_count INTEGER;
BEGIN
    SELECT abt.cp_timestamp INTO cp_timestamp
    FROM arxiv_batch_tracker abt;

    LOOP
        WITH batch_entries AS (
        -- entry data for batch
            SELECT
                e.id,
                e.id_original,
                e.title,
                e.doi,
                e.summary,
                e.full_text,
                -- e.journal_ref,
                 e.comment,
                -- e.primary_category, -- not in entry
                -- e.category_term,
                -- e.categories,
                e.published_date,
                e.updated_date,
                e.updated_at
            FROM arxiv.entry e
            WHERE e.updated_at > cp_timestamp
            ORDER BY e.updated_at
            LIMIT (SELECT batch_size FROM arxiv_batch_tracker)
        ),
        inserted_outputs AS (
        -- how to insert into core.researchoutput
            INSERT INTO core.researchoutput (
                source_id,
                source_system,
                doi,
                arxiv_id,
                publication_date,
                updated_date,
                -- language_code,
                -- type,
                title,
                abstract,
                full_text,
                comment,
            )
            SELECT
                id_original,
                'arxiv'::core.source_type
                doi,
                id_original,
                published_date,
                updated_date,
                title,
                summary,
                full_text,
                comment
            FROM batch_entries
            ON CONFLICT (source_system, source_id) DO UPDATE
            SET
                -- Update only these fields on conflict
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                full_text = EXCLUDED.full_text,
                updated_date = EXCLUDED.updated_date,
                updated_at = NOW()
            -- Not needed now but could be useful later
            RETURNING id, source_id
        )
        -- save last updated_at as timestamp -- wouldnt work if an earlier batch had a higher timestamp right?
        SELECT COUNT(*), MAX(updated_at) FROM batch_entries INTO batch_count, max_timestamp;



















-- RUN THIS FOR ONE BATCH ONLY TO TEST IT
-- MAKE indexs for sources that speed this up the most