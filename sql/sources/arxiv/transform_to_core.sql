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

--- Insert arxiv.entry batch

    LOOP
        WITH batch_entries AS (
            -- entry data for this batch
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
            -- insert entries into core.researchoutput
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
            -- Not using the id's but keeping them
            RETURNING id, source_id
        )
        -- Update max_timestamp for this batch and num of batch entries
        SELECT COUNT(*), MAX(updated_at) FROM batch_entries INTO batch_count, max_timestamp;

        -- Finish if no more entries
        IF batch_count = 0 THEN
            EXIT;
        END IF;

--- Insert arxiv.authors and junctions

        WITH author_entries AS (
            SELECT
                e.id_original,
                a.name,
                j_ea.author_position
            FROM arxiv.entry e
            JOIN arxiv.j_entry_author j_ea ON e.id = j_ea.entry_id
            JOIN arxiv.author a ON j_ea.author_id = a.id
            WHERE e.updated_at > cp_timestamp
            -- Query Optimization
            AND e.updated_at <= max_timestamp
        ),
        inserted_authors AS (
            -- Insert author into core.person without duplicates
            INSERT INTO core.person (name)
            SELECT DISTINCT name
            FROM author_entries
            ON CONFLICT (name) DO NOTHING
            RETURNING id, name
        ),
        -- rename to inserted_author_junctions?
        -- i think i dont really understand the purpose of this subquery
        all_authors(
            SELECT id, name FROM inserted_authors
            -- union combines them?
            UNION ALL
            -- how come we use p.id form author_entries?
            SELECT p.id, p.name FROM author_entries ae
            -- joining on names and not id
            JOIN core.person p ON p.name = ae.name
            WHERE NOT EXISTS (
                SELECT 1 FROM inserted_authors ia WHERE ia.name = ae.name
            )
        )
        -- Insert core.ro core.person junction
        INSERT INTO core.j_researchoutput_person (
            researchoutput_id,
            person_id,
            role,
            position
        )
        SELECT
            ro.id,
            a.id,
            'author',
            ae.author_position,
        FROM author_entries ae
        JOIN core.researchoutput ro ON ro.source_system = 'arxiv' AND ro.source_id = ae.id_original
        JOIN all_authors a ON a.name = ae.name
        ON CONFLICT (researchoutput_id, person_id) DO NOTHING;











-- RUN THIS FOR ONE BATCH ONLY TO TEST IT
-- MAKE indexs for sources that speed this up the most
