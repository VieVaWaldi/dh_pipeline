-----------------------------------------------
-- Transform cordis to core

CREATE OR REPLACE FUNCTION transform_cordis_to_core_with_junctions()
RETURNS INTEGER AS $$
DECLARE
    transformed_count INTEGER := 0;
    cordis_record RECORD;
    new_core_id INTEGER;
    journal_id INTEGER;
    error_record TEXT;
BEGIN
    -- Process each cordis entry that doesn't have a corresponding core entry yet
    FOR cordis_record IN
        SELECT c.*
        FROM cordis.researchoutput c
        LEFT JOIN core.researchoutput r ON r.source_system = 'cordis' AND r.source_id = c.id_original
        WHERE r.id IS NULL
        LIMIT 1000  -- Process in batches of 1000
    LOOP
        BEGIN
            -- Insert the transformed record into core.researchoutput
            INSERT INTO core.researchoutput (
                source_id,
                source_system,
                doi,
                publication_date,
                updated_date,
                type,
                title,
                abstract,
                full_text,
                comment,
                funding_number
            ) VALUES (
                cordis_record.id_original,
                'cordis'::core.source_type,
                cordis_record.doi,
                cordis_record.publication_date,
                cordis_record.updated_at::DATE,
                cordis_record.type,
                cordis_record.title,
                cordis_record.summary,
                cordis_record.fulltext,
                cordis_record.comment,
                cordis_record.funding_number
            ) RETURNING id INTO new_core_id;

            -- Handle journal reference if it exists
            IF cordis_record.journal IS NOT NULL AND cordis_record.journal != '' THEN
                -- Insert or get the journal
                INSERT INTO core.journal (name)
                VALUES (cordis_record.journal)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id INTO journal_id;

                -- Create the junction between research output and journal
                INSERT INTO core.j_researchoutput_journal (researchoutput_id, journal_id)
                VALUES (new_core_id, journal_id);
            END IF;

            transformed_count := transformed_count + 1;
        EXCEPTION WHEN OTHERS THEN
            error_record := 'Error processing record ID: ' || cordis_record.id_original ||
                           ', Error: ' || SQLERRM ||
                           ', Detail: ' || SQLSTATE;
            RAISE NOTICE '%', error_record;
            -- Optionally log to a table for persistent error tracking
            -- INSERT INTO error_log (record_id, error_message, error_time) VALUES (cordis_record.id_original, SQLERRM, now());
            -- Continue with the next record
        END;
    END LOOP;

    RETURN transformed_count;
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------
-- Run Transform

DO $$
DECLARE
    total_transformed INTEGER := 0;
    newly_transformed INTEGER := 0;
    batch_num INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting cordis to core transformation with junctions...';

    LOOP
        batch_num := batch_num + 1;
        RAISE NOTICE 'Processing batch %...', batch_num;

        newly_transformed := transform_cordis_to_core_with_junctions();
        total_transformed := total_transformed + newly_transformed;

        RAISE NOTICE 'Transformed % records in batch %', newly_transformed, batch_num;

        -- Exit when no more records to transform
        EXIT WHEN newly_transformed = 0;

        -- COMMIT;
        PERFORM pg_catalog.pg_stat_reset();  -- Reset statistics
        -- VACUUM;  -- Release memory

    END LOOP;

    RAISE NOTICE 'Transformation complete. Total records transformed: %', total_transformed;
END;
$$;


-----------------------------------------------
-- Test Transform

-- SELECT transform_cordis_to_core_with_junctions();