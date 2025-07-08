



CREATE OR REPLACE FUNCTION transform_cordis_to_core_with_junctions()
RETURNS INTEGER AS $$
DECLARE
    transformed_count INTEGER := 0;
    cordis_record RECORD;
    new_core_id INTEGER;
    journal_id INTEGER;
BEGIN
    -- Process each cordis entry that doesn't have a corresponding core entry yet
    FOR cordis_record IN
        SELECT c.*
        FROM cordis.researchoutput c
        LEFT JOIN core.researchoutput r ON r.source_system = 'cordis' AND r.source_id = c.id_original
        WHERE r.id IS NULL
        LIMIT 1000  -- Process in batches of 1000
    LOOP
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
    END LOOP;

    RETURN transformed_count;
END;
$$ LANGUAGE plpgsql;

----------

DO $$
DECLARE
    -- existing variables
    last_error TEXT;
BEGIN
    -- Add logging table if it doesn't exist
    CREATE TABLE IF NOT EXISTS batch_process_log (
        batch_num INTEGER,
        timestamp TIMESTAMP DEFAULT now(),
        records_processed INTEGER,
        error_message TEXT
    );

    LOOP
        -- Your existing loop code
        BEGIN
            newly_transformed := transform_cordis_to_core_with_junctions();

            -- Log success
            INSERT INTO batch_process_log (batch_num, records_processed)
            VALUES (batch_num, newly_transformed);

        EXCEPTION WHEN OTHERS THEN
            last_error := SQLERRM;

            -- Log error
            INSERT INTO batch_process_log (batch_num, records_processed, error_message)
            VALUES (batch_num, 0, last_error);

            RAISE NOTICE 'Batch % failed: %', batch_num, last_error;
        END;

        -- More aggressive maintenance
        COMMIT;
        PERFORM pg_catalog.pg_stat_reset();
        VACUUM ANALYZE;  -- More thorough VACUUM with statistics update

        -- Perhaps add a checkpoint
        CHECKPOINT;

        -- Exit when no more records or on error
        EXIT WHEN newly_transformed = 0 OR last_error IS NOT NULL;
    END LOOP;
END;
$$;