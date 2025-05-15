-----------------------------------------------
-- Transform arxiv to core

CREATE OR REPLACE FUNCTION transform_arxiv_to_core_with_junctions()
RETURNS INTEGER AS $$
DECLARE
    transformed_count INTEGER := 0;
    arxiv_record RECORD;
    new_core_id INTEGER;
    journal_id INTEGER;
    topic_id INTEGER;
    person_id INTEGER;
    link_id INTEGER;
    category TEXT;
BEGIN
    -- Process each arxiv entry that doesn't have a corresponding core entry yet
    FOR arxiv_record IN
        SELECT a.*
        FROM arxiv.entry a
        LEFT JOIN core.researchoutput r ON r.source_system = 'arxiv' AND r.source_id = a.id_original
        WHERE r.id IS NULL
        LIMIT 1000  -- Process in batches of 1000
    LOOP
        -- Insert the transformed record into core.researchoutput
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
            comment
        ) VALUES (
            arxiv_record.id_original,
            'arxiv'::core.source_type,
            arxiv_record.doi,
            arxiv_record.id_original,
            arxiv_record.published_date::DATE,
            arxiv_record.updated_date::DATE,
            -- 'en',  -- Assuming English as default language
            -- 'publication',  -- Standard type for arxiv entries
            arxiv_record.title,
            arxiv_record.summary,
            arxiv_record.full_text,
            arxiv_record.comment
        ) RETURNING id INTO new_core_id;

        -- Handle journal reference if it exists
        IF arxiv_record.journal_ref IS NOT NULL AND arxiv_record.journal_ref != '' THEN
            -- Insert or get the journal
            INSERT INTO core.journal (name)
            VALUES (arxiv_record.journal_ref)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id INTO journal_id;

            -- Create the junction between research output and journal
            INSERT INTO core.j_researchoutput_journal (researchoutput_id, journal_id)
            VALUES (new_core_id, journal_id);
        END IF;

        -- Handle primary category as a topic
        IF arxiv_record.primary_category IS NOT NULL AND arxiv_record.primary_category != '' THEN
            -- Insert or get the topic
            INSERT INTO core.topic (source_system, name)
            VALUES ('arxiv'::core.source_type, arxiv_record.primary_category)
            ON CONFLICT (source_system, name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id INTO topic_id;

            -- Create the junction between research output and topic
            INSERT INTO core.j_researchoutput_topic (researchoutput_id, topic_id)
            VALUES (new_core_id, topic_id);
        END IF;

        -- Handle all categories as topics
        IF arxiv_record.categories IS NOT NULL THEN
            FOREACH category IN ARRAY arxiv_record.categories
            LOOP
                -- Skip empty categories
                IF category IS NOT NULL AND category != '' THEN
                    -- Insert or get the topic
                    INSERT INTO core.topic (source_system, name)
                    VALUES ('arxiv'::core.source_type, category)
                    ON CONFLICT (source_system, name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id INTO topic_id;

                    -- Create the junction between research output and topic
                    INSERT INTO core.j_researchoutput_topic (researchoutput_id, topic_id)
                    VALUES (new_core_id, topic_id)
                    ON CONFLICT DO NOTHING;  -- In case primary_category was already added
                END IF;
            END LOOP;
        END IF;

        -- Handle authors
        DECLARE
            person_record RECORD;
        BEGIN
            FOR person_record IN
                SELECT a.*, j.author_position
                FROM arxiv.author a
                JOIN arxiv.j_entry_author j ON a.id = j.author_id
                WHERE j.entry_id = arxiv_record.id
                ORDER BY j.author_position
            LOOP
            -- Insert or get the person
            INSERT INTO core.person (name)
            VALUES (person_record.name)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id INTO person_id;

            -- Create the junction between research output and person
            INSERT INTO core.j_researchoutput_person (
                researchoutput_id,
                person_id,
                role,
                position
            )
            VALUES (
                new_core_id,
                person_id,
                'author',
                person_record.author_position
            );
        END LOOP;
        END;

        -- Handle links
        DECLARE
            link_record RECORD;
        BEGIN
            FOR link_record IN
                SELECT l.*
                FROM arxiv.link l
                JOIN arxiv.j_entry_link j ON l.id = j.link_id
                WHERE j.entry_id = arxiv_record.id
            LOOP
            -- Insert or get the link
            INSERT INTO core.link (url, type)
            VALUES (link_record.href, link_record.type)
            ON CONFLICT (url) DO UPDATE SET type = EXCLUDED.type
            RETURNING id INTO link_id;

            -- Create the junction between research output and link
            INSERT INTO core.j_researchoutput_link (researchoutput_id, link_id)
            VALUES (new_core_id, link_id)
            ON CONFLICT DO NOTHING;
        END LOOP;
        END;

        transformed_count := transformed_count + 1;
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
    RAISE NOTICE 'Starting arxiv to core transformation with junctions...';
    
    LOOP
        batch_num := batch_num + 1;
        RAISE NOTICE 'Processing batch %...', batch_num;
        
        newly_transformed := transform_arxiv_to_core_with_junctions();
        total_transformed := total_transformed + newly_transformed;
        
        RAISE NOTICE 'Transformed % records in batch %', newly_transformed, batch_num;
        
        -- Exit when no more records to transform
        EXIT WHEN newly_transformed = 0;
        
        -- Optional: Add a commit here for large datasets
        -- COMMIT;
    END LOOP;
    
    RAISE NOTICE 'Transformation complete. Total records transformed: %', total_transformed;
END;
$$;

-----------------------------------------------
-- Test Transform

-- SELECT transform_arxiv_to_core_with_junctions();