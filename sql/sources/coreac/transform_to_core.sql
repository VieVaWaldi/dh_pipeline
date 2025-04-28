-----------------------------------------------
-- Transform coreac to core

CREATE OR REPLACE FUNCTION transform_coreac_to_core_with_junctions()
RETURNS INTEGER AS $$
DECLARE
    transformed_count INTEGER := 0;
    coreac_record RECORD;
    new_core_id INTEGER;
    journal_id INTEGER;
    person_id INTEGER;
    link_id INTEGER;
    author_name TEXT;
    contributor_name TEXT;
    journal_title TEXT;
BEGIN
    -- Process each CoreAC entry that doesn't have a corresponding core entry yet
    FOR coreac_record IN 
        SELECT c.* 
        FROM coreac.work c
        LEFT JOIN core.researchoutput r ON r.source_system = 'coreac' AND r.source_id = c.id_original
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
            language_code,
            type,
            title,
            abstract,
            full_text
        ) VALUES (
            coreac_record.id_original,
            'coreac'::core.source_type,
            coreac_record.doi,
            coreac_record.arxiv_id,
            coreac_record.published_date::DATE,
            coreac_record.updated_date::DATE,
            coreac_record.language_code,
            COALESCE(coreac_record.document_type, 'publication'),
            coreac_record.title,
            coreac_record.abstract,
            coreac_record.fulltext
        ) RETURNING id INTO new_core_id;
        
        -- Handle journals if they exist
        IF coreac_record.journals_title IS NOT NULL THEN
            FOREACH journal_title IN ARRAY coreac_record.journals_title
            LOOP
                -- Skip empty journal titles
                IF journal_title IS NOT NULL AND journal_title != '' THEN
                    -- Insert or get the journal
                    INSERT INTO core.journal (name)
                    VALUES (journal_title)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id INTO journal_id;
                    
                    -- Create the junction between research output and journal
                    INSERT INTO core.j_researchoutput_journal (researchoutput_id, journal_id)
                    VALUES (new_core_id, journal_id)
                    ON CONFLICT DO NOTHING;
                END IF;
            END LOOP;
        END IF;
        
        -- Handle authors if they exist
        IF coreac_record.authors IS NOT NULL THEN
            DECLARE
                position_counter INTEGER := 1;
            BEGIN
                FOREACH author_name IN ARRAY coreac_record.authors
                LOOP
                    -- Skip empty author names
                    IF author_name IS NOT NULL AND author_name != '' THEN
                        -- Insert or get the person
                        INSERT INTO core.person (name)
                        VALUES (author_name)
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
                            position_counter
                        )
                        ON CONFLICT DO NOTHING;
                        
                        position_counter := position_counter + 1;
                    END IF;
                END LOOP;
            END;
        END IF;
        
        -- Handle contributors if they exist
        IF coreac_record.contributors IS NOT NULL THEN
            DECLARE
                position_counter INTEGER := 1;
            BEGIN
                FOREACH contributor_name IN ARRAY coreac_record.contributors
                LOOP
                    -- Skip empty contributor names
                    IF contributor_name IS NOT NULL AND contributor_name != '' THEN
                        -- Insert or get the person
                        INSERT INTO core.person (name)
                        VALUES (contributor_name)
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
                            'contributor', 
                            position_counter
                        )
                        ON CONFLICT DO NOTHING;
                        
                        position_counter := position_counter + 1;
                    END IF;
                END LOOP;
            END;
        END IF;
        
        -- Handle download URL if it exists
        IF coreac_record.download_url IS NOT NULL AND coreac_record.download_url != '' THEN
            -- Insert or get the link
            INSERT INTO core.link (url, type, description)
            VALUES (coreac_record.download_url, 'download', 'Download URL from CoreAC')
            ON CONFLICT (url) DO UPDATE SET type = 'download'
            RETURNING id INTO link_id;
            
            -- Create the junction between research output and link
            INSERT INTO core.j_researchoutput_link (researchoutput_id, link_id)
            VALUES (new_core_id, link_id)
            ON CONFLICT DO NOTHING;
        END IF;
        
        -- Handle additional links from coreac.link if they exist
        DECLARE
            link_record RECORD;
        BEGIN
            FOR link_record IN 
                SELECT l.* 
                FROM coreac.link l
                JOIN coreac.j_work_link j ON l.id = j.link_id
                WHERE j.work_id = coreac_record.id
            LOOP
                -- Insert or get the link
                INSERT INTO core.link (url, type)
                VALUES (link_record.url, link_record.type)
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
    RAISE NOTICE 'Starting CoreAC to core transformation with junctions...';
    
    LOOP
        batch_num := batch_num + 1;
        RAISE NOTICE 'Processing batch %...', batch_num;
        
        newly_transformed := transform_coreac_to_core_with_junctions();
        total_transformed := total_transformed + newly_transformed;
        
        RAISE NOTICE 'Transformed % records in batch %', newly_transformed, batch_num;
        
        -- Exit when no more records to transform
        EXIT WHEN newly_transformed = 0;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'Transformation complete. Total records transformed: %', total_transformed;
END;
$$;

-----------------------------------------------
-- Test Transform

-- SELECT transform_coreac_to_core_with_junctions();