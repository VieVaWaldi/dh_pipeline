-----------------------------------------------
-- Transform arxiv to core

CREATE OR REPLACE FUNCTION transform_arxiv_to_core_with_junctions()
RETURNS void AS $$
DECLARE
    batch_size INTEGER := 1000;
    total_count INTEGER;
    batch_count INTEGER;
    entry_id INTEGER;
    entry_rec RECORD;
    author_id INTEGER;
    author_rec RECORD;
    link_id INTEGER;
    link_rec RECORD;
    topic_id INTEGER;
    journal_id INTEGER;
    new_core_id INTEGER;
    already_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting arxiv to core transformation with junctions...';

    -- Get total count and calculate batch count
    SELECT COUNT(*) INTO total_count FROM arxiv.entry;
    batch_count := CEIL(total_count::FLOAT / batch_size);

    -- Process in batches
    FOR batch_num IN 1..batch_count LOOP
        RAISE NOTICE 'Processing batch %...', batch_num;

        -- Fetch entries in current batch
        FOR entry_rec IN
            SELECT e.*
            FROM arxiv.entry e
            ORDER BY e.id
            LIMIT batch_size
            OFFSET (batch_num - 1) * batch_size
        LOOP
            -- Check if this entry has already been transformed
            SELECT EXISTS (
                SELECT 1
                FROM core.researchoutput
                WHERE source_system = 'arxiv'
                AND source_id = entry_rec.id_original
            ) INTO already_exists;

            IF NOT already_exists THEN
                -- Transform entry to core.researchoutput
                INSERT INTO core.researchoutput (
                    source_id,
                    source_system,
                    doi,
                    arxiv_id,
                    publication_date,
                    updated_date,
                    title,
                    abstract,
                    full_text,
                    comment
                ) VALUES (
                    entry_rec.id_original,
                    'arxiv',
                    entry_rec.doi,
                    entry_rec.id_original,
                    entry_rec.published_date::DATE,
                    entry_rec.updated_date::DATE,
                    entry_rec.title,
                    entry_rec.summary,
                    entry_rec.full_text,
                    entry_rec.comment
                ) RETURNING id INTO new_core_id;

                -- Process authors
                FOR author_rec IN
                    SELECT a.*
                    FROM arxiv.author a
                    JOIN arxiv.j_entry_author ja ON a.id = ja.author_id
                    WHERE ja.entry_id = entry_rec.id
                LOOP
                    -- Get or create person in core
                    SELECT id INTO author_id FROM core.person WHERE name = author_rec.name;

                    IF author_id IS NULL THEN
                        INSERT INTO core.person (name) VALUES (author_rec.name) RETURNING id INTO author_id;
                    END IF;

                    -- Create junction
                    INSERT INTO core.j_researchoutput_person (
                        researchoutput_id,
                        person_id,
                        role,
                        position
                    ) VALUES (
                        new_core_id,
                        author_id,
                        'author',
                        (SELECT author_position FROM arxiv.j_entry_author
                         WHERE entry_id = entry_rec.id AND author_id = author_rec.id)
                    );
                END LOOP;

                -- Process links
                FOR link_rec IN
                    SELECT l.*
                    FROM arxiv.link l
                    JOIN arxiv.j_entry_link jl ON l.id = jl.link_id
                    WHERE jl.entry_id = entry_rec.id
                LOOP
                    -- Get or create link in core
                    SELECT id INTO link_id FROM core.link WHERE url = link_rec.href;

                    IF link_id IS NULL THEN
                        INSERT INTO core.link (url, type)
                        VALUES (link_rec.href, link_rec.type)
                        RETURNING id INTO link_id;
                    END IF;

                    -- Check if junction already exists before creating it
                    SELECT EXISTS (
                        SELECT 1
                        FROM core.j_researchoutput_link
                        WHERE researchoutput_id = new_core_id
                        AND link_id = link_id
                    ) INTO already_exists;

                    IF NOT already_exists THEN
                        -- Create junction if it doesn't exist
                        INSERT INTO core.j_researchoutput_link (researchoutput_id, link_id)
                        VALUES (new_core_id, link_id);
                    END IF;
                END LOOP;

                -- Process topics (from primary_category and categories)
                IF entry_rec.primary_category IS NOT NULL THEN
                    -- Get or create topic
                    SELECT id INTO topic_id FROM core.topic
                    WHERE source_system = 'arxiv' AND name = entry_rec.primary_category;

                    IF topic_id IS NULL THEN
                        INSERT INTO core.topic (source_system, name)
                        VALUES ('arxiv', entry_rec.primary_category)
                        RETURNING id INTO topic_id;
                    END IF;

                    -- Create junction
                    INSERT INTO core.j_researchoutput_topic (researchoutput_id, topic_id)
                    VALUES (new_core_id, topic_id);
                END IF;

                -- Process other categories
                IF entry_rec.categories IS NOT NULL THEN
                    FOREACH topic_id IN ARRAY entry_rec.categories
                    LOOP
                        -- Get or create topic
                        SELECT id INTO topic_id FROM core.topic
                        WHERE source_system = 'arxiv' AND name = topic_id;

                        IF topic_id IS NULL THEN
                            INSERT INTO core.topic (source_system, name)
                            VALUES ('arxiv', topic_id)
                            RETURNING id INTO topic_id;
                        END IF;

                        -- Check if junction already exists
                        SELECT EXISTS (
                            SELECT 1
                            FROM core.j_researchoutput_topic
                            WHERE researchoutput_id = new_core_id
                            AND topic_id = topic_id
                        ) INTO already_exists;

                        IF NOT already_exists THEN
                            -- Create junction if it doesn't exist
                            INSERT INTO core.j_researchoutput_topic (researchoutput_id, topic_id)
                            VALUES (new_core_id, topic_id);
                        END IF;
                    END LOOP;
                END IF;

                -- Process journal reference
                IF entry_rec.journal_ref IS NOT NULL THEN
                    -- Get or create journal
                    SELECT id INTO journal_id FROM core.journal WHERE name = entry_rec.journal_ref;

                    IF journal_id IS NULL THEN
                        INSERT INTO core.journal (name)
                        VALUES (entry_rec.journal_ref)
                        RETURNING id INTO journal_id;
                    END IF;

                    -- Create junction
                    INSERT INTO core.j_researchoutput_journal (researchoutput_id, journal_id)
                    VALUES (new_core_id, journal_id);
                END IF;
            END IF;
        END LOOP;

        RAISE NOTICE 'Transformed % records in batch %',
            LEAST(batch_size, total_count - (batch_num - 1) * batch_size), batch_num;
    END LOOP;

    RAISE NOTICE 'Arxiv to core transformation completed successfully';
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
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'Transformation complete. Total records transformed: %', total_transformed;
END;
$$;

-----------------------------------------------
-- Test Transform

-- SELECT transform_arxiv_to_core_with_junctions();