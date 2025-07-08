-------------------------------------------------------------------
-- RO: Full text search test
-- If you use multiple columns best practice and fastest ist to create a combined index

-- DO NOT FORGET often you only need to filter RO that has an I

-----------------------------
-- https://www.crunchydata.com/blog/postgres-full-text-search-a-search-engine-in-a-database
-- We can store the ts_vector as a precomputed column
-- Then create a GIN index
-- Query with ts @@ to_tsquery

-- without index - cultur AND heritage:
-- title 2 ms
-- abstract 2s
-- full_text > 5m , stopped trying
SELECT id, left(full_text, 200) FROM core.researchoutput
WHERE full_text ilike '%cultur%' AND
full_text ilike '%heritage%';

ALTER TABLE core.researchoutput ADD COLUMN ts_full_text tsvector
    GENERATED ALWAYS AS (to_tsvector('english', left(full_text, 200000))) STORED;

select * from cordis.project
limit 10;


-----------------------------


-- For title + abstract searches (fast, smaller index)
SELECT id, title, LEFT(abstract, 200) as preview, publication_date,
    ts_rank(
        to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(abstract, '')),
        to_tsquery('english', 'cultural | heritage')
    ) as score
FROM core.researchoutput
WHERE to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(abstract, '')) 
    @@ to_tsquery('english', 'cultural | heritage')
LIMIT 50;

-- Index: Took about 4 minutes to create with old hardware settings
CREATE INDEX idx_ro_title_abstract_search ON core.researchoutput 
USING gin(to_tsvector('english', 
    COALESCE(title, '') || ' ' || COALESCE(abstract, '')
));

-----------------------------
-- For comprehensive searches including full text (slower to build, larger, but most complete)
SELECT id, title, LEFT(abstract, 200) as preview, publication_date
WHERE to_tsvector('english', 
    COALESCE(title, '') || ' ' || 
    COALESCE(abstract, '') || ' ' || 
    LEFT(COALESCE(full_text, ''), 800000)  -- Must match index exactly
) @@ to_tsquery('english', 'cultural & heritage');

-- Index: Took about --- to create with new hardware settings
CREATE INDEX idx_ro_full_content_search ON core.researchoutput 
USING gin(to_tsvector('english', 
    COALESCE(title, '') || ' ' || 
    COALESCE(abstract, '') || ' ' || 
    LEFT(COALESCE(full_text, ''), 800000)  -- Only first 800KB indexed for max 1MB tsvector
));

-- INDEX RELATED ERROR: some full texts too big for ts vector
SELECT MAX(LENGTH(full_text)), AVG(LENGTH(full_text)) 
FROM core.researchoutput 
WHERE full_text IS NOT NULL;
-- Avg is 212kB, while max is 24 MB, while tsvector allows for max 1MB

SELECT id, title, LENGTH(full_text)
FROM core.researchoutput 
WHERE full_text IS NOT NULL
ORDER BY LENGTH(full_text) DESC
limit 200;

-----------------------------
-- Check these settings to utilize hardware better:
-- Check current settings
SHOW shared_buffers; -- 128MB
SHOW effective_cache_size; -- 4gb
SHOW work_mem; -- 4 MB
SHOW maintenance_work_mem; -- 64 MB

-- Recommended for my setup:
-- In postgresql.conf
shared_buffers = 8GB          -- Or more with your RAM
effective_cache_size = 100GB   -- Estimate of OS cache + shared_buffers
work_mem = 256MB              -- For sorting/ranking operations
maintenance_work_mem = 2GB    -- For index creation

-- Temporary Session Settings! Change conf
SET shared_buffers = '8GB';
SET effective_cache_size = '200GB';
SET work_mem = '512MB';
SET maintenance_work_mem = '8GB';