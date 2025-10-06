------------------------------
----  OpenAIRE Data Exploration for Deduplication
------------------------------

------------------------------
----  Research Outputs
------------------------------

-- Check if there's a DOI field (might be in a different location)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'openaire'
  AND table_name = 'researchoutput'
  AND column_name ILIKE '%doi%'
ORDER BY ordinal_position;

-- Forgot to include doi, cant use it.

------------------------------
-- Title Duplicates (main_title)
SELECT LOWER(TRIM(main_title)) as clean_title, COUNT(*) as duplicate_count
FROM openaire.researchoutput
WHERE main_title IS NOT NULL
GROUP BY LOWER(TRIM(main_title))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 4500 duplicates, but lots of generic names
-- Typd doesnt help here

------------------------------
-- Check id_original for potential DOI or ArXiv patterns
SELECT 'Sample id_original patterns' as analysis_type;
SELECT
    id_original,
    CASE
        WHEN id_original ILIKE '10.%' THEN 'potential_doi'
        WHEN id_original ILIKE 'arxiv:%' THEN 'potential_arxiv'
        WHEN id_original ILIKE 'http%' THEN 'potential_url'
        ELSE 'other'
    END as id_type,
    COUNT(*) as count
FROM openaire.researchoutput
GROUP BY id_original,
    CASE
        WHEN id_original ILIKE '10.%' THEN 'potential_doi'
        WHEN id_original ILIKE 'arxiv:%' THEN 'potential_arxiv'
        WHEN id_original ILIKE 'http%' THEN 'potential_url'
        ELSE 'other'
    END
ORDER BY count DESC;
-- all count 1

------------------------------
-- Research Output Type Distribution
SELECT type, COUNT(*) as count
FROM openaire.researchoutput
GROUP BY type
ORDER BY count DESC;
-- "publication"	125408
-- "dataset"	8237
-- "other"	5492
-- "software"	1215


------------------------------
----  Projects
------------------------------

------------------------------
-- Project DOI Duplicates
SELECT doi, COUNT(*) as duplicate_count
FROM openaire.project
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 0
ORDER BY duplicate_count DESC;

-- None

------------------------------
-- Project Code Duplicates (should be unique identifiers)
SELECT code, COUNT(*) as duplicate_count
FROM openaire.project
WHERE code IS NOT NULL
GROUP BY code
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 23 rows with count 2, one with count 3

------------------------------
-- Project id_openaire Duplicates (should be unique)
SELECT id_openaire, COUNT(*) as duplicate_count
FROM openaire.project
WHERE id_openaire IS NOT NULL
GROUP BY id_openaire
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- None

------------------------------
-- Project id_original Duplicates
SELECT id_original, COUNT(*) as duplicate_count
FROM openaire.project
WHERE id_original IS NOT NULL
GROUP BY id_original
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- None

------------------------------
-- Project Title Duplicates
SELECT LOWER(TRIM(title)) as clean_title, COUNT(*) as duplicate_count
FROM openaire.project
WHERE title IS NOT NULL
GROUP BY LOWER(TRIM(title))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- >100 projects with unititled name. 818 rows in total

------------------------------
-- Project Acronym Duplicates
SELECT UPPER(TRIM(acronym)) as clean_acronym, COUNT(*) as duplicate_count
FROM openaire.project
WHERE acronym IS NOT NULL
GROUP BY UPPER(TRIM(acronym))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- DO not use!

------------------------------
-- Combined Project Code + Title potential duplicates
SELECT
    code,
    LOWER(TRIM(title)) as clean_title,
    COUNT(*) as duplicate_count,
    ARRAY_AGG(id ORDER BY id) as project_ids
FROM openaire.project
WHERE code IS NOT NULL AND title IS NOT NULL
GROUP BY code, LOWER(TRIM(title))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- None

------------------------------
----  Organizations
------------------------------

------------------------------
-- Organization original_id Duplicates
SELECT original_id, COUNT(*) as duplicate_count
FROM openaire.organization
WHERE original_id IS NOT NULL
GROUP BY original_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- None

------------------------------
-- Organization Legal Name Duplicates (exact)
SELECT legal_name, COUNT(*) as duplicate_count
FROM openaire.organization
WHERE legal_name IS NOT NULL
GROUP BY legal_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- No 

------------------------------
-- Organization Legal Name Duplicates (case-insensitive)
SELECT LOWER(TRIM(legal_name)) as clean_legal_name, COUNT(*) as duplicate_count
FROM openaire.organization
WHERE legal_name IS NOT NULL
GROUP BY LOWER(TRIM(legal_name))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 1571 rows with count 2 and 3

------------------------------
-- Organization Website URL Duplicates
SELECT website_url, COUNT(*) as duplicate_count
FROM openaire.organization
WHERE website_url IS NOT NULL
GROUP BY website_url
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 177 rows with up to 6 counts

select * from openaire.organization
where website_url = 'http://www.technopolis-group.com'; --  'http://www.philips.com', 'http://www.nokia.com', 'http://www.thalesgroup.com', 'http://www.technopolis-group.com'

------------------------------
-- Organization Short Name Duplicates
SELECT UPPER(TRIM(legal_short_name)) as clean_short_name, COUNT(*) as duplicate_count
FROM openaire.organization
WHERE legal_short_name IS NOT NULL
GROUP BY UPPER(TRIM(legal_short_name))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Too short could mean anything

------------------------------
-- Combined Organization Name + Country potential duplicates
SELECT
    LOWER(TRIM(legal_name)) as clean_legal_name,
    country_code,
    COUNT(*) as duplicate_count,
    ARRAY_AGG(id ORDER BY id) as organization_ids
FROM openaire.organization
WHERE legal_name IS NOT NULL
GROUP BY LOWER(TRIM(legal_name)), country_code
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- just picks null and UNKNOWN for countries

------------------------------
-- Organization geolocation analysis (to see if useful for deduplication)
SELECT
    COUNT(*) as total_orgs,
    COUNT(geolocation) as orgs_with_geolocation,
    ROUND(COUNT(geolocation) * 100.0 / COUNT(*), 2) as geolocation_coverage_pct
FROM openaire.organization;