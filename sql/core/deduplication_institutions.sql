-----------------------------------------------
-- Deduplication Institutions

select count (*) from cordis.institution;
-- 32518
select count (*) from openaire.organization;
-- 17470

select count (*) from openaire.organization
where geolocation is null;
-- cordis 6875 without geolocation, 6647 got geolocation from mapbox, 
-- some others were fixed, eg dresden university in hamburg, still some errors 
-- which have to be iteratively fixed

-----------------------------------------------
-- Institution Deduplication Between Cordis and OpenAIRE

-----------------------------------------------
-- Exact Legal Name Matching

-- Find duplicates with exact legal name matches
WITH unified_institutions AS (
    SELECT id, legal_name, short_name, 'cordis' AS source_system, country AS country_cordis, NULL AS country_code_openaire, NULL AS original_id, url AS website_url
    FROM cordis.institution
    UNION ALL
    SELECT id, legal_name, legal_short_name AS short_name, 'openaire' AS source_system, country_label AS country_cordis, country_code AS country_code_openaire, original_id, website_url
    FROM openaire.organization
)
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.legal_name, a.source_system AS source1, b.source_system AS source2,
       a.country_cordis AS country1, b.country_cordis AS country2,
       a.short_name AS short_name1, b.short_name AS short_name2,
       a.website_url AS website1, b.website_url AS website2
FROM unified_institutions a
JOIN unified_institutions b ON 
    LOWER(a.legal_name) = LOWER(b.legal_name) AND 
    a.id < b.id
;

-- 4263 duplicates

-----------------------------------------------
-- Fuzzy Legal Name Matching

DROP MATERIALIZED VIEW IF EXISTS institution_dedupe_candidates;

-- Materialized view with normalized legal names and name length for efficiency  
CREATE MATERIALIZED VIEW institution_dedupe_candidates AS
SELECT 
    id, 
    source_system,
    legal_name,
    LOWER(regexp_replace(legal_name, '[[:punct:][:space:]]+', '', 'g')) AS normalized_name,
    LENGTH(LOWER(regexp_replace(legal_name, '[[:punct:][:space:]]+', '', 'g'))) AS name_length,
    short_name,
    LOWER(regexp_replace(short_name, '[[:punct:][:space:]]+', '', 'g')) AS normalized_short_name,
    country,
    website_url,
    original_id,
    coordinates
FROM (
    SELECT id, legal_name, short_name, 'cordis' AS source_system, country, url AS website_url, NULL::TEXT AS original_id,
           -- Convert geolocation array to point for Cordis
           CASE WHEN geolocation IS NOT NULL THEN POINT(geolocation[1], geolocation[2]) ELSE NULL END AS coordinates
    FROM cordis.institution
    UNION ALL  
    SELECT id, legal_name, legal_short_name AS short_name, 'openaire' AS source_system, country_label AS country, website_url, original_id,
           -- Convert geolocation array to point for OpenAIRE
           CASE WHEN geolocation IS NOT NULL THEN POINT(geolocation[1], geolocation[2]) ELSE NULL END AS coordinates
    FROM openaire.organization
) combined_institutions;

-- Indexes for performance
CREATE INDEX idx_institution_dedupe_norm_name ON institution_dedupe_candidates(normalized_name);
CREATE INDEX idx_institution_dedupe_name_length ON institution_dedupe_candidates(name_length);
CREATE INDEX idx_institution_dedupe_source ON institution_dedupe_candidates(source_system);
CREATE INDEX idx_institution_dedupe_short_name ON institution_dedupe_candidates(normalized_short_name) WHERE normalized_short_name IS NOT NULL;
CREATE INDEX idx_institution_dedupe_country ON institution_dedupe_candidates(country);
CREATE INDEX idx_institution_dedupe_website ON institution_dedupe_candidates(website_url) WHERE website_url IS NOT NULL;

-- Query with fuzzy matching and additional constraints
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.legal_name AS name1, b.legal_name AS name2,
       a.source_system AS source1, b.source_system AS source2,
       a.original_id AS original_id1, b.original_id AS original_id2,
       a.short_name AS short_name1, b.short_name AS short_name2,
       a.country AS country1, b.country AS country2,
       a.website_url AS website1, b.website_url AS website2,
       similarity(a.normalized_name, b.normalized_name) AS sim_score,
       -- Calculate distance if both have coordinates
       CASE 
           WHEN a.coordinates IS NOT NULL AND b.coordinates IS NOT NULL 
           THEN point_distance(a.coordinates, b.coordinates)
           ELSE NULL 
       END AS geo_distance
FROM institution_dedupe_candidates a
JOIN institution_dedupe_candidates b ON 
    a.id < b.id AND
    -- Different sources only  
    a.source_system != b.source_system AND
    -- Length constraint: only compare names with similar lengths
    ABS(a.name_length - b.name_length) <= 10 AND
    -- Similarity threshold for legal names
    similarity(a.normalized_name, b.normalized_name) > 0.9
WHERE
    -- Optional: Country matching when available
    (a.country IS NULL OR b.country IS NULL OR 
     LOWER(a.country) = LOWER(b.country) OR 
     similarity(LOWER(a.country), LOWER(b.country)) > 0.8)  -- Fuzzy country match too
    AND
    -- Optional: Short name matching if both exist
    (a.normalized_short_name IS NULL OR b.normalized_short_name IS NULL OR 
     similarity(a.normalized_short_name, b.normalized_short_name) > 0.8)
ORDER BY sim_score DESC, geo_distance ASC NULLS LAST;

-----------------------------------------------
-- Website Domain Matching
-- Additional query to find institutions with same website domain

SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.legal_name AS name1, b.legal_name AS name2,
       a.source_system AS source1, b.source_system AS source2,
       a.website_url AS website1, b.website_url AS website2,
       similarity(a.normalized_name, b.normalized_name) AS sim_score
FROM institution_dedupe_candidates a
JOIN institution_dedupe_candidates b ON 
    a.id < b.id AND
    a.source_system != b.source_system AND
    a.website_url IS NOT NULL AND
    b.website_url IS NOT NULL AND
    -- Extract domain from URL and compare
    regexp_replace(a.website_url, '^https?://([^/]+).*', '\1') = 
    regexp_replace(b.website_url, '^https?://([^/]+).*', '\1')
ORDER BY sim_score DESC;

-----------------------------------------------
-- Analysis Query to Check Deduplication Results

-- Count exact legal name matches
WITH exact_matches AS (
    SELECT COUNT(*) AS exact_count
    FROM (
        SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2
        FROM institution_dedupe_candidates a
        JOIN institution_dedupe_candidates b ON 
            LOWER(a.legal_name) = LOWER(b.legal_name) AND 
            a.id < b.id AND
            a.source_system != b.source_system
    ) exact
),
-- Count fuzzy matches
fuzzy_matches AS (
    SELECT COUNT(*) AS fuzzy_count  
    FROM (
        SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2
        FROM institution_dedupe_candidates a
        JOIN institution_dedupe_candidates b ON 
            a.id < b.id AND
            a.source_system != b.source_system AND
            ABS(a.name_length - b.name_length) <= 10 AND
            similarity(a.normalized_name, b.normalized_name) > 0.9
    ) fuzzy
),
-- Count website domain matches
website_matches AS (
    SELECT COUNT(*) AS website_count
    FROM (
        SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2
        FROM institution_dedupe_candidates a
        JOIN institution_dedupe_candidates b ON 
            a.id < b.id AND
            a.source_system != b.source_system AND
            a.website_url IS NOT NULL AND
            b.website_url IS NOT NULL AND
            regexp_replace(a.website_url, '^https?://([^/]+).*', '\1') = 
            regexp_replace(b.website_url, '^https?://([^/]+).*', '\1')
    ) website
)
SELECT 
    (SELECT exact_count FROM exact_matches) AS exact_legal_name_matches,
    (SELECT fuzzy_count FROM fuzzy_matches) AS fuzzy_legal_name_matches,
    (SELECT website_count FROM website_matches) AS website_domain_matches,
    (SELECT COUNT(*) FROM cordis.institution) AS cordis_institution_count,
    (SELECT COUNT(*) FROM openaire.organization) AS openaire_organization_count;

-----------------------------------------------
-- Find institutions with geographic proximity but different names
SELECT 
    a.id AS id1,
    b.id AS id2,
    a.legal_name AS name1,
    b.legal_name AS name2,
    a.country AS country1,
    b.country AS country2,
    point_distance(a.coordinates, b.coordinates) AS geo_distance,
    similarity(a.normalized_name, b.normalized_name) AS name_similarity
FROM institution_dedupe_candidates a
JOIN institution_dedupe_candidates b ON
    a.source_system = 'cordis' AND
    b.source_system = 'openaire' AND
    a.coordinates IS NOT NULL AND
    b.coordinates IS NOT NULL AND
    -- Within 10km
    point_distance(a.coordinates, b.coordinates) < 10
WHERE
    similarity(a.normalized_name, b.normalized_name) < 0.7  -- Different names but close location
ORDER BY geo_distance ASC
LIMIT 100;

-----------------------------------------------
-- Find institutions with same short name but different legal names
SELECT 
    a.id AS id1,
    b.id AS id2,
    a.legal_name AS name1,
    b.legal_name AS name2,
    a.source_system AS source1,
    b.source_system AS source2,
    a.short_name AS short_name1,
    b.short_name AS short_name2,
    similarity(a.normalized_name, b.normalized_name) AS name_similarity
FROM institution_dedupe_candidates a
JOIN institution_dedupe_candidates b ON
    a.id < b.id AND
    a.source_system != b.source_system AND
    a.normalized_short_name IS NOT NULL AND
    b.normalized_short_name IS NOT NULL AND
    LOWER(a.normalized_short_name) = LOWER(b.normalized_short_name) AND
    -- Different legal names
    LOWER(a.legal_name) != LOWER(b.legal_name)
ORDER BY name_similarity DESC;