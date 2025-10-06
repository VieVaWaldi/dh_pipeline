------------------------------
----  Data Exploration Cordis
------------------------------

------------------------------
----  Research Outputs
------------------------------

-- DOI Coverage
SELECT
    COUNT(*) as total_researchoutputs,
    COUNT(doi) as ro_with_doi,
    ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) as doi_coverage_pct
FROM cordis.researchoutput;
-- 44%, 279k ROs with doi

------------------------------
-- DOI Duplicates
SELECT doi, COUNT(*) as duplicate_count
FROM cordis.researchoutput
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
-- 14_478 doi duplicate with count of up to 9

------------------------------
-- Exact title matches
-- (case-insensitive)
SELECT LOWER(TRIM(title)) as clean_title, COUNT(*) as duplicate_count
FROM cordis.researchoutput
WHERE from_pdf = false
GROUP BY LOWER(TRIM(title))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Do not use! Very tricky because we got lots of generic names like 'data management plan', 
-- 	   'project website' etc. Unusable with 24k duplicate groups

------------------------------
-- Research Output Types Distribution
SELECT type, COUNT(*) as count
FROM cordis.researchoutput
GROUP BY type
ORDER BY count DESC;

------------------------------
-- Potential version duplicates
-- (look for patterns like v1, v2, version indicators)
-- Potential version duplicates
WITH title_counts AS (
    SELECT
        id_original,
        title,
		from_pdf,
        COUNT(*) OVER (PARTITION BY LOWER(TRIM(title))) as title_matches
    FROM cordis.researchoutput
)
SELECT *
FROM title_counts
WHERE title_matches > 1
AND from_pdf = false
ORDER BY title_matches DESC, LOWER(TRIM(title));

-- Do not use! Same naming issue.

------------------------------
----  Projects
------------------------------

-- DOI Coverage for Projects
SELECT
    COUNT(*) as total_projects,
    COUNT(doi) as projects_with_doi,
    ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) as doi_coverage_pct
FROM cordis.project;
-- 53%, 34k projects with doi

------------------------------
-- Project DOI Duplicates
SELECT doi, COUNT(*) as duplicate_count
FROM cordis.project
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
-- 0 project duplicates with doi

------------------------------
-- Project Title Duplicates
SELECT LOWER(TRIM(title)) as clean_title, COUNT(*) as duplicate_count
FROM cordis.project
GROUP BY LOWER(TRIM(title))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 338 duplicate project names with count up to 9

SELECT SUM(duplicate_count - 1) as excess_duplicates,
       SUM(duplicate_count) as total_records_with_duplicates
FROM (
    SELECT LOWER(TRIM(title)) as clean_title, COUNT(*) as duplicate_count
    FROM cordis.project
    GROUP BY LOWER(TRIM(title))
    HAVING COUNT(*) > 1
) subquery;

-- 409 excess duplicates

------------------------------
-- Project Acronym Duplicates
SELECT UPPER(TRIM(acronym)) as clean_acronym, COUNT(*) as duplicate_count
FROM cordis.project
WHERE acronym IS NOT NULL
GROUP BY UPPER(TRIM(acronym))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Do not use! Can definitely be used multiple times, the acronyms are short. Ignore for deduplication!


------------------------------
-- Combined Title + Acronym potential duplicates
SELECT
    LOWER(TRIM(title)) as clean_title,
    UPPER(TRIM(acronym)) as clean_acronym,
    COUNT(*) as duplicate_count,
    array_agg(id_original ORDER BY id_original) as project_ids
FROM cordis.project
GROUP BY LOWER(TRIM(title)), UPPER(TRIM(acronym))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 32 with count 2 each

------------------------------
----  Institutions
------------------------------

-- Institution Legal Name Duplicates
SELECT legal_name, COUNT(*) as duplicate_count
FROM cordis.institution
GROUP BY legal_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- None

------------------------------
-- Institution Legal Name (case-insensitive)
SELECT LOWER(TRIM(legal_name)) as clean_legal_name, COUNT(*) as duplicate_count
FROM cordis.institution
GROUP BY LOWER(TRIM(legal_name))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 2_2565 duplicates with count up to 4
-- Some are just called university

select * from cordis.institution
where lower(legal_name) = 'university';

------------------------------
-- Institution Short Name Duplicates
SELECT short_name, COUNT(*) as duplicate_count
FROM cordis.institution
WHERE short_name IS NOT NULL
GROUP BY short_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Do not use! These are 2 small and could actually refer to different institutions.

------------------------------
-- Institution VAT Number Duplicates (should be unique)
SELECT vat_number, COUNT(*) as duplicate_count
FROM cordis.institution
WHERE vat_number IS NOT NULL
GROUP BY vat_number
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 130 duplicates with count up to 11
-- Filter out 'MISSING' or 'NOTAPPLICABLE' here

select * from cordis.institution 
where vat_number = 'DE811335517';-- ESS0811001G, GB888808059

-- Seems to be a good deparment indicator!

------------------------------
-- Institution URL Duplicates
SELECT url, COUNT(*) as duplicate_count
FROM cordis.institution
WHERE url IS NOT NULL
GROUP BY url
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 1_781 duplicates with count up to 28

select * from cordis.institution 
where url = 'http://www.ethz.ch'; -- http://www.mpg.de, http://www.uni-frankfurt.de, http://www.cam.ac.uk

-- Seems to be a very good indicator of duplicates