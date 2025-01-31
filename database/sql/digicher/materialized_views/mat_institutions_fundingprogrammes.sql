-----------------------------------------------
-- Institutions Funding Programmes           --
-----------------------------------------------

-- Matches all Institutions to all Projects they worked on, and those projects to all FundingProgrammes that belong to that Project.
-- Assigns these FundingProgrammes deduplicated as a list to the Institutions.
-- So this is basically a table of all the Institutions each having a list of FundingProgrammes they worked on.

CREATE MATERIALIZED VIEW mat_institutions_fundingprogrammes AS
SELECT 
    i.id as institution_id,
	array_agg(DISTINCT f.id) as funding_ids
	-- array_agg(DISTINCT ARRAY[t.level, t.id]) as topics
FROM institutions as i
INNER JOIN projects_institutions as pi ON pi.institution_id = i.id
INNER JOIN projects as p ON pi.project_id = p.id
INNER JOIN projects_fundingprogrammes as pfp ON pfp.project_id = p.id
INNER JOIN fundingprogrammes as f ON pfp.fundingprogramme_id = f.id
GROUP BY i.id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT count(*) FROM mat_institutions_fundingprogrammes; -- 30k
LIMIT 100;

-- Funding Programme Distribution --
SELECT 
    avg(array_length(funding_ids, 1)) as avg_fp_per_institution, -- 5 avg fp per institution
    max(array_length(funding_ids, 1)) as max_fp_per_institution, -- 432 max fp for one instiution
    min(array_length(funding_ids, 1)) as min_fp_per_institution, -- 1 min fp
    count(*) as total_institutions								  -- And 30k total institutions
FROM mat_institutions_fundingprogrammes;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- Primary index on institution id
-- CREATE UNIQUE INDEX idx_mat_institutions_fundingprogrammes_id ON mat_institutions_fundingprogrammes(id);

-- GIN index for searching within the fp array
-- CREATE INDEX idx_mat_institutions_fundingprogrammes_gin ON mat_institutions_fundingprogrammes USING gin(funding_ids);

-- Optional: If you frequently search for specific fp names
-- CREATE INDEX idx_mat_institutions_fundingprogrammes_names ON mat_institutions_fundingprogrammes USING gin((funding_ids[][1]));

-- Optional: If you frequently filter by level
-- CREATE INDEX idx_mat_institutions_fundingprogrammes_levels ON mat_institutions_fundingprogrammes USING gin((funding_ids[][2]));

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes
SELECT pg_size_pretty(pg_total_relation_size('mat_institutions_fundingprogrammes'));

-- Data size without indexes
SELECT pg_size_pretty(pg_relation_size('mat_institutions_fundingprogrammes'));

-- Size of fp array column
SELECT 
    pg_size_pretty(sum(pg_column_size(funding_ids))) as total_fp_size,
    pg_size_pretty(avg(pg_column_size(funding_ids))) as avg_fp_size
FROM mat_institutions_fundingprogrammes;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW mat_institutions_fundingprogrammes;
DROP INDEX 
	idx_mat_institutions_fundingprogrammes_id, 
	idx_mat_institutions_fundingprogrammes_gin, 
	idx_mat_institutions_fundingprogrammes_names, 
	idx_mat_institutions_fundingprogrammes_levels;