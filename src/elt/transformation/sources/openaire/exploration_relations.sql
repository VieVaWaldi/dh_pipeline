-- exploration_openaire_relationships.sql

-- =====================================================
-- CONTAINER TO RESEARCHOUTPUT RELATIONSHIP ANALYSIS
-- =====================================================

-- Check if container_id relationship is 1:1, 1:many, or many:1
SELECT 'Container to ResearchOutput Cardinality' as analysis_type;

-- Count research outputs per container
SELECT
    'Research outputs per container' as metric,
    COUNT(*) as total_containers,
    AVG(ro_count) as avg_ro_per_container,
    MIN(ro_count) as min_ro_per_container,
    MAX(ro_count) as max_ro_per_container
FROM (
    SELECT
        container_id,
        COUNT(*) as ro_count
    FROM openaire.researchoutput
    WHERE container_id IS NOT NULL
    GROUP BY container_id
) container_counts;

-- Count containers per research output (should be 0 or 1)
SELECT
    'Containers per research output' as metric,
    COUNT(*) as total_research_outputs,
    COUNT(container_id) as research_outputs_with_container,
    COUNT(*) - COUNT(container_id) as research_outputs_without_container
FROM openaire.researchoutput;

-- Sample container data to understand what we're working with
SELECT 'Sample container data' as analysis_type;
SELECT
    c.id,
    c.name,
    c.issn_printed,
    c.issn_online,
    COUNT(ro.id) as research_output_count
FROM openaire.container c
LEFT JOIN openaire.researchoutput ro ON c.id = ro.container_id
GROUP BY c.id, c.name, c.issn_printed, c.issn_online
ORDER BY research_output_count DESC
LIMIT 10;

-- =====================================================
-- FUNDING STRUCTURE ANALYSIS
-- =====================================================

SELECT 'Funding Structure Analysis' as analysis_type;

-- Count projects per funder
SELECT
    'Projects per funder' as metric,
    COUNT(*) as total_funders,
    AVG(project_count) as avg_projects_per_funder,
    MIN(project_count) as min_projects_per_funder,
    MAX(project_count) as max_projects_per_funder
FROM (
    SELECT
        funder_id,
        COUNT(*) as project_count
    FROM openaire.j_project_funder
    GROUP BY funder_id
) funder_counts;

-- Count funders per project
SELECT
    'Funders per project' as metric,
    COUNT(*) as total_projects,
    AVG(funder_count) as avg_funders_per_project,
    MIN(funder_count) as min_funders_per_project,
    MAX(funder_count) as max_funders_per_project
FROM (
    SELECT
        project_id,
        COUNT(*) as funder_count
    FROM openaire.j_project_funder
    GROUP BY project_id
) project_counts;

-- Sample funding data
SELECT 'Top funders by project count' as analysis_type;
SELECT
    f.name,
    f.short_name,
    f.jurisdiction,
    COUNT(jpf.project_id) as project_count,
    AVG(jpf.funded_amount) as avg_funded_amount,
    SUM(jpf.funded_amount) as total_funded_amount
FROM openaire.funder f
LEFT JOIN openaire.j_project_funder jpf ON f.id = jpf.funder_id
GROUP BY f.id, f.name, f.short_name, f.jurisdiction
ORDER BY project_count DESC
LIMIT 10;

-- =====================================================
-- FUNDING STREAM HIERARCHY ANALYSIS
-- =====================================================

SELECT 'Funding Stream Hierarchy' as analysis_type;

-- Check funding stream hierarchy depth
WITH RECURSIVE funding_hierarchy AS (
    -- Root level (no parent)
    SELECT
        id,
        original_id,
        name,
        parent_id,
        0 as level
    FROM openaire.funding_stream
    WHERE parent_id IS NULL

    UNION ALL

    -- Recursive part
    SELECT
        fs.id,
        fs.original_id,
        fs.name,
        fs.parent_id,
        fh.level + 1
    FROM openaire.funding_stream fs
    JOIN funding_hierarchy fh ON fs.parent_id = fh.id
)
SELECT
    level,
    COUNT(*) as stream_count,
    ARRAY_AGG(name ORDER BY name) as sample_names
FROM funding_hierarchy
GROUP BY level
ORDER BY level;

-- Projects per funding stream
SELECT
    'Projects per funding stream' as metric,
    COUNT(*) as total_funding_streams,
    AVG(project_count) as avg_projects_per_stream,
    MAX(project_count) as max_projects_per_stream
FROM (
    SELECT
        funding_stream_id,
        COUNT(*) as project_count
    FROM openaire.j_project_funding_stream
    GROUP BY funding_stream_id
) stream_counts;

-- =====================================================
-- H2020 PROGRAMME ANALYSIS
-- =====================================================

SELECT 'H2020 Programme Analysis' as analysis_type;

-- Count projects per H2020 programme
SELECT
    COUNT(*) as total_h2020_programmes,
    AVG(project_count) as avg_projects_per_programme,
    MAX(project_count) as max_projects_per_programme
FROM (
    SELECT
        h2020_programme_id,
        COUNT(*) as project_count
    FROM openaire.j_project_h2020_programme
    GROUP BY h2020_programme_id
) programme_counts;

-- Sample H2020 programmes
SELECT
    h.code,
    h.description,
    COUNT(jph.project_id) as project_count
FROM openaire.h2020_programme h
LEFT JOIN openaire.j_project_h2020_programme jph ON h.id = jph.h2020_programme_id
GROUP BY h.id, h.code, h.description
ORDER BY project_count DESC
LIMIT 10;

-- =====================================================
-- CONSOLIDATION RECOMMENDATION ANALYSIS
-- =====================================================

SELECT 'Data Consolidation Recommendations' as analysis_type;

-- Check for overlapping funding information
SELECT
    'Projects with multiple funding types' as metric,
    COUNT(*) as projects_with_funders,
    COUNT(CASE WHEN has_funding_stream THEN 1 END) as projects_with_funding_streams,
    COUNT(CASE WHEN has_h2020 THEN 1 END) as projects_with_h2020,
    COUNT(CASE WHEN has_funding_stream AND has_h2020 THEN 1 END) as projects_with_both_stream_and_h2020
FROM (
    SELECT
        p.id,
        EXISTS(SELECT 1 FROM openaire.j_project_funder jpf WHERE jpf.project_id = p.id) as has_funders,
        EXISTS(SELECT 1 FROM openaire.j_project_funding_stream jpfs WHERE jpfs.project_id = p.id) as has_funding_stream,
        EXISTS(SELECT 1 FROM openaire.j_project_h2020_programme jph WHERE jph.project_id = p.id) as has_h2020
    FROM openaire.project p
) funding_overlap;

-- Sample project with all funding types
SELECT 'Sample project with comprehensive funding info' as analysis_type;
SELECT
    p.title,
    p.code,
    f.name as funder_name,
    fs.name as funding_stream_name,
    h.code as h2020_code,
    jpf.funded_amount
FROM openaire.project p
LEFT JOIN openaire.j_project_funder jpf ON p.id = jpf.project_id
LEFT JOIN openaire.funder f ON jpf.funder_id = f.id
LEFT JOIN openaire.j_project_funding_stream jpfs ON p.id = jpfs.project_id
LEFT JOIN openaire.funding_stream fs ON jpfs.funding_stream_id = fs.id
LEFT JOIN openaire.j_project_h2020_programme jph ON p.id = jph.project_id
LEFT JOIN openaire.h2020_programme h ON jph.h2020_programme_id = h.id
WHERE jpf.project_id IS NOT NULL
   OR jpfs.project_id IS NOT NULL
   OR jph.project_id IS NOT NULL
LIMIT 5;