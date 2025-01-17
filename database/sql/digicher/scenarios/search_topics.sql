--------------------------------------------------------------------------------------------
-- Find institutions working on the same EuroSciVoc topics through project collaborations --
--------------------------------------------------------------------------------------------

WITH TopicInstitutions AS (
    SELECT
        t.id AS topic_id,
        t.name AS topic_name,
        i.id AS institution_id,
        i.name AS institution_name
    FROM Topics t
    JOIN Projects_Topics pt ON t.id = pt.topic_id
    JOIN Projects_Institutions pi ON pt.project_id = pi.project_id
    JOIN Institutions i ON pi.institution_id = i.id
    WHERE t.cordis_classification = 'euroSciVoc'
),
InstitutionGroups AS (
    SELECT
        topic_id,
        topic_name,
        COUNT(DISTINCT institution_id) as institution_count,
        array_agg(DISTINCT institution_name) as institutions
    FROM TopicInstitutions
    GROUP BY topic_id, topic_name
    HAVING COUNT(DISTINCT institution_id) >= 2
)
SELECT
    topic_name,
    institution_count,
    institutions[1:5] as sample_institutions,
    CASE
        WHEN array_length(institutions, 1) > 5
        THEN array_length(institutions, 1) - 5
        ELSE 0
    END as additional_institutions
FROM InstitutionGroups
ORDER BY institution_count DESC;