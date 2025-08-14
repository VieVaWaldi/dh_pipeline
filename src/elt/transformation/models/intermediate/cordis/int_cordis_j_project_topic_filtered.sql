{{ config(materialized='table') }}

SELECT pt.*
FROM {{ ref('stg_cordis_j_project_topic') }} pt
WHERE pt.project_id IN (
    SELECT project_id
    FROM {{ ref('int_cordis_project_dedup') }}
)