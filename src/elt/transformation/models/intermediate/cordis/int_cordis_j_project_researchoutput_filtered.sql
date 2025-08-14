{{ config(materialized='table') }}

SELECT pr.*
FROM {{ ref('stg_cordis_j_project_researchoutput') }} pr
WHERE pr.project_id IN (
    SELECT project_id
    FROM {{ ref('int_cordis_project_dedup') }}
)
