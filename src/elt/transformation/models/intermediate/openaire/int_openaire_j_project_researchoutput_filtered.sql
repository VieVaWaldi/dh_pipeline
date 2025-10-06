{{ config(materialized='table') }}

SELECT jr.*
FROM {{ ref('stg_openaire_j_project_researchoutput') }} jr
WHERE jr.project_id IN (
    SELECT project_id
    FROM {{ ref('int_openaire_project_dedup') }}
)
AND jr.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_openaire_researchoutput_dedup') }}
)