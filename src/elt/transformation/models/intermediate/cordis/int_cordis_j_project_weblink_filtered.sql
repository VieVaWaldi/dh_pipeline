{{ config(materialized='table') }}

SELECT pw.*
FROM {{ ref('stg_cordis_j_project_weblink') }} pw
WHERE pw.project_id IN (
    SELECT project_id
    FROM {{ ref('int_cordis_project_dedup') }}
)