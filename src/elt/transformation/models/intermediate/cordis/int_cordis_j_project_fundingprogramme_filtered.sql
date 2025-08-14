{{ config(materialized='table') }}

SELECT pf.*
FROM {{ ref('stg_cordis_j_project_fundingprogramme') }} pf
WHERE pf.project_id IN (
    SELECT project_id
    FROM {{ ref('int_cordis_project_dedup') }}
)
