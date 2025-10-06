{{ config(materialized='table') }}

SELECT jo.*
FROM {{ ref('stg_openaire_j_project_organization') }} jo
WHERE jo.project_id IN (
    SELECT project_id
    FROM {{ ref('int_openaire_project_dedup') }}
)
AND jo.organization_id IN (
    SELECT organization_id
    FROM {{ ref('int_openaire_organization_dedup') }}
)