{{ config(materialized='table') }}

SELECT jro.*
FROM {{ ref('stg_openaire_j_researchoutput_organization') }} jro
WHERE jro.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_openaire_researchoutput_dedup') }}
)
AND jro.organization_id IN (
    SELECT organization_id
    FROM {{ ref('int_openaire_organization_dedup') }}
)