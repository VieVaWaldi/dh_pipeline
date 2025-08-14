{{ config(materialized='table') }}

SELECT rt.*
FROM {{ ref('stg_cordis_j_researchoutput_topic') }} rt
WHERE rt.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_cordis_researchoutput_dedup') }}
)
