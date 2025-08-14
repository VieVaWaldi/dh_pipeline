{{ config(materialized='table') }}

SELECT rp.*
FROM {{ ref('stg_cordis_j_researchoutput_person') }} rp
WHERE rp.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_cordis_researchoutput_dedup') }}
)
