{{ config(materialized='table') }}

SELECT jra.*
FROM {{ ref('stg_openaire_j_researchoutput_author') }} jra
WHERE jra.researchoutput_id IN (
    SELECT researchoutput_id
    FROM {{ ref('int_openaire_researchoutput_dedup') }}
)
AND jra.author_id IN (
    SELECT author_id
    FROM {{ ref('int_openaire_author_dedup') }}
)
