{{ config(
    materialized='view'
) }}

select
    research_output_id as researchoutput_id,
    author_id,
    rank as author_position
from {{ source('openaire', 'j_researchoutput_author') }}
