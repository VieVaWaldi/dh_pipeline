{{ config(
    materialized='view'
) }}

select
    researchoutput_id,
    topic_id
from {{ source('cordis', 'j_researchoutput_topic') }}