{{ config(
    materialized='view'
) }}

select
    project_id,
    topic_id
from {{ source('cordis', 'j_project_topic') }}
