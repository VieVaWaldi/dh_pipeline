{{ config(
    materialized='view',
) }}

select
    id as topic_id,
    name,
    level
from {{ source('cordis', 'topic') }}