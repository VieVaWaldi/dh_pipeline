{{ config(
    materialized='view',
) }}

select
    id as researchoutput_id,
    id_original,
    main_title as title,
    sub_title,
    publication_date,
    publisher,
    type,
    language_code,
    language_label,
    open_access_color,
    publicly_funded,
    is_green,
    is_in_diamond_journal,
    description as abstract,
    citation_count,
    influence,
    popularity,
    impulse,
    citation_class,
    influence_class,
    impulse_class,
    popularity_class,
    container_id,
    created_at,
    updated_at
from {{ source('openaire', 'researchoutput') }}