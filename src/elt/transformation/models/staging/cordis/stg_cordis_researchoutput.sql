{{ config(
    materialized='view',
) }}

select
    id as researchoutput_id,
    id_original,
    from_pdf,
    type,
    doi,
    title,
    publication_date,
    journal,
    summary as abstract,
    comment,
    fulltext,
    funding_number,
    journal_number,
    journal_title,
    published_pages,
    published_year,
    publisher,
    issn
from {{ source('cordis', 'researchoutput') }}