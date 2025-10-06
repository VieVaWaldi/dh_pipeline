{{ config(
    materialized='table',
    post_hook='ALTER TABLE {{ this }} ADD PRIMARY KEY (id);'
) }}

WITH arxiv_data AS (
    SELECT
        entry_id::TEXT as source_id,
        'arxiv'::TEXT as source_system,
        doi,
        id_original as original_id,
        published_date::TIMESTAMP as publication_date,
        updated_date,
        'publication'::TEXT as type,
        NULL::TEXT as language_code,
        title,
        abstract,
        fulltext,
        comment,
        NULL::TEXT as funding_number,
        NULL::TEXT as journal_number,
        NULL::TEXT as journal_name,
        NULL::TEXT as start_page,
        NULL::TEXT as end_page,
        NULL::TEXT as publisher,
        NULL::TEXT as issn_printed,
        NULL::TEXT as issn_online,
        NULL::TEXT as issn_linking,
        -- OpenAIRE specific columns (NULL for ArXiv)
        NULL::TEXT as sub_title,
        NULL::TEXT as language_label,
        NULL::TEXT as open_access_color,
        NULL::BOOLEAN as publicly_funded,
        NULL::BOOLEAN as is_green,
        NULL::BOOLEAN as is_in_diamond_journal,
        NULL::DECIMAL as citation_count,
        NULL::DECIMAL as influence,
        NULL::DECIMAL as popularity,
        NULL::DECIMAL as impulse,
        NULL::TEXT as citation_class,
        NULL::TEXT as influence_class,
        NULL::TEXT as impulse_class,
        NULL::TEXT as popularity_class,
        NULL::TEXT as volume,
        NULL::TEXT as issue,
        NULL::TEXT as edition,
        NULL::TEXT as conference_place,
        NULL::DATE as conference_date
    FROM {{ ref('int_arxiv_entry_dedup') }}
),

cordis_data AS (
    SELECT
        researchoutput_id::TEXT as source_id,
        'cordis'::TEXT as source_system,
        doi,
        id_original as original_id,
        publication_date::TIMESTAMP as publication_date,
        NULL::TIMESTAMP as updated_date,
        type,
        NULL::TEXT as language_code,
        title,
        abstract,
        fulltext,
        comment,
        funding_number,
        journal_number,
        journal_title as journal_name,
        published_pages as start_page,  -- Assuming published_pages contains start page info
        NULL::TEXT as end_page,         -- Extract if published_pages has range format
        publisher,
        NULL::TEXT as issn_printed,
        NULL::TEXT as issn_online,
        NULL::TEXT as issn_linking,
        -- OpenAIRE specific columns (NULL for CORDIS)
        NULL::TEXT as sub_title,
        NULL::TEXT as language_label,
        NULL::TEXT as open_access_color,
        NULL::BOOLEAN as publicly_funded,
        NULL::BOOLEAN as is_green,
        NULL::BOOLEAN as is_in_diamond_journal,
        NULL::DECIMAL as citation_count,
        NULL::DECIMAL as influence,
        NULL::DECIMAL as popularity,
        NULL::DECIMAL as impulse,
        NULL::TEXT as citation_class,
        NULL::TEXT as influence_class,
        NULL::TEXT as impulse_class,
        NULL::TEXT as popularity_class,
        NULL::TEXT as volume,
        NULL::TEXT as issue,
        NULL::TEXT as edition,
        NULL::TEXT as conference_place,
        NULL::DATE as conference_date
    FROM {{ ref('int_cordis_researchoutput_dedup') }}
),

openaire_data AS (
    SELECT
        researchoutput_id::TEXT as source_id,
        'openaire'::TEXT as source_system,
        NULL::TEXT as doi,  -- No DOI field in OpenAIRE RO
        id_original as original_id,
        publication_date::TIMESTAMP as publication_date,
        updated_at as updated_date,
        type,
        language_code,
        title,
        abstract,
        NULL::TEXT as fulltext,  -- No fulltext in OpenAIRE
        NULL::TEXT as comment,   -- No comment in OpenAIRE
        NULL::TEXT as funding_number,
        NULL::TEXT as journal_number,
        journal_name,
        start_page,
        end_page,
        publisher,
        issn_printed,
        issn_online,
        issn_linking,
        -- OpenAIRE specific columns
        sub_title,
        language_label,
        open_access_color,
        publicly_funded,
        is_green,
        is_in_diamond_journal,
        citation_count,
        influence,
        popularity,
        impulse,
        citation_class,
        influence_class,
        impulse_class,
        popularity_class,
        volume,
        issue,
        edition,
        conference_place,
        conference_date
    FROM {{ ref('int_openaire_researchoutput_dedup') }}
),

combined AS (
    SELECT * FROM arxiv_data
    UNION ALL
    SELECT * FROM cordis_data
    UNION ALL
    SELECT * FROM openaire_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,
    source_id,
    source_system,
    doi,
    original_id,
    publication_date,
    updated_date,
    type,
    language_code,
    title,
    abstract,
    fulltext,
    comment,
    funding_number,
    journal_number,
    journal_name,
    start_page,
    end_page,
    -- Computed fields
    CASE
        WHEN start_page IS NOT NULL AND end_page IS NOT NULL
        THEN start_page || '-' || end_page
        WHEN start_page IS NOT NULL
        THEN start_page
        ELSE NULL
    END as page_range,
    COALESCE(issn_printed, issn_online, issn_linking) as issn,
    publisher,
    -- OpenAIRE specific columns
    sub_title,
    language_label,
    open_access_color,
    publicly_funded,
    is_green,
    is_in_diamond_journal,
    citation_count,
    influence,
    popularity,
    impulse,
    citation_class,
    influence_class,
    impulse_class,
    popularity_class,
    issn_printed,
    issn_online,
    issn_linking,
    volume,
    issue,
    edition,
    conference_place,
    conference_date,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
WHERE source_system IS NOT NULL
    AND source_id IS NOT NULL
ORDER BY source_system, source_id