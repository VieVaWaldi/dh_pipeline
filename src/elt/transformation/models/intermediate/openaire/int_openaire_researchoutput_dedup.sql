{{ config(materialized='table') }}

WITH researchoutput_with_container AS (
  SELECT
    ro.researchoutput_id,
    ro.id_original,
    ro.title,
    ro.sub_title,
    ro.publication_date,
    ro.publisher,
    ro.type,
    ro.language_code,
    ro.language_label,
    ro.open_access_color,
    ro.publicly_funded,
    ro.is_green,
    ro.is_in_diamond_journal,
    ro.abstract,
    ro.citation_count,
    ro.influence,
    ro.popularity,
    ro.impulse,
    ro.citation_class,
    ro.influence_class,
    ro.impulse_class,
    ro.popularity_class,
    ro.created_at,
    ro.updated_at,

    -- Flattened container information
    c.name as journal_name,
    c.issn_printed,
    c.issn_online,
    c.issn_linking,
    c.volume,
    c.issue,
    c.start_page,
    c.end_page,
    c.edition,
    c.conference_place,
    c.conference_date

  FROM {{ ref('stg_openaire_researchoutput') }} ro
  LEFT JOIN {{ ref('stg_openaire_container') }} c ON ro.container_id = c.container_id
)

SELECT * FROM researchoutput_with_container