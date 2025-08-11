------------------------------
-- Deduplication Macros

-- ToDo:
-- Some parameter default values are Arxiv specific

------------------------------
-- Macro for DOI deduplication
{% macro dedup_by_doi(source_table, id_col='entry_id', doi_col='doi', updated_col='updated_date') %}
  SELECT *
  FROM {{ source_table }}
  WHERE {{ id_col }} IN (
    -- Keep entries with unique DOIs
    SELECT {{ id_col }}
    FROM {{ source_table }}
    WHERE {{ doi_col }} IS NULL

    UNION
    -- Keep the latest entry for each DOI group
    SELECT {{ id_col }}
    FROM (
      SELECT {{ id_col }},
             ROW_NUMBER() OVER (PARTITION BY {{ doi_col }} ORDER BY {{ updated_col }} DESC) as rn
      FROM {{ source_table }}
      WHERE {{ doi_col }} IS NOT NULL
    ) ranked
    WHERE rn = 1
  )
{% endmacro %}

------------------------------
-- Macro for exact title deduplication
{% macro dedup_by_title(source_table, id_col='entry_id', title_col='title', updated_col='updated_date') %}
  SELECT *
  FROM {{ source_table }}
  WHERE {{ title_col }} IS NOT NULL
    AND TRIM({{ title_col }}) != ''
    AND {{ id_col }} IN (
      -- Keep the latest entry for each title group
      SELECT {{ id_col }}
      FROM (
        SELECT {{ id_col }},
               ROW_NUMBER() OVER (
                 PARTITION BY LOWER(TRIM({{ title_col }}))
                 ORDER BY {{ updated_col }} DESC
               ) as rn
        FROM {{ source_table }}
        WHERE {{ title_col }} IS NOT NULL
          AND TRIM({{ title_col }}) != ''
      ) ranked
      WHERE rn = 1
    )
{% endmacro %}

------------------------------
-- Macro for ArXiv version deduplication
{% macro dedup_by_arxiv_version(source_table, id_col='entry_id', arxiv_id_col='id_original', updated_col='updated_date') %}
  SELECT *
  FROM {{ source_table }}
  WHERE {{ id_col }} IN (
    SELECT {{ id_col }}
    FROM (
      SELECT {{ id_col }},
             ROW_NUMBER() OVER (
               PARTITION BY REGEXP_REPLACE({{ arxiv_id_col }}, 'v\d+', '')
               ORDER BY {{ updated_col }} DESC, {{ id_col }} DESC
             ) as rn
      FROM {{ source_table }}
    ) ranked
    WHERE rn = 1
  )
{% endmacro %}