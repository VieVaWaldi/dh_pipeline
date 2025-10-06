{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT project_institution_pkey PRIMARY KEY (project_id, institution_id, institution_position)"
) }}

WITH cordis_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      i.id as institution_id,
      junction.institution_position,
      junction.ec_contribution,
      junction.net_ec_contribution,
      junction.total_cost,
      junction.type,
      junction.organization_id,
      junction.rcn,
      -- OpenAIRE specific columns (NULL for CORDIS)
      NULL::TEXT as relation_type,
      NULL::DATE as validation_date,
      NULL::BOOLEAN as validated
  FROM {{ ref('int_cordis_j_project_institution_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'cordis'
  JOIN {{ ref('institution') }} i
    ON i.source_id = junction.institution_id::TEXT AND i.source_system = 'cordis'
),

openaire_junctions AS (
  SELECT DISTINCT
      p.id as project_id,
      i.id as institution_id,
      NULL::INTEGER as institution_position,
      NULL::DECIMAL as ec_contribution,
      NULL::DECIMAL as net_ec_contribution,
      NULL::DECIMAL as total_cost,
      NULL::TEXT as type,
      NULL::TEXT as organization_id,
      NULL::INTEGER as rcn,
      -- OpenAIRE specific columns
      junction.relation_type,
      junction.validation_date,
      junction.validated
  FROM {{ ref('int_openaire_j_project_organization_filtered') }} junction
  JOIN {{ ref('project') }} p
    ON p.source_id = junction.project_id::TEXT AND p.source_system = 'openaire'
  JOIN {{ ref('institution') }} i
    ON i.source_id = junction.organization_id::TEXT AND i.source_system = 'openaire'
),

combined AS (
  SELECT * FROM cordis_junctions
  UNION ALL
  SELECT * FROM openaire_junctions
)

SELECT
  project_id,
  institution_id,
  COALESCE(institution_position, -1) as institution_position,
  ec_contribution,
  net_ec_contribution,
  total_cost,
  type,
  organization_id,
  rcn,
  relation_type,
  validation_date,
  validated
FROM combined
ORDER BY project_id, institution_position, institution_id