{{ config(materialized='table') }}

SELECT *
FROM {{ ref('stg_openaire_author') }}