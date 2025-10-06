{{ config(materialized='table') }}

SELECT * FROM {{ ref('stg_cordis_fundingprogramme') }}
