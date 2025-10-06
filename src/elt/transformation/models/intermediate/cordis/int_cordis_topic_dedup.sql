{{ config(materialized='table') }}

-- Simple pass-through for topic - no deduplication needed
SELECT * FROM {{ ref('stg_cordis_topic') }}