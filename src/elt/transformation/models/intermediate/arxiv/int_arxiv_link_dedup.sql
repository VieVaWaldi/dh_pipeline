{{ config(materialized='table') }}

SELECT * FROM {{ ref('stg_arxiv_link') }}