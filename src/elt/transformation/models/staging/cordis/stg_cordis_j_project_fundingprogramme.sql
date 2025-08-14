{{ config(
    materialized='view'
) }}

select
    project_id,
    fundingprogramme_id
from {{ source('cordis', 'j_project_fundingprogramme') }}