{{
    config(
        materialized="view",
    )
}}

with staging_cpi as (select * from {{ ref("stg_cpi_2019_2022") }})

select year, cpi
from staging_cpi
