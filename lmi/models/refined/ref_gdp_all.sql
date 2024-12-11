{{
    config(
        materialized="view",
    )
}}

with staging_gdp as (select * from {{ ref("stg_gdp_2019_2022") }})

select year(date) as year, gdp_in_millions
from staging_gdp
