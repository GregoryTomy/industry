{{
    config(
        materialized="view",
    )
}}

with
    source as (select * from {{ source("staging", "COGOVSLRGSP") }}),

    renamed as (
        select
            {{ adapter.quote("DATE") }} as date,
            {{ adapter.quote("COGOVSLRGSP") }} as gdp_in_millions,
        from source
    ),

    typed as (
        select
            cast(date as date) as date,
            cast(gdp_in_millions as numeric) as gdp_in_millions
        from renamed
    )

select *
from typed
