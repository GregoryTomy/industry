{{
    config(
        materialized="view",
    )
}}

with
    source as (select * from {{ source("staging", "income__20190100") }}),

    renamed as (
        select
            {{ adapter.quote("areaname") }} as area_name,
            {{ adapter.quote("periodyear") }} as year,
            {{ adapter.quote("Incdesc") }} as income_description,
            {{ adapter.quote("Incsrcdesc") }} as source_description,
            {{ adapter.quote("income") }} as income,
        from source
    ),

    typed as (
        select
            cast(area_name as string) as area_name,
            cast(year as int) as year,
            cast(income_description as string) as income_description,
            cast(source_description as string) as source_description,
            cast(income as bigint) as income
        from renamed
    )

select *
from typed
