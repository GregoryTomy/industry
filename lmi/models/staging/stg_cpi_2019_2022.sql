{{
    config(
        materialized="view",
    )
}}

with
    source_2019 as (select * from {{ source("staging", "cpi__20190100") }}),
    source_2020 as (select * from {{ source("staging", "cpi__20200100") }}),
    source_2021 as (select * from {{ source("staging", "cpi__20210100") }}),
    source_2022 as (select * from {{ source("staging", "cpi__20220100") }}),

    source_all as (
        select *
        from source_2019
        union
        select *
        from source_2020
        union
        select *
        from source_2021
        union
        select *
        from source_2022
    ),

    renamed as (
        select
            {{ adapter.quote("areaname") }} as area_name,
            {{ adapter.quote("periodyear") }} as year,
            {{ adapter.quote("cpi") }} as cpi,
        from source_all
    ),

    typed as (
        select
            cast(area_name as string) as area_name,
            cast(year as int) as year,
            cast(cpi as numeric) as cpi
        from renamed
    )

select *
from typed
