-- Periodtype select 2 which brings in the quaterly data and not the annual aggregates
-- We will aggregate it later.
{{
    config(
        materialized="view",
    )
}}

with
    source as (select * from {{ source("staging", "ind202__20200100") }}),

    renamed as (
        select
            {{ adapter.quote("Areaname") }} as area_name,
            {{ adapter.quote("Industry") }} as industry,
            {{ adapter.quote("OwnershipDesc") }} as ownership_description,
            {{ adapter.quote("Periodtype") }} as periodtype,
            {{ adapter.quote("Periodyear") }} as year,
            {{ adapter.quote("Period") }} as quarter,
            {{ adapter.quote("Mnth1emp") }} as month_1_employment,
            {{ adapter.quote("Mnth2emp") }} as month_2_employment,
            {{ adapter.quote("Mnth3emp") }} as month_3_employment,
            {{ adapter.quote("Avgemp") }} as average_employment,
            {{ adapter.quote("Avgwkwage") }} as average_weekly_wage,
            {{ adapter.quote("Avgannualwage") }} as average_annual_wage
        from source
    ),

    typed as (
        select
            cast(area_name as string) as area_name,
            cast(industry as string) as industry,
            cast(ownership_description as string) as ownership_description,
            cast(year as integer) as year,
            cast(quarter as integer) as quarter,
            cast(month_1_employment as integer) as month_1_employment,
            cast(month_2_employment as integer) as month_2_employment,
            cast(month_3_employment as integer) as month_3_employment,
            cast(average_employment as integer) as average_employment,
            cast(average_weekly_wage as numeric) as average_weekly_wage,
            cast(average_annual_wage as numeric) as average_annual_wage
        from renamed
        where
            periodtype = 2
            and month_1_employment != 's;'
            and month_2_employment != 's;'
            and month_3_employment != 's;'
    ),

    final as (select * from typed)

select *
from final
