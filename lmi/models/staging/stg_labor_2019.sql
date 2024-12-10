-- Note labor force is monthly.
{{
    config(
        materialized="view",
    )
}}

with
    source as (select * from {{ source("staging", "labforce__20190100") }}),

    renamed as (
        select
            {{ adapter.quote("Areaname") }} as area_name,
            {{ adapter.quote("Periodtype") }} as periodtype,
            {{ adapter.quote("Periodyear") }} as year,
            {{ adapter.quote("Period") }} as month,
            {{ adapter.quote("Adjusted") }} as is_adjusted,
            {{ adapter.quote("Laborforce") }} as labor_force,
            {{ adapter.quote("Emplab") }} as labor_force_employed,
            {{ adapter.quote("Unemp") }} as labor_force_unemployed,
            {{ adapter.quote("Unemprate") }} as unemployment_rate
        from source
    ),

    typed as (
        select
            cast(area_name as string) as area_name,
            cast(periodtype as int) as period_type,
            cast(year as int) as year,
            cast(month as int) as month,
            cast(
                case
                    when is_adjusted = 'Yes'
                    then true
                    when is_adjusted = 'No'
                    then false
                end as boolean
            ) as is_adjusted,
            cast(labor_force as int) as labor_force,
            cast(labor_force_employed as int) as labor_force_employed,
            cast(labor_force_unemployed as int) as labor_force_unemployed,
            cast(unemployment_rate as numeric) as unemployment_rate
        from renamed
    )

select *
from typed
