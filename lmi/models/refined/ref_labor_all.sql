{{
    config(
        materialized="view",
    )
}}

with
    stg_2019 as (select * from {{ ref("stg_labor_2019") }}),
    stg_2020 as (select * from {{ ref("stg_labor_2020") }}),
    stg_2021 as (select * from {{ ref("stg_labor_2021") }}),
    stg_2022 as (select * from {{ ref("stg_labor_2022") }}),

    labor_combined as (
        select *
        from stg_2019
        union
        select *
        from stg_2020
        union
        select *
        from stg_2021
        union
        select *
        from stg_2022
    ),

    labor_deduped as (select distinct * from labor_combined),

    labor_aggregated as (
        select
            year,
            case
                when month in (1, 2, 3)
                then 1
                when month in (4, 5, 6)
                then 2
                when month in (7, 8, 9)
                then 3
                when month in (10, 11, 12)
                then 4
            end as quarter,
            sum(labor_force) as total_labor_force,
            sum(labor_force_unemployed) as total_labor_force_unemployed
        from labor_deduped
        where area_name = 'Colorado' and period_type = 3 and is_adjusted = true
        group by year, quarter

    )

select
    *,
    (
        total_labor_force_unemployed::numeric / total_labor_force::numeric
    ) as unemployment_rate_calc
from labor_aggregated
order by year, quarter
