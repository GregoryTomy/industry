{{
    config(
        materialized="view",
    )
}}

with
    stg_2019 as (select * from {{ ref("stg_income_2019") }}),
    stg_2020 as (select * from {{ ref("stg_income_2020") }}),
    stg_2021 as (select * from {{ ref("stg_income_2021") }}),
    stg_2022 as (select * from {{ ref("stg_income_2022") }}),

    income_combined as (
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

    income_deduped as (
        select distinct * from income_combined where area_name = 'Colorado'
    )

select
    year,
    max(
        case when income_description = 'Median Household Income' then income end
    ) as median_household_income,
    max(
        case when income_description = 'Per Capita Personal Income' then income end
    ) as per_capital_personal_income,
    max(
        case when income_description = 'Total Personal Income' then income end
    ) as total_personal_income
from income_deduped
group by year
order by year
