{{
    config(
        materialized="view",
    )
}}

with
    stg_2019 as (select * from {{ ref("stg_qcew_2019") }}),
    stg_2020 as (select * from {{ ref("stg_qcew_2020") }}),
    stg_2021 as (select * from {{ ref("stg_qcew_2021") }}),
    stg_2022 as (select * from {{ ref("stg_qcew_2022") }}),

    qcew_combined as (
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

    qcew_deduped as (select distinct * from qcew_combined),

    state_level as (
        select
            year,
            quarter,
            industry,
            avg(month_1_employment) as average_1_month_employment,
            avg(month_2_employment) as average_2_month_employment,
            avg(month_3_employment) as average_3_month_employment,
            sum(month_1_employment) as total_1_month_employment,
            sum(month_2_employment) as total_2_month_employment,
            sum(month_3_employment) as total_3_month_employment,
            avg(average_weekly_wage) as average_weekly_wage,
            avg(average_annual_wage) as average_annual_wage
        from qcew_deduped
        where
            area_name = 'Colorado' and ownership_description = 'Aggregate of all types'
        group by year, quarter, industry
    )

select
    year,
    quarter,
    industry,
    (
        average_1_month_employment
        + average_2_month_employment
        + average_3_month_employment
    )
    / 3 as average_quarter_employment,
    (
        total_1_month_employment + total_2_month_employment + total_3_month_employment
    ) as total_quarter_employment,
    average_weekly_wage,
    average_annual_wage
from state_level
order by year, quarter, average_quarter_employment desc
