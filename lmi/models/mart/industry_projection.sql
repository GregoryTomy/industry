{{
    config(
        materialized="view",
    )
}}

with
    qcew as (select * from {{ ref("ref_qcew_all") }}),
    labor as (select * from {{ ref("ref_labor_all") }}),
    income as (select * from {{ ref("ref_income_all") }}),
    gdp as (select * from {{ ref("ref_gdp_all") }}),
    cpi as (select * from {{ ref("ref_cpi_all") }})

select
    q.year,
    q.quarter,
    q.industry,
    q.average_quarter_employment,
    q.average_weekly_wage,
    q.average_annual_wage,
    l.unemployment_rate_calc,
    i.median_household_income,
    i.per_capital_personal_income,
    i.total_personal_income,
    g.gdp_in_millions,
    c.cpi
from qcew as q
left join labor as l on q.year = l.year and q.quarter = l.quarter
left join income as i on q.year = i.year
left join gdp as g on q.year = g.year
left join cpi as c on q.year = c.year
