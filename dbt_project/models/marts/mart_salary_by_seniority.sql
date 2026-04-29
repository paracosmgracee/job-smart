-- Salary by seniority level across all roles
with jobs as (
    select * from {{ ref('stg_all_postings') }}
    where annual_salary_est > 20000
      and annual_salary_est < 800000
      and experience_level is not null
)

select
    case experience_level
        when 'EN' then 'Entry Level'
        when 'MI' then 'Mid Level'
        when 'SE' then 'Senior'
        when 'EX' then 'Staff/Lead'
        when 'DI' then 'Principal'
        else 'Other'
    end                                              as seniority,
    case experience_level
        when 'EN' then 1
        when 'MI' then 2
        when 'SE' then 3
        when 'EX' then 4
        when 'DI' then 5
        else 9
    end                                              as sort_order,
    count(*)                                         as posting_count,
    round(avg(annual_salary_est), 0)                 as avg_salary,
    round(median(annual_salary_est), 0)              as median_salary,
    round(percentile_cont(0.10) within group (order by annual_salary_est), 0) as p10_salary,
    round(percentile_cont(0.90) within group (order by annual_salary_est), 0) as p90_salary
from jobs
where experience_level in ('EN','MI','SE','EX','DI')
group by experience_level
order by sort_order
