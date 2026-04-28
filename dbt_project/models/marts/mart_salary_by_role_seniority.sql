-- Salary by role cluster AND seniority (for grouped bar chart)
with jobs as (
    select * from {{ ref('stg_job_postings') }}
    where annual_salary_est > 20000
      and annual_salary_est < 800000
      and experience_level in ('EN','MI','SE','EX','DI')
),

clustered as (
    select
        case
            when lower(job_title) like '%data engineer%'    then 'Data Engineer'
            when lower(job_title) like '%data scientist%'   then 'Data Scientist'
            when lower(job_title) like '%data analyst%'     then 'Data Analyst'
            when lower(job_title) like '%machine learning%' then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'
              or lower(job_title) like '%llm%'              then 'AI/LLM Engineer'
            when lower(job_title) like '%analytics engineer%' then 'Analytics Engineer'
            when lower(job_title) like '%bi %'
              or lower(job_title) like '%business intelligence%' then 'BI Developer'
            else null
        end as role_cluster,
        case experience_level
            when 'EN' then 'Entry Level'
            when 'MI' then 'Mid Level'
            when 'SE' then 'Senior'
            when 'EX' then 'Staff/Lead'
            when 'DI' then 'Principal'
        end as seniority,
        case experience_level
            when 'EN' then 1
            when 'MI' then 2
            when 'SE' then 3
            when 'EX' then 4
            when 'DI' then 5
        end as sort_order,
        annual_salary_est
    from jobs
)

select
    role_cluster,
    seniority,
    sort_order,
    count(*)                                         as posting_count,
    round(median(annual_salary_est), 0)              as median_salary,
    round(percentile_cont(0.10) within group (order by annual_salary_est), 0) as p10_salary,
    round(percentile_cont(0.90) within group (order by annual_salary_est), 0) as p90_salary
from clustered
where role_cluster is not null
group by role_cluster, seniority, sort_order
order by role_cluster, sort_order
