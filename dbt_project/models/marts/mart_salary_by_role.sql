-- Salary distribution by job title cluster
-- Powers the salary analysis dashboard panel
with jobs as (
    select * from {{ ref('stg_job_postings') }}
    where annual_salary_est > 10000  -- filter out bad data
      and annual_salary_est < 1000000
),

-- Normalize common title variants into clusters
title_clusters as (
    select
        job_id,
        company_name,
        job_title,
        annual_salary_est,
        experience_level,
        work_type,
        location,
        listed_at,
        case
            when lower(job_title) like '%data engineer%'       then 'Data Engineer'
            when lower(job_title) like '%data scientist%'      then 'Data Scientist'
            when lower(job_title) like '%data analyst%'        then 'Data Analyst'
            when lower(job_title) like '%machine learning%'    then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'         then 'AI Engineer'
            when lower(job_title) like '%software engineer%'   then 'Software Engineer'
            when lower(job_title) like '%backend%'             then 'Backend Engineer'
            when lower(job_title) like '%frontend%'            then 'Frontend Engineer'
            when lower(job_title) like '%fullstack%'
              or lower(job_title) like '%full stack%'          then 'Full Stack Engineer'
            when lower(job_title) like '%product manager%'     then 'Product Manager'
            when lower(job_title) like '%devops%'
              or lower(job_title) like '%platform engineer%'   then 'DevOps/Platform'
            else 'Other'
        end as role_cluster
    from jobs
)

select
    role_cluster,
    count(*)                           as posting_count,
    round(avg(annual_salary_est), 0)   as avg_salary,
    round(median(annual_salary_est), 0) as median_salary,
    round(min(annual_salary_est), 0)   as p10_salary,
    round(max(annual_salary_est), 0)   as p90_salary,
    round(percentile_cont(0.25) within group (order by annual_salary_est), 0) as p25_salary,
    round(percentile_cont(0.75) within group (order by annual_salary_est), 0) as p75_salary
from title_clusters
where role_cluster != 'Other'
group by role_cluster
order by median_salary desc
