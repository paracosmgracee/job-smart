-- Job demand and salary by US state
-- Uses raw state_code field from API (more accurate than parsing location strings)
with jobs as (
    select
        job_id,
        job_title,
        state_code,
        annual_salary_est,
        case
            when lower(job_title) like '%data engineer%'      then 'Data Engineer'
            when lower(job_title) like '%data scientist%'     then 'Data Scientist'
            when lower(job_title) like '%data analyst%'       then 'Data Analyst'
            when lower(job_title) like '%machine learning%'   then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'
              or lower(job_title) like '%llm%'                then 'AI/LLM Engineer'
            when lower(job_title) like '%software engineer%'  then 'Software Engineer'
            when lower(job_title) like '%analytics engineer%' then 'Analytics Engineer'
            else null
        end as role_cluster
    from {{ ref('stg_all_postings') }}
    where state_code is not null
      and length(trim(state_code)) = 2
      and upper(trim(state_code)) rlike '[A-Z]{2}'
      and upper(trim(state_code)) != 'US'
)

select
    upper(trim(state_code))                           as state_code,
    count(distinct job_id)                            as job_count,
    round(median(annual_salary_est), 0)               as median_salary,
    round(avg(annual_salary_est), 0)                  as avg_salary,
    count(case when role_cluster = 'Data Engineer'    then 1 end) as de_count,
    count(case when role_cluster = 'Data Scientist'   then 1 end) as ds_count,
    count(case when role_cluster = 'Data Analyst'     then 1 end) as da_count,
    count(case when role_cluster = 'Software Engineer' then 1 end) as swe_count,
    count(case when role_cluster = 'ML Engineer'      then 1 end) as ml_count
from jobs
group by upper(trim(state_code))
having job_count >= 3
order by job_count desc
