-- Top skills by demand: how many jobs require each skill
-- Used by: dashboard skill trends chart, resume gap analysis
with skills as (
    select * from {{ ref('stg_job_skills') }}
),

jobs as (
    select job_id, listed_at, work_type, experience_level, annual_salary_est
    from {{ ref('stg_job_postings') }}
),

joined as (
    select
        s.skill_id,
        count(distinct s.job_id)                          as job_count,
        avg(j.annual_salary_est)                          as avg_salary,
        median(j.annual_salary_est)                       as median_salary,
        count(case when j.work_type = 'FULL-TIME' then 1 end) as fulltime_count,
        count(case when j.experience_level in ('EN','EX') then 1 end) as senior_count,
        date_trunc('month', min(j.listed_at))             as first_seen_month
    from skills s
    left join jobs j on s.job_id = j.job_id
    group by s.skill_id
)

select
    skill_id,
    job_count,
    round(avg_salary, 0)    as avg_salary,
    round(median_salary, 0) as median_salary,
    fulltime_count,
    senior_count,
    round(100.0 * senior_count / nullif(job_count, 0), 1) as senior_pct,
    first_seen_month,
    rank() over (order by job_count desc) as demand_rank
from joined
order by job_count desc
