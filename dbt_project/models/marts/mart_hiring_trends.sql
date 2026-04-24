-- Monthly job posting volume by role cluster
-- Used by: hiring trends time-series chart
with jobs as (
    select * from {{ ref('stg_job_postings') }}
    where listed_at is not null
),

clustered as (
    select
        date_trunc('month', listed_at) as month,
        case
            when lower(job_title) like '%data engineer%'    then 'Data Engineer'
            when lower(job_title) like '%data scientist%'   then 'Data Scientist'
            when lower(job_title) like '%data analyst%'     then 'Data Analyst'
            when lower(job_title) like '%machine learning%' then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'      then 'AI Engineer'
            when lower(job_title) like '%software engineer%' then 'Software Engineer'
            else 'Other'
        end as role_cluster,
        job_id
    from jobs
)

select
    month,
    role_cluster,
    count(job_id) as posting_count
from clustered
where role_cluster != 'Other'
group by month, role_cluster
order by month, role_cluster
