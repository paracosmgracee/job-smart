-- Technical skill demand broken down by role cluster
-- Enables role-specific skill filtering in the dashboard
with tech_skills as (
    select trim(value::string) as skill
    from table(flatten(input => parse_json('[
        "python","sql","spark","aws","azure","gcp","kafka","airflow","dbt",
        "snowflake","tableau","power bi","docker","kubernetes","tensorflow",
        "pytorch","scikit-learn","pandas","numpy","excel","looker","databricks",
        "java","scala","r studio","git","hadoop","hive","redshift","bigquery",
        "mongodb","postgresql","mysql","javascript","typescript","fastapi",
        "flask","langchain","llm","pinecone","mlflow","terraform","linux",
        "pyspark","streamlit","plotly","dask","redis","elasticsearch",
        "github actions","dagster","prefect"
    ]')))
),

jobs as (
    select
        job_id,
        lower(description) as desc_lower,
        annual_salary_est,
        case
            when lower(job_title) like '%data engineer%'     then 'Data Engineer'
            when lower(job_title) like '%data scientist%'    then 'Data Scientist'
            when lower(job_title) like '%data analyst%'      then 'Data Analyst'
            when lower(job_title) like '%machine learning%'  then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'
              or lower(job_title) like '%llm%'               then 'AI/LLM Engineer'
            when lower(job_title) like '%software engineer%' then 'Software Engineer'
            when lower(job_title) like '%analytics engineer%' then 'Analytics Engineer'
            else 'Other'
        end as role_cluster
    from {{ ref('stg_all_postings') }}
    where description is not null
      and length(description) > 100
),

matches as (
    select
        s.skill,
        j.role_cluster,
        j.job_id,
        j.annual_salary_est
    from tech_skills s
    cross join jobs j
    where j.role_cluster != 'Other'
      and j.desc_lower like '%' || s.skill || '%'
)

select
    role_cluster,
    skill,
    count(distinct job_id)              as job_count,
    round(median(annual_salary_est), 0) as median_salary,
    rank() over (
        partition by role_cluster
        order by count(distinct job_id) desc
    )                                   as role_rank
from matches
group by role_cluster, skill
having job_count >= 5
order by role_cluster, role_rank
