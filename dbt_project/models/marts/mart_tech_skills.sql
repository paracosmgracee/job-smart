-- Extract technical skill mentions from job descriptions via keyword matching
-- Cross-joins a predefined skill list against job descriptions using ILIKE
with tech_skills as (
    select trim(value::string) as skill
    from table(flatten(input => parse_json('[
        "python","sql","spark","aws","azure","gcp","kafka","airflow","dbt",
        "snowflake","tableau","power bi","docker","kubernetes","tensorflow",
        "pytorch","scikit-learn","pandas","numpy","excel","looker","databricks",
        "java","scala","r studio","git","hadoop","hive","redshift","bigquery",
        "mongodb","postgresql","mysql","javascript","typescript","fastapi",
        "flask","langchain","llm","pinecone","mlflow","terraform","linux",
        "pyspark","streamlit","plotly","dask","celery","redis","elasticsearch",
        "github actions","airflow","dagster","prefect"
    ]')))
),

jobs as (
    select
        job_id,
        lower(description)          as desc_lower,
        annual_salary_est,
        experience_level
    from {{ ref('stg_all_postings') }}
    where description is not null
      and length(description) > 100
),

matches as (
    select
        s.skill,
        j.job_id,
        j.annual_salary_est,
        j.experience_level
    from tech_skills s
    cross join jobs j
    where j.desc_lower like '%' || s.skill || '%'
)

select
    skill,
    count(distinct job_id)                          as job_count,
    round(avg(annual_salary_est), 0)                as avg_salary,
    round(median(annual_salary_est), 0)             as median_salary,
    count(case when experience_level in ('EX','DI') then 1 end) as senior_count,
    rank() over (order by count(distinct job_id) desc) as demand_rank
from matches
group by skill
having job_count > 10
order by job_count desc
