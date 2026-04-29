-- Skills that co-occur with AI/LLM job requirements
-- Shows which skills appear most in jobs that mention AI/LLM keywords
with tech_skills as (
    select trim(value::string) as skill
    from table(flatten(input => parse_json('[
        "python","sql","spark","aws","azure","gcp","kafka","airflow","dbt",
        "snowflake","tableau","power bi","docker","kubernetes","tensorflow",
        "pytorch","scikit-learn","pandas","numpy","excel","looker","databricks",
        "java","scala","git","hadoop","hive","redshift","bigquery",
        "mongodb","postgresql","mysql","javascript","typescript","fastapi",
        "flask","langchain","pinecone","mlflow","terraform","linux",
        "pyspark","streamlit","plotly","dask","redis","elasticsearch",
        "github actions","dagster","prefect","hugging face","ray","triton"
    ]')))
),

ai_jobs as (
    select
        job_id,
        lower(description) as desc_lower,
        annual_salary_est
    from {{ ref('stg_all_postings') }}
    where description is not null
      and length(description) > 100
      and (
          lower(description) rlike '.*(large language model|llm|generative ai|gen.?ai|gpt|llama|claude|gemini|vector database|rag |retrieval.augmented|embedding model|foundation model|transformer model).*'
          or lower(job_title) rlike '.*(ai engineer|llm engineer|ml engineer|machine learning engineer|genai|gen ai).*'
      )
),

all_jobs as (
    select count(distinct job_id) as total_ai_jobs from ai_jobs
),

matches as (
    select
        s.skill,
        count(distinct j.job_id)            as ai_job_count,
        round(median(j.annual_salary_est), 0) as median_salary
    from tech_skills s
    cross join ai_jobs j
    cross join all_jobs t
    where j.desc_lower like '%' || s.skill || '%'
    group by s.skill
),

ranked as (
    select
        skill,
        ai_job_count,
        median_salary,
        round(ai_job_count * 100.0 / nullif((select total_ai_jobs from all_jobs), 0), 1) as pct_of_ai_jobs,
        rank() over (order by ai_job_count desc) as skill_rank
    from matches
    where ai_job_count >= 3
)

select * from ranked
order by skill_rank
