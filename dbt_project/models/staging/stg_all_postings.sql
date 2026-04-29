-- Union of Kaggle (2023-2024) and Adzuna (2025-2026) job postings
-- Used by mart models as the single source of truth
with kaggle as (
    select
        job_id,
        company_name,
        job_title,
        description,
        location,
        min_salary,
        max_salary,
        med_salary,
        pay_period,
        work_type,
        remote_allowed,
        views,
        applies,
        experience_level,
        listed_at,
        expires_at,
        annual_salary_est,
        null       as search_query,
        'kaggle'   as source,
        _loaded_at
    from {{ ref('stg_job_postings') }}
),

adzuna as (
    select
        job_id,
        company_name,
        job_title,
        description,
        location,
        min_salary,
        max_salary,
        med_salary,
        pay_period,
        work_type,
        remote_allowed,
        views,
        applies,
        experience_level,
        listed_at,
        expires_at,
        annual_salary_est,
        search_query,
        source,
        _loaded_at
    from {{ ref('stg_adzuna_postings') }}
)

select * from kaggle
union all
select * from adzuna
