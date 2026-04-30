-- All live job postings: Adzuna + JSearch + Target Company Portals
-- Deduped by company + title + date; salary-bearing records preferred
with adzuna as (
    select * from {{ ref('stg_adzuna_postings') }}
),

jsearch as (
    select * from {{ ref('stg_jsearch_postings') }}
),

portals as (
    select * from {{ ref('stg_portal_postings') }}
),

unioned as (
    select * from adzuna
    union all
    select * from jsearch
    union all
    select * from portals
),

deduped as (
    select *,
        row_number() over (
            partition by
                lower(trim(company_name)),
                lower(trim(job_title)),
                date(listed_at)
            order by
                case when annual_salary_est is not null then 0 else 1 end,
                source
        ) as rn
    from unioned
)

select * exclude(rn)
from deduped
where rn = 1
