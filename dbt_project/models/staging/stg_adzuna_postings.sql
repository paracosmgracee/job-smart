-- Cleaned and typed view over live Adzuna job postings (2025-2026)
with source as (
    select * from {{ source('raw', 'ADZUNA_POSTINGS') }}
),

cleaned as (
    select
        JOB_ID::varchar                                      as job_id,
        trim(COMPANY)                                        as company_name,
        trim(TITLE)                                          as job_title,
        trim(DESCRIPTION)                                    as description,
        trim(LOCATION)                                       as location,
        SALARY_MIN::float                                    as min_salary,
        SALARY_MAX::float                                    as max_salary,
        SALARY_EST::float                                    as med_salary,
        'YEARLY'                                             as pay_period,
        'FULL-TIME'                                          as work_type,
        null::varchar                                        as remote_allowed,
        null::int                                            as views,
        null::int                                            as applies,
        null::varchar                                        as experience_level,
        try_to_timestamp(POSTED_AT)                          as listed_at,
        null::timestamp                                      as expires_at,
        SALARY_EST::float                                    as annual_salary_est,
        QUERY                                                as search_query,
        'adzuna'                                             as source,
        try_to_timestamp(FETCHED_AT)                         as _loaded_at
    from source
    where JOB_ID is not null
      and TITLE is not null
)

select * from cleaned
