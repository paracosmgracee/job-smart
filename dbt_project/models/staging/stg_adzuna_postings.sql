-- Cleaned and typed view over live Adzuna job postings (2025-2026)
-- experience_level, remote_allowed, work_type extracted from title/description
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

        -- work_type: infer from description
        case
            when lower(DESCRIPTION) rlike '.*(contract|contractor|freelance|1099|corp.to.corp|c2c).*' then 'CONTRACT'
            when lower(DESCRIPTION) rlike '.*(part[ -]time).*'                                        then 'PART_TIME'
            else 'FULL_TIME'
        end                                                  as work_type,

        -- remote_allowed: check location + description
        case
            when lower(LOCATION) like '%remote%'                                                      then '1'
            when lower(DESCRIPTION) rlike '.*(fully remote|100% remote|work from home|wfh|remote.first|remote only).*' then '1'
            when lower(DESCRIPTION) rlike '.*(hybrid).*'                                              then 'hybrid'
            else '0'
        end                                                  as remote_allowed,

        null::int                                            as views,
        null::int                                            as applies,

        -- experience_level: infer from job title (more reliable than description)
        case
            when lower(TITLE) rlike '.*(vp |vice president|director|head of|chief).*'   then 'EX'
            when lower(TITLE) rlike '.*(staff |principal |lead ).*'                      then 'DI'
            when lower(TITLE) rlike '.*(senior|sr\\.? |sr-).*'                           then 'SE'
            when lower(TITLE) rlike '.*(junior|jr\\.? |jr-|entry.level|associate).*'    then 'EN'
            else 'MI'
        end                                                  as experience_level,

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
