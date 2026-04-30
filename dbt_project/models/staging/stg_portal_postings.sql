-- Target company job postings via Greenhouse/Ashby public APIs (zero API key)
with source as (
    select * from {{ source('raw', 'PORTAL_POSTINGS') }}
),

cleaned as (
    select
        JOB_ID::varchar                                      as job_id,
        trim(COMPANY)                                        as company_name,
        trim(TITLE)                                          as job_title,
        trim(DESCRIPTION)                                    as description,
        trim(LOCATION)                                       as location,
        trim(STATE)                                          as state_code,
        null::float                                          as min_salary,
        null::float                                          as max_salary,
        null::float                                          as med_salary,
        'YEARLY'                                             as pay_period,

        case
            when lower(DESCRIPTION) rlike '.*(contract|contractor|freelance|1099|c2c).*' then 'CONTRACT'
            when lower(DESCRIPTION) rlike '.*(part[ -]time).*'                           then 'PART_TIME'
            else 'FULL_TIME'
        end                                                  as work_type,

        case
            when REMOTE = '1'                                                             then '1'
            when lower(LOCATION) like '%remote%'                                          then '1'
            else '0'
        end                                                  as remote_allowed,

        null::int                                            as views,
        null::int                                            as applies,

        case
            when lower(TITLE) rlike '.*(vp |vice president|director|head of|chief).*'  then 'EX'
            when lower(TITLE) rlike '.*(staff |principal |lead ).*'                     then 'DI'
            when lower(TITLE) rlike '.*(senior|sr\\.? |sr-).*'                          then 'SE'
            when lower(TITLE) rlike '.*(junior|jr\\.? |jr-|entry.level|associate).*'   then 'EN'
            else 'MI'
        end                                                  as experience_level,

        try_to_timestamp(POSTED_AT)                          as listed_at,
        null::timestamp                                      as expires_at,
        null::float                                          as annual_salary_est,
        QUERY                                                as search_query,
        SOURCE                                               as source,
        try_to_timestamp(FETCHED_AT)                         as _loaded_at
    from source
    where JOB_ID is not null
      and TITLE is not null
)

select * from cleaned
