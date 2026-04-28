-- Cleaned and typed view over raw job postings
with source as (
    select * from {{ source('raw', 'JOB_POSTINGS') }}
),

cleaned as (
    select
        JOB_ID::varchar                                      as job_id,
        trim(COMPANY_NAME)                                   as company_name,
        trim(TITLE)                                          as job_title,
        trim(DESCRIPTION)                                    as description,
        trim(LOCATION)                                       as location,
        MIN_SALARY::float                                    as min_salary,
        MAX_SALARY::float                                    as max_salary,
        MED_SALARY::float                                    as med_salary,
        upper(trim(PAY_PERIOD))                              as pay_period,   -- HOURLY / MONTHLY / YEARLY
        upper(trim(FORMATTED_WORK_TYPE))                     as work_type,    -- Full-time / Part-time / Contract
        upper(trim(REMOTE_ALLOWED))                          as remote_allowed,
        VIEWS::int                                           as views,
        APPLIES::int                                         as applies,
        FORMATTED_EXPERIENCE_LEVEL::varchar                  as experience_level,
        try_to_timestamp(ORIGINAL_LISTED_TIME::varchar)       as listed_at,
        try_to_timestamp(EXPIRY::varchar)                    as expires_at,
        -- normalize salary to annual
        case pay_period
            when 'HOURLY'   then med_salary * 2080
            when 'MONTHLY'  then med_salary * 12
            else med_salary
        end                                                  as annual_salary_est,
        current_timestamp()                                  as _loaded_at
    from source
    where JOB_ID is not null
      and TITLE is not null
)

select * from cleaned
