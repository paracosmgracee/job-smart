with source as (
    select * from {{ source('raw', 'COMPANIES') }}
)

select
    COMPANY_ID::varchar      as company_id,
    trim(NAME)               as company_name,
    trim(DESCRIPTION)        as company_description,
    trim(COMPANY_SIZE)       as company_size,
    trim(STATE)              as state,
    trim(COUNTRY)            as country,
    trim(URL)                as website_url,
    try_cast(FOLLOWER_COUNT as int) as follower_count,
    try_cast(EMPLOYEE_COUNT as int) as employee_count,
    current_timestamp()      as _loaded_at
from source
where COMPANY_ID is not null
