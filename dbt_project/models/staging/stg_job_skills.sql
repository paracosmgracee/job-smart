-- Exploded skill tags per job posting
with source as (
    select * from {{ source('raw', 'JOB_SKILLS') }}
)

select
    JOB_ID::varchar        as job_id,
    lower(trim(SKILL_ABR)) as skill_id,
    current_timestamp()    as _loaded_at
from source
where JOB_ID is not null
  and SKILL_ABR is not null
