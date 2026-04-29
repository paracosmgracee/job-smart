-- Job demand and salary by US state
-- Extracts state code from "City, ST" format location strings
with jobs as (
    select
        job_id,
        job_title,
        location,
        annual_salary_est,
        experience_level,
        work_type,
        remote_allowed,
        case
            when lower(job_title) like '%data engineer%'    then 'Data Engineer'
            when lower(job_title) like '%data scientist%'   then 'Data Scientist'
            when lower(job_title) like '%data analyst%'     then 'Data Analyst'
            when lower(job_title) like '%machine learning%' then 'ML Engineer'
            when lower(job_title) like '%ai engineer%'
              or lower(job_title) like '%llm%'              then 'AI/LLM Engineer'
            when lower(job_title) like '%software engineer%' then 'Software Engineer'
            when lower(job_title) like '%analytics engineer%' then 'Analytics Engineer'
            else null
        end as role_cluster
    from {{ ref('stg_all_postings') }}
    where location is not null
      and location like '%, %'
),

with_state as (
    select
        job_id,
        role_cluster,
        annual_salary_est,
        trim(split_part(location, ', ', -1))    as raw_state,
        -- normalize to 2-letter code
        case trim(split_part(location, ', ', -1))
            when 'Alabama' then 'AL' when 'Alaska' then 'AK'
            when 'Arizona' then 'AZ' when 'Arkansas' then 'AR'
            when 'California' then 'CA' when 'Colorado' then 'CO'
            when 'Connecticut' then 'CT' when 'Delaware' then 'DE'
            when 'Florida' then 'FL' when 'Georgia' then 'GA'
            when 'Hawaii' then 'HI' when 'Idaho' then 'ID'
            when 'Illinois' then 'IL' when 'Indiana' then 'IN'
            when 'Iowa' then 'IA' when 'Kansas' then 'KS'
            when 'Kentucky' then 'KY' when 'Louisiana' then 'LA'
            when 'Maine' then 'ME' when 'Maryland' then 'MD'
            when 'Massachusetts' then 'MA' when 'Michigan' then 'MI'
            when 'Minnesota' then 'MN' when 'Mississippi' then 'MS'
            when 'Missouri' then 'MO' when 'Montana' then 'MT'
            when 'Nebraska' then 'NE' when 'Nevada' then 'NV'
            when 'New Hampshire' then 'NH' when 'New Jersey' then 'NJ'
            when 'New Mexico' then 'NM' when 'New York' then 'NY'
            when 'North Carolina' then 'NC' when 'North Dakota' then 'ND'
            when 'Ohio' then 'OH' when 'Oklahoma' then 'OK'
            when 'Oregon' then 'OR' when 'Pennsylvania' then 'PA'
            when 'Rhode Island' then 'RI' when 'South Carolina' then 'SC'
            when 'South Dakota' then 'SD' when 'Tennessee' then 'TN'
            when 'Texas' then 'TX' when 'Utah' then 'UT'
            when 'Vermont' then 'VT' when 'Virginia' then 'VA'
            when 'Washington' then 'WA' when 'West Virginia' then 'WV'
            when 'Wisconsin' then 'WI' when 'Wyoming' then 'WY'
            when 'District of Columbia' then 'DC'
            else trim(split_part(location, ', ', -1))
        end as state_code
    from jobs
)

select
    state_code,
    count(distinct job_id)                          as job_count,
    round(median(annual_salary_est), 0)             as median_salary,
    round(avg(annual_salary_est), 0)                as avg_salary,
    count(case when role_cluster = 'Data Engineer'    then 1 end) as de_count,
    count(case when role_cluster = 'Data Scientist'   then 1 end) as ds_count,
    count(case when role_cluster = 'Data Analyst'     then 1 end) as da_count,
    count(case when role_cluster = 'Software Engineer' then 1 end) as swe_count,
    count(case when role_cluster = 'ML Engineer'      then 1 end) as ml_count
from with_state
where length(state_code) = 2
  and state_code rlike '[A-Z]{2}'
group by state_code
having job_count >= 10
order by job_count desc
