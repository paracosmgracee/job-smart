-- Remote vs Hybrid vs On-site trend over time (monthly %)
with jobs as (
    select
        date_trunc('month', listed_at)  as month,
        remote_allowed,
        work_type,
        job_id
    from {{ ref('stg_job_postings') }}
    where listed_at is not null
),

monthly as (
    select
        month,
        count(*)                                                        as total,
        count(case when remote_allowed = 'TRUE' then 1 end)            as remote_count,
        count(case when remote_allowed = 'FALSE' then 1 end)           as onsite_count,
        count(case when remote_allowed is null then 1 end)             as hybrid_count
    from jobs
    group by month
)

select
    month,
    total,
    remote_count,
    onsite_count,
    hybrid_count,
    round(100.0 * remote_count / nullif(total, 0), 1)  as remote_pct,
    round(100.0 * onsite_count / nullif(total, 0), 1)  as onsite_pct,
    round(100.0 * hybrid_count / nullif(total, 0), 1)  as hybrid_pct
from monthly
order by month
