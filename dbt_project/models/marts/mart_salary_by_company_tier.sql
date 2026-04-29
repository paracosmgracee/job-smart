-- Median salary by company tier (FAANG+ / Scale-up / Enterprise / Mid-market / Startup)
with classified as (
    select
        annual_salary_est,
        case
            when lower(company_name) rlike
                '.*(google|meta|facebook|amazon|apple|netflix|microsoft|openai|anthropic|deepmind|nvidia|bytedance|tiktok).*'
                then 'FAANG+'
            when lower(company_name) rlike
                '.*(stripe|databricks|snowflake|airbnb|doordash|instacart|figma|notion|linear|vercel|hugging face|cohere|mistral|scale ai|anyscale|weights.*biases|confluent|dbt labs|fivetran|airbyte|airtable|asana|canva|miro|retool).*'
                then 'Scale-up'
            when lower(company_name) rlike
                '.*(oracle|sap|ibm|salesforce|servicenow|workday|dell|hp|intel|cisco|vmware|adobe|autodesk|intuit|paypal|ebay|twitter|x corp|linkedin|uber|lyft|robinhood|coinbase|palantir|datadog|splunk|elastic|mongodb|cloudflare|okta|zendesk|twilio|sendgrid|box|dropbox|hubspot|atlassian|gitlab|github).*'
                then 'Enterprise'
            when company_name is not null and length(company_name) > 2
                then 'Startup / Other'
            else 'Unknown'
        end as company_tier
    from {{ ref('stg_all_postings') }}
    where annual_salary_est is not null
      and annual_salary_est between 30000 and 500000
),

tiered as (
    select
        company_tier,
        count(*)                                as job_count,
        round(median(annual_salary_est), 0)     as median_salary,
        round(avg(annual_salary_est), 0)        as avg_salary,
        round(percentile_cont(0.25) within group (order by annual_salary_est), 0) as p25_salary,
        round(percentile_cont(0.75) within group (order by annual_salary_est), 0) as p75_salary
    from classified
    where company_tier != 'Unknown'
    group by company_tier
)

select *,
    case company_tier
        when 'FAANG+'           then 1
        when 'Scale-up'         then 2
        when 'Enterprise'       then 3
        when 'Startup / Other'  then 4
    end as sort_order
from tiered
order by sort_order
