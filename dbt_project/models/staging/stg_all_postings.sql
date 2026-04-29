-- Live Adzuna job postings (2025-2026), fetched daily
-- Kaggle 2023-2024 snapshot removed: after tech-role filtering it contributed <600 rows
-- and skewed all statistics toward stale market conditions
select * from {{ ref('stg_adzuna_postings') }}
