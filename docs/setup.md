# Setup Guide

## Prerequisites
- Docker Desktop
- Python 3.11+
- Snowflake free trial account (30 days, no credit card needed)
- Kaggle account (free)
- Anthropic API key

## Step 1: Environment variables

```bash
cp .env.example .env
# Fill in your Snowflake, Kaggle, and Anthropic credentials
```

## Step 2: Download data

```bash
pip install kaggle python-dotenv
python scripts/download_data.py
```

This downloads ~100k LinkedIn job postings CSV from Kaggle.

## Step 3: Upload to Snowflake

```bash
pip install snowflake-connector-python pandas
python scripts/upload_to_snowflake.py
```

## Step 4: Run dbt transformations

```bash
pip install dbt-snowflake
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Step 5: Start Airflow (Docker)

```bash
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080 (admin / admin), enable the `job_market_pipeline` DAG.

## Step 6: Launch dashboard

```bash
pip install streamlit plotly
streamlit run dashboard/app.py
```

Open http://localhost:8501

## Snowflake Free Trial

Sign up at snowflake.com → Start for free.
Account format: `abc12345.us-east-1` (shown in the URL after login).
