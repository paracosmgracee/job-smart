"""
Fetch live job data from Adzuna API and append to Snowflake RAW.ADZUNA_POSTINGS
Pulls multiple role queries, deduplicates by job ID, then upserts.
"""
import os, time, requests, pandas as pd, snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

APP_ID  = os.environ["ADZUNA_APP_ID"]
APP_KEY = os.environ["ADZUNA_API_KEY"]
BASE    = "https://api.adzuna.com/v1/api/jobs/us/search"

QUERIES = [
    "data engineer", "data scientist", "data analyst",
    "machine learning engineer", "ai engineer", "software engineer",
    "analytics engineer", "business intelligence", "llm engineer",
]

PAGES_PER_QUERY = 10  # 50 results/page × 10 pages = 500 per role (Adzuna max)
RESULTS_PER_PAGE = 50
MAX_DAYS_OLD = 30     # pull postings up to 30 days old


def fetch_jobs(query: str, page: int) -> list[dict]:
    url = f"{BASE}/{page}"
    params = {
        "app_id": APP_ID, "app_key": APP_KEY,
        "results_per_page": RESULTS_PER_PAGE,
        "what": query,
        "where": "united states",
        "content-type": "application/json",
        "sort_by": "date",
        "max_days_old": MAX_DAYS_OLD,
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("results", [])


def parse_job(job: dict, query: str) -> dict:
    loc = job.get("location", {})
    area = loc.get("area", [])
    return {
        "JOB_ID":        job.get("id"),
        "TITLE":         job.get("title"),
        "COMPANY":       job.get("company", {}).get("display_name"),
        "DESCRIPTION":   job.get("description"),
        "LOCATION":      loc.get("display_name"),
        "STATE":         area[1] if len(area) > 1 else None,
        "CITY":          area[-1] if len(area) > 1 else None,
        "SALARY_MIN":    job.get("salary_min"),
        "SALARY_MAX":    job.get("salary_max"),
        "SALARY_EST":    (job.get("salary_min", 0) + job.get("salary_max", 0)) / 2 or None,
        "REMOTE":        None,
        "CATEGORY":      job.get("category", {}).get("label"),
        "URL":           job.get("redirect_url"),
        "POSTED_AT":     job.get("created"),
        "QUERY":         query,
        "FETCHED_AT":    datetime.now(timezone.utc).isoformat(),
        "SOURCE":        "adzuna",
    }


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )


def main():
    all_jobs = []

    for query in QUERIES:
        print(f"Fetching: {query}")
        for page in range(1, PAGES_PER_QUERY + 1):
            try:
                jobs = fetch_jobs(query, page)
                if not jobs:
                    break
                all_jobs.extend([parse_job(j, query) for j in jobs])
                print(f"  page {page}: {len(jobs)} jobs")
                time.sleep(0.5)   # respect rate limit
            except Exception as e:
                print(f"  page {page} error: {e}")
                break

    df = pd.DataFrame(all_jobs)
    df = df.drop_duplicates(subset=["JOB_ID"])
    print(f"\nTotal unique jobs fetched: {len(df):,}")

    print("Uploading to Snowflake RAW.ADZUNA_POSTINGS ...")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.ADZUNA_POSTINGS (
                JOB_ID VARCHAR, TITLE VARCHAR, COMPANY VARCHAR,
                DESCRIPTION TEXT, LOCATION VARCHAR, STATE VARCHAR, CITY VARCHAR,
                SALARY_MIN FLOAT, SALARY_MAX FLOAT, SALARY_EST FLOAT,
                REMOTE VARCHAR, CATEGORY VARCHAR, URL VARCHAR,
                POSTED_AT VARCHAR, QUERY VARCHAR, FETCHED_AT VARCHAR, SOURCE VARCHAR
            )
        """)
        # Remove today's fetch to avoid dupes on re-run
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur.execute(f"DELETE FROM RAW.ADZUNA_POSTINGS WHERE FETCHED_AT LIKE '{today}%'")

        success, _, n_rows, _ = write_pandas(conn, df, "ADZUNA_POSTINGS", overwrite=False)
        print(f"✓ {n_rows:,} rows inserted")


if __name__ == "__main__":
    main()
