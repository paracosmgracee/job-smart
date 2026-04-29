"""
Fetch job data from JSearch API (RapidAPI) — scrapes Indeed + LinkedIn.
Free tier: 200 requests/month. Run monthly for historical backfill,
or daily for ~6 queries/day to stay within limits.
"""
import os, time, requests, pandas as pd, snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY  = os.environ["JSEARCH_API_KEY"]
BASE     = "https://jsearch.p.rapidapi.com/search"
HEADERS  = {
    "X-RapidAPI-Key":  API_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

QUERIES = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "ai engineer",
    "analytics engineer",
    "llm engineer",
    "software engineer data",
]

PAGES_PER_QUERY = 3   # 10 results/page × 3 pages = 30 per query
                       # 8 queries × 3 pages = 24 requests/run — well within 200/month


def fetch_jobs(query: str, page: int) -> list[dict]:
    params = {
        "query":           f"{query} United States",
        "page":            str(page),
        "num_pages":       "1",
        "date_posted":     "month",   # last 30 days
        "country":         "us",
        "language":        "en",
    }
    r = requests.get(BASE, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("data", [])


def parse_job(job: dict, query: str) -> dict:
    # salary: JSearch returns structured min/max/period
    s_min  = job.get("job_min_salary")
    s_max  = job.get("job_max_salary")
    period = (job.get("job_salary_period") or "").upper()

    # annualize if hourly
    def annualize(v):
        if v is None:
            return None
        if period == "HOUR":
            return round(v * 2080, 0)
        if period in ("WEEK", "WEEKLY"):
            return round(v * 52, 0)
        if period in ("MONTH", "MONTHLY"):
            return round(v * 12, 0)
        return v

    s_min_a = annualize(s_min)
    s_max_a = annualize(s_max)
    s_est   = round((s_min_a + s_max_a) / 2, 0) if s_min_a and s_max_a else (s_min_a or s_max_a)

    return {
        "JOB_ID":      job.get("job_id"),
        "TITLE":       job.get("job_title"),
        "COMPANY":     job.get("employer_name"),
        "DESCRIPTION": job.get("job_description"),
        "LOCATION":    f"{job.get('job_city', '')}, {job.get('job_state', '')}".strip(", "),
        "STATE":       job.get("job_state"),
        "CITY":        job.get("job_city"),
        "SALARY_MIN":  s_min_a,
        "SALARY_MAX":  s_max_a,
        "SALARY_EST":  s_est,
        "REMOTE":      "1" if job.get("job_is_remote") else "0",
        "CATEGORY":    job.get("job_required_experience", {}).get("required_experience_in_months"),
        "URL":         job.get("job_apply_link") or job.get("job_google_link"),
        "POSTED_AT":   job.get("job_posted_at_datetime_utc"),
        "QUERY":       query,
        "FETCHED_AT":  datetime.now(timezone.utc).isoformat(),
        "SOURCE":      "jsearch",
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
                    print(f"  page {page}: no results, stopping")
                    break
                all_jobs.extend([parse_job(j, query) for j in jobs])
                print(f"  page {page}: {len(jobs)} jobs")
                time.sleep(1.0)
            except Exception as e:
                print(f"  page {page} error: {e}")
                break

    df = pd.DataFrame(all_jobs)
    df = df.drop_duplicates(subset=["JOB_ID"])
    print(f"\nTotal unique jobs: {len(df):,}")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.JSEARCH_POSTINGS (
                JOB_ID VARCHAR, TITLE VARCHAR, COMPANY VARCHAR,
                DESCRIPTION TEXT, LOCATION VARCHAR, STATE VARCHAR, CITY VARCHAR,
                SALARY_MIN FLOAT, SALARY_MAX FLOAT, SALARY_EST FLOAT,
                REMOTE VARCHAR, CATEGORY VARCHAR, URL VARCHAR,
                POSTED_AT VARCHAR, QUERY VARCHAR, FETCHED_AT VARCHAR, SOURCE VARCHAR
            )
        """)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur.execute(f"DELETE FROM RAW.JSEARCH_POSTINGS WHERE FETCHED_AT LIKE '{today}%'")

        success, _, n_rows, _ = write_pandas(conn, df, "JSEARCH_POSTINGS", overwrite=False)
        print(f"✓ {n_rows:,} rows inserted into RAW.JSEARCH_POSTINGS")


if __name__ == "__main__":
    main()
