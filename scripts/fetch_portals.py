"""
Fetch jobs from target companies via Greenhouse / Ashby public APIs.
Zero API keys needed — these endpoints are publicly accessible.
Source: career-ops/portals.yml (companies with api: field)
"""
import os, time, requests, yaml, hashlib, pandas as pd, snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

PORTALS_YML = os.path.expanduser("~/career-ops/portals.yml")

TITLE_POSITIVE = [
    "data analyst", "analytics analyst", "business analyst", "bi analyst",
    "business intelligence", "reporting analyst", "operations analyst",
    "product analyst", "healthcare data", "clinical data", "health informatics",
    "clinical informatics", "population health", "quality analyst",
    "data engineer", "analytics engineer", "etl", "data scientist",
    "applied scientist", "quantitative analyst",
]
TITLE_NEGATIVE = [
    "senior", "staff", "principal", "lead", "director", "vp", "head of",
    "manager", "research scientist", "ml researcher", "machine learning engineer",
    "deep learning", "computer vision", "nlp researcher",
    "ios", "android", "blockchain", "web3",
]


def title_matches(title: str) -> bool:
    t = title.lower()
    if any(neg in t for neg in TITLE_NEGATIVE):
        return False
    return any(pos in t for pos in TITLE_POSITIVE)


def fetch_greenhouse(slug: str, company: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        jobs = r.json().get("jobs", [])
        results = []
        for j in jobs:
            if not title_matches(j.get("title", "")):
                continue
            loc = j.get("location", {}).get("name", "")
            results.append({
                "JOB_ID":      f"gh_{j['id']}",
                "TITLE":       j.get("title"),
                "COMPANY":     company,
                "DESCRIPTION": None,
                "LOCATION":    loc,
                "STATE":       loc.split(", ")[-1] if ", " in loc else None,
                "SALARY_MIN":  None,
                "SALARY_MAX":  None,
                "SALARY_EST":  None,
                "REMOTE":      "1" if "remote" in loc.lower() else "0",
                "URL":         j.get("absolute_url"),
                "POSTED_AT":   j.get("updated_at"),
                "SOURCE":      "greenhouse",
            })
        return results
    except Exception as e:
        print(f"  {company} greenhouse error: {e}")
        return []


def fetch_ashby(slug: str, company: str) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        jobs = r.json().get("jobPostings", [])
        results = []
        for j in jobs:
            if not title_matches(j.get("title", "")):
                continue
            loc = j.get("location", "")
            results.append({
                "JOB_ID":      f"ashby_{j['id']}",
                "TITLE":       j.get("title"),
                "COMPANY":     company,
                "DESCRIPTION": j.get("descriptionPlain", "")[:2000] if j.get("descriptionPlain") else None,
                "LOCATION":    loc,
                "STATE":       loc.split(", ")[-1] if ", " in loc else None,
                "SALARY_MIN":  None,
                "SALARY_MAX":  None,
                "SALARY_EST":  None,
                "REMOTE":      "1" if j.get("isRemote") else "0",
                "URL":         j.get("jobUrl"),
                "POSTED_AT":   j.get("publishedAt"),
                "SOURCE":      "ashby",
            })
        return results
    except Exception as e:
        print(f"  {company} ashby error: {e}")
        return []


def load_targets() -> list[dict]:
    """Load companies with API endpoints from portals.yml."""
    try:
        with open(PORTALS_YML) as f:
            raw = f.read()
        # portals.yml has a non-standard structure — parse company blocks manually
        import re
        targets = []
        # Match entries with both name and api fields
        blocks = re.findall(
            r'- name: (.+?)\n.*?api: (https://[^\n]+)',
            raw, re.DOTALL
        )
        for name, api_url in blocks:
            name = name.strip()
            api_url = api_url.strip()
            if "greenhouse" in api_url:
                slug = api_url.rstrip("/").split("/")[-2] if "/jobs" in api_url else api_url.rstrip("/").split("/")[-1]
                targets.append({"company": name, "ats": "greenhouse", "slug": slug})
            elif "ashby" in api_url:
                slug = api_url.rstrip("/").split("/")[-1]
                targets.append({"company": name, "ats": "ashby", "slug": slug})
        return targets
    except Exception as e:
        print(f"Warning: could not load portals.yml ({e}), using hardcoded list")
        return HARDCODED_TARGETS


# Fallback if portals.yml not found
HARDCODED_TARGETS = [
    {"company": "Airbnb",       "ats": "greenhouse", "slug": "airbnb"},
    {"company": "DoorDash",     "ats": "greenhouse", "slug": "doordashusa"},
    {"company": "Stripe",       "ats": "greenhouse", "slug": "stripe"},
    {"company": "Lyft",         "ats": "greenhouse", "slug": "lyft"},
    {"company": "Databricks",   "ats": "greenhouse", "slug": "databricks"},
    {"company": "Figma",        "ats": "greenhouse", "slug": "figma"},
    {"company": "Notion",       "ats": "greenhouse", "slug": "notion"},
    {"company": "Plaid",        "ats": "greenhouse", "slug": "plaid"},
    {"company": "Chime",        "ats": "greenhouse", "slug": "chime"},
    {"company": "Ramp",         "ats": "ashby",      "slug": "ramp"},
    {"company": "Brex",         "ats": "ashby",      "slug": "brex"},
]


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
    targets = load_targets()
    print(f"Loaded {len(targets)} target companies from portals.yml")

    all_jobs = []
    for t in targets:
        print(f"Fetching: {t['company']} ({t['ats']})")
        if t["ats"] == "greenhouse":
            jobs = fetch_greenhouse(t["slug"], t["company"])
        elif t["ats"] == "ashby":
            jobs = fetch_ashby(t["slug"], t["company"])
        else:
            continue
        print(f"  → {len(jobs)} matching jobs")
        all_jobs.extend(jobs)
        time.sleep(0.3)

    if not all_jobs:
        print("No jobs found.")
        return

    fetched_at = datetime.now(timezone.utc).isoformat()
    df = pd.DataFrame(all_jobs)
    df["FETCHED_AT"] = fetched_at
    df["QUERY"] = "portal_scan"
    df = df.drop_duplicates(subset=["JOB_ID"]).reset_index(drop=True)
    print(f"\nTotal unique jobs: {len(df):,}")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.PORTAL_POSTINGS (
                JOB_ID VARCHAR, TITLE VARCHAR, COMPANY VARCHAR,
                DESCRIPTION TEXT, LOCATION VARCHAR, STATE VARCHAR,
                SALARY_MIN FLOAT, SALARY_MAX FLOAT, SALARY_EST FLOAT,
                REMOTE VARCHAR, URL VARCHAR,
                POSTED_AT VARCHAR, SOURCE VARCHAR,
                FETCHED_AT VARCHAR, QUERY VARCHAR
            )
        """)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur.execute(f"DELETE FROM RAW.PORTAL_POSTINGS WHERE FETCHED_AT LIKE '{today}%'")
        success, _, n_rows, _ = write_pandas(conn, df, "PORTAL_POSTINGS", overwrite=False)
        print(f"✓ {n_rows:,} rows inserted into RAW.PORTAL_POSTINGS")


if __name__ == "__main__":
    main()
