"""
Upload raw CSVs to Snowflake RAW schema.
Run once after download_data.py.
"""
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# Actual file paths from the Kaggle dataset structure
TABLES = {
    "postings.csv": "JOB_POSTINGS",
    "jobs/job_skills.csv": "JOB_SKILLS",
    "companies/companies.csv": "COMPANIES",
    "jobs/salaries.csv": "JOB_SALARIES",
    "jobs/job_industries.csv": "JOB_INDUSTRIES",
}

# Column normalization — Snowflake prefers UPPER_SNAKE
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )


def upload_table(conn, csv_name: str, table_name: str):
    path = RAW_DIR / csv_name
    if not path.exists():
        print(f"  SKIP {csv_name} — not found")
        return

    print(f"  Loading {csv_name} → RAW.{table_name} ...")
    df = pd.read_csv(path, low_memory=False)
    df = normalize_columns(df)

    success, n_chunks, n_rows, _ = write_pandas(
        conn, df, table_name, auto_create_table=True, overwrite=True
    )
    print(f"  ✓ {n_rows:,} rows → {table_name}")


def main():
    print("Connecting to Snowflake ...")
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        cur.execute("CREATE SCHEMA IF NOT EXISTS STAGING")
        cur.execute("CREATE SCHEMA IF NOT EXISTS MARTS")
        for csv_name, table_name in TABLES.items():
            upload_table(conn, csv_name, table_name)
    print("Done.")


if __name__ == "__main__":
    main()
