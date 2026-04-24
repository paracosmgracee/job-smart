"""
Download job market dataset from Kaggle.
Dataset: arshkon/linkedin-job-postings (~100k postings)
"""
import os
import zipfile
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

DATASET = "arshkon/linkedin-job-postings"
TARGET_FILES = ["job_postings.csv", "skills.csv", "companies.csv"]


def download():
    print(f"Downloading {DATASET} ...")
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", DATASET, "-p", str(RAW_DIR), "--unzip"],
        check=True,
    )
    downloaded = list(RAW_DIR.iterdir())
    print(f"Files in {RAW_DIR}:")
    for f in downloaded:
        size_mb = f.stat().st_size / 1_048_576
        print(f"  {f.name} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    download()
