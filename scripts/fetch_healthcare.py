"""
Daily healthcare company job scanner via Serper.dev (Google Search API).
Runs site: queries for 15 target companies, filters by title, deduplicates,
and sends an email digest of new matching roles.

Secrets required (GitHub Actions / .env):
  SERPER_API_KEY       - from serper.dev (2,500 free searches at signup)
  GMAIL_USER           - sender Gmail address
  GMAIL_APP_PASSWORD   - Gmail App Password (Settings > Security > App passwords)
  RECIPIENT_EMAIL      - destination email (optional, defaults to GMAIL_USER)
"""
import os, json, smtplib, requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SERPER_API_KEY    = os.environ["SERPER_API_KEY"]
GMAIL_USER        = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT_EMAIL   = os.environ.get("RECIPIENT_EMAIL", GMAIL_USER)

SEEN_FILE = Path("data/healthcare_seen_urls.txt")

QUERIES = [
    ("Cedars-Sinai",          'site:cedars-sinai.org "Data Analyst" OR "Data Scientist" OR "Data Engineer" OR "Informatics"'),
    ("UCLA Health",           'site:uclahealth.org "Data Analyst" OR "Data Scientist" OR "Data Engineer" OR "Analytics"'),
    ("Kaiser Permanente",     'site:jobs.kaiserpermanente.org "Data Analyst" OR "Data Scientist" OR "Data Engineer" OR "Health Analytics"'),
    ("Providence Health",     'site:jobs.providence.org "Data Analyst" OR "Data Scientist" OR "Data Engineer" OR "Analytics"'),
    ("HCA Healthcare",        'site:careers.hcahealthcare.com "Data Analyst" OR "Data Scientist" OR "Data Engineer" OR "Analytics"'),
    ("Ascension Health",      'site:careers.ascension.org "Data Analyst" OR "Data Scientist" OR "Analytics" OR "Informatics"'),
    ("CommonSpirit Health",   'site:jobs.commonspirit.org "Data Analyst" OR "Data Scientist" OR "Analytics" OR "Informatics"'),
    ("Keck Medicine of USC",  'site:usccareers.usc.edu "Data Analyst" OR "Data Engineer" OR "Analytics"'),
    ("City of Hope",          'site:careers.cityofhope.org "Data Analyst" OR "Data Scientist" OR "Analytics"'),
    ("Children\'s Hospital LA", 'site:jobs.chla.org "Data Analyst" OR "Data Scientist" OR "Analytics" OR "Informatics"'),
    ("Blue Shield of CA",     'site:careers.blueshieldca.com "Data Analyst" OR "Data Scientist" OR "Data Engineer"'),
    ("Elevance Health",       'site:careers.elevancehealth.com "Data Analyst" OR "Data Scientist" OR "Data Engineer"'),
    ("Molina Healthcare",     'site:careers.molinahealthcare.com "Data Analyst" OR "Data Scientist" OR "Analytics"'),
    ("Optum / UnitedHealth",  'site:careers.unitedhealthgroup.com "Data Analyst" OR "Data Scientist" OR "Healthcare Analytics"'),
    ("Centene",               'site:jobs.centene.com "Data Analyst" OR "Data Scientist" OR "Analytics"'),
]

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


def clean_title(raw: str) -> str:
    for sep in [" | ", " - ", " – ", " — ", " at ", " @ "]:
        if sep in raw:
            return raw.split(sep)[0].strip()
    return raw.strip()


def search(query: str) -> list[dict]:
    r = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
        json={"q": query, "gl": "us", "num": 10},
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("organic", [])


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        lines = SEEN_FILE.read_text().strip().splitlines()
        return set(l.strip() for l in lines if l.strip())
    return set()


def save_seen(seen: set[str]):
    SEEN_FILE.parent.mkdir(exist_ok=True)
    SEEN_FILE.write_text("\n".join(sorted(seen)) + "\n")


def send_email(new_jobs: list[dict]):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not new_jobs:
        print("No new jobs today — skipping email")
        return

    by_company: dict[str, list] = {}
    for job in new_jobs:
        by_company.setdefault(job["company"], []).append(job)

    rows = ""
    for company, jobs in by_company.items():
        for job in jobs:
            rows += f"""
            <tr>
              <td style="padding:10px 12px;border-bottom:1px solid #2a2a3a;color:#a0a0b0;white-space:nowrap">{company}</td>
              <td style="padding:10px 12px;border-bottom:1px solid #2a2a3a">
                <a href="{job['url']}" style="color:#818cf8;text-decoration:none;font-weight:500">{job['title']}</a>
              </td>
            </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#0f0f1a;font-family:Inter,Arial,sans-serif">
  <div style="max-width:600px;margin:40px auto;background:#13132a;border-radius:12px;overflow:hidden">
    <div style="background:#1a1a3a;padding:24px 28px;border-bottom:1px solid #2a2a4a">
      <div style="font-size:11px;color:#6366f1;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px">job-smart · Healthcare Digest</div>
      <div style="font-size:22px;font-weight:700;color:#f0f0f8">{len(new_jobs)} new roles · {today}</div>
      <div style="font-size:13px;color:#6b6b8a;margin-top:4px">{len(by_company)} companies · Data Analyst / Data Scientist / Data Engineer</div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#1e1e3a">
          <th style="padding:10px 12px;text-align:left;font-size:11px;color:#6b6b8a;letter-spacing:0.05em;text-transform:uppercase">Company</th>
          <th style="padding:10px 12px;text-align:left;font-size:11px;color:#6b6b8a;letter-spacing:0.05em;text-transform:uppercase">Role</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <div style="padding:20px 28px;border-top:1px solid #2a2a3a">
      <p style="color:#4a4a6a;font-size:12px;margin:0">
        Filters: no Senior / Staff / Principal / Lead / Director / Manager.<br>
        Click any role to apply. Sent automatically by your job-smart pipeline.
      </p>
    </div>
  </div>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[job-smart] {len(new_jobs)} new healthcare roles · {today}"
    msg["From"]    = GMAIL_USER
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, RECIPIENT_EMAIL, msg.as_string())

    print(f"✓ Email sent to {RECIPIENT_EMAIL} · {len(new_jobs)} new jobs")


def main():
    seen = load_seen()
    new_jobs = []

    for company, query in QUERIES:
        print(f"Scanning: {company}")
        try:
            results = search(query)
            found = 0
            for r in results:
                url   = r.get("link", "").strip()
                title = clean_title(r.get("title", ""))
                if not url or url in seen:
                    continue
                if not title_matches(title):
                    continue
                new_jobs.append({"company": company, "title": title, "url": url})
                seen.add(url)
                found += 1
                print(f"  + {title}")
            if found == 0:
                print("  (no new matches)")
        except Exception as e:
            print(f"  Error: {e}")

    save_seen(seen)
    print(f"\nTotal new jobs: {len(new_jobs)}")
    send_email(new_jobs)


if __name__ == "__main__":
    main()
