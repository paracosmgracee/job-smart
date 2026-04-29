"""
Resume/JD skill extraction and gap analysis using Google Gemini API (free tier).
Free tier: 1,500 req/day, no credit card needed — aistudio.google.com
"""
import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))
_model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config={"response_mime_type": "application/json"},
)

EXTRACT_SYSTEM = (
    "You are a technical recruiter expert. Extract structured data from resumes and job descriptions. "
    "Always respond with valid JSON only — no markdown, no explanation."
)
MATCH_SYSTEM = (
    "You are a career advisor. Given a candidate's skill profile and job market data, "
    "provide actionable analysis. Respond in valid JSON only."
)


def _call(system: str, user: str) -> dict:
    response = _model.generate_content(f"{system}\n\n{user}")
    return json.loads(response.text)


def extract_skills_from_resume(resume_text: str) -> dict:
    return _call(EXTRACT_SYSTEM, f"""Extract from this resume:
{{
  "skills": ["list of technical skills"],
  "soft_skills": ["list of soft skills"],
  "years_experience": <number>,
  "seniority": "entry|mid|senior|lead",
  "current_role": "current or most recent title",
  "target_roles": ["inferred target roles"],
  "education": {{"degree": "...", "field": "...", "school": "..."}},
  "highlights": ["top 3 quantified achievements"]
}}

Resume:
{resume_text}""")


def extract_skills_from_jd(jd_text: str) -> dict:
    return _call(EXTRACT_SYSTEM, f"""Extract from this job description:
{{
  "role": "job title",
  "company": "company name",
  "seniority": "entry|mid|senior|lead",
  "required_skills": ["must-have skills"],
  "preferred_skills": ["nice-to-have skills"],
  "salary_range": {{"min": null, "max": null, "currency": "USD"}},
  "remote": true,
  "responsibilities": ["top 5 responsibilities"]
}}

Job description:
{jd_text}""")


def skill_gap_analysis(resume_profile: dict, jd_profile: dict, market_top_skills: list) -> dict:
    return _call(MATCH_SYSTEM, f"""Analyze skill gap between candidate and target role.

Candidate profile:
{json.dumps(resume_profile, indent=2)}

Target role requirements:
{json.dumps(jd_profile, indent=2)}

Top market skills by demand:
{market_top_skills}

Return:
{{
  "match_score": <0.0-1.0>,
  "match_summary": "one sentence summary",
  "matched_skills": ["skills candidate has that JD requires"],
  "missing_critical": ["required skills candidate lacks"],
  "missing_preferred": ["nice-to-have gaps"],
  "strengths": ["candidate advantages for this role"],
  "learning_path": [
    {{"skill": "...", "priority": "high|medium|low", "reason": "...", "resource": "suggested course/cert"}}
  ],
  "application_recommendation": "strong|moderate|weak",
  "tailoring_tips": ["top 3 ways to tailor resume for this JD"]
}}""")


def generate_learning_path(candidate_skills: list, target_role: str, market_top_skills: list) -> dict:
    return _call(MATCH_SYSTEM, f"""Generate a 90-day learning roadmap.

Candidate current skills: {candidate_skills}
Target role: {target_role}
Market high-demand skills: {market_top_skills[:20]}

Return:
{{
  "target_role": "{target_role}",
  "readiness_score": <0-100>,
  "weeks_to_job_ready": <estimate>,
  "roadmap": [
    {{
      "week_range": "1-2",
      "focus": "theme",
      "skills": ["skill1", "skill2"],
      "resources": ["free course/project/book"],
      "milestone": "what you can build/show"
    }}
  ],
  "portfolio_projects": [
    {{"title": "...", "skills_demonstrated": ["..."], "description": "..."}}
  ]
}}""")
