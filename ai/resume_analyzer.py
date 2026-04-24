"""
Resume/JD skill extraction and gap analysis using Claude API.
Usage:
    from ai.resume_analyzer import analyze_resume, match_jobs, skill_gap
"""
import os
import json
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

client = Anthropic()

EXTRACT_SYSTEM = """You are a technical recruiter expert. Extract structured data from resumes and job descriptions.
Always respond with valid JSON only — no markdown, no explanation."""

MATCH_SYSTEM = """You are a career advisor. Given a candidate's skill profile and job market data,
provide actionable analysis. Respond in valid JSON only."""


def extract_skills_from_resume(resume_text: str) -> dict:
    """
    Extract skills, experience, and profile from a resume.
    Returns structured dict with: skills, years_exp, roles, education, highlights
    """
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=EXTRACT_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": f"""Extract from this resume:
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
{resume_text}""",
            }
        ],
    )
    return json.loads(response.content[0].text)


def extract_skills_from_jd(jd_text: str) -> dict:
    """
    Extract required skills and metadata from a job description.
    Returns: required_skills, preferred_skills, role, seniority, salary_range
    """
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=EXTRACT_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": f"""Extract from this job description:
{{
  "role": "job title",
  "company": "company name",
  "seniority": "entry|mid|senior|lead",
  "required_skills": ["must-have skills"],
  "preferred_skills": ["nice-to-have skills"],
  "salary_range": {{"min": null, "max": null, "currency": "USD"}},
  "remote": true|false|"hybrid",
  "responsibilities": ["top 5 responsibilities"]
}}

Job description:
{jd_text}""",
            }
        ],
    )
    return json.loads(response.content[0].text)


def skill_gap_analysis(resume_profile: dict, jd_profile: dict, market_top_skills: list[str]) -> dict:
    """
    Compare candidate skills against JD requirements and market demand.
    Returns gap analysis with learning priority recommendations.
    """
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=MATCH_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": f"""Analyze skill gap between candidate and target role.

Candidate profile:
{json.dumps(resume_profile, indent=2)}

Target role requirements:
{json.dumps(jd_profile, indent=2)}

Top 20 market skills by demand:
{market_top_skills}

Return:
{{
  "match_score": <0.0-1.0>,
  "match_summary": "one sentence summary",
  "matched_skills": ["skills candidate has that JD requires"],
  "missing_critical": ["required skills candidate lacks — must learn"],
  "missing_preferred": ["nice-to-have gaps"],
  "strengths": ["candidate advantages for this role"],
  "learning_path": [
    {{"skill": "...", "priority": "high|medium|low", "reason": "...", "resource": "suggested course/cert"}}
  ],
  "application_recommendation": "strong|moderate|weak",
  "tailoring_tips": ["top 3 ways to tailor resume for this JD"]
}}""",
            }
        ],
    )
    return json.loads(response.content[0].text)


def generate_learning_path(candidate_skills: list[str], target_role: str, market_top_skills: list[str]) -> dict:
    """
    Generate personalized learning roadmap for a target role.
    """
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=MATCH_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": f"""Generate a 90-day learning roadmap.

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
}}""",
            }
        ],
    )
    return json.loads(response.content[0].text)
