"""
Job Market Intelligence Dashboard
Run: streamlit run dashboard/app.py
"""
import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Job Market Intelligence",
    page_icon="📊",
    layout="wide",
)

# ── Snowflake connection (cached) ──────────────────────────────────────────
@st.cache_resource
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="MARTS",
    )


@st.cache_data(ttl=3600)
def query(_conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _conn)


# ── Sidebar navigation ─────────────────────────────────────────────────────
st.sidebar.title("Job Market Intel")
page = st.sidebar.radio(
    "Navigate",
    ["📈 Market Overview", "🔧 Skill Demand", "💰 Salary Analysis", "🤖 Resume Analyzer"],
)

# ── Page: Market Overview ──────────────────────────────────────────────────
if page == "📈 Market Overview":
    st.title("📈 Tech Job Market Overview")

    conn = get_snowflake_conn()
    trends_df = query(conn, "SELECT * FROM MARTS.MART_HIRING_TRENDS ORDER BY MONTH, ROLE_CLUSTER")

    if trends_df.empty:
        st.warning("No data yet. Run the pipeline first.")
    else:
        # KPI row
        total_postings = int(trends_df["POSTING_COUNT"].sum())
        roles = trends_df["ROLE_CLUSTER"].nunique()
        latest_month = trends_df["MONTH"].max()

        k1, k2, k3 = st.columns(3)
        k1.metric("Total Postings Tracked", f"{total_postings:,}")
        k2.metric("Role Categories", roles)
        k3.metric("Data Through", str(latest_month)[:7])

        st.subheader("Monthly Hiring Trends by Role")
        fig = px.line(
            trends_df,
            x="MONTH",
            y="POSTING_COUNT",
            color="ROLE_CLUSTER",
            title="Job Posting Volume Over Time",
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Page: Skill Demand ─────────────────────────────────────────────────────
elif page == "🔧 Skill Demand":
    st.title("🔧 Skill Demand Analysis")

    conn = get_snowflake_conn()
    skills_df = query(conn, "SELECT * FROM MARTS.MART_SKILL_DEMAND ORDER BY DEMAND_RANK LIMIT 50")

    if skills_df.empty:
        st.warning("No data yet.")
    else:
        top_n = st.slider("Show top N skills", 10, 50, 25)
        df = skills_df.head(top_n)

        fig = px.bar(
            df,
            x="SKILL_ID",
            y="JOB_COUNT",
            color="MEDIAN_SALARY",
            color_continuous_scale="Blues",
            title=f"Top {top_n} Most In-Demand Skills",
            labels={"SKILL_ID": "Skill", "JOB_COUNT": "# Job Postings", "MEDIAN_SALARY": "Median Salary"},
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Skill Detail Table")
        st.dataframe(
            df[["DEMAND_RANK", "SKILL_ID", "JOB_COUNT", "MEDIAN_SALARY", "SENIOR_PCT"]].rename(
                columns={
                    "DEMAND_RANK": "Rank",
                    "SKILL_ID": "Skill",
                    "JOB_COUNT": "# Jobs",
                    "MEDIAN_SALARY": "Median Salary",
                    "SENIOR_PCT": "Senior %",
                }
            ),
            use_container_width=True,
        )

# ── Page: Salary Analysis ──────────────────────────────────────────────────
elif page == "💰 Salary Analysis":
    st.title("💰 Salary Analysis by Role")

    conn = get_snowflake_conn()
    salary_df = query(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE ORDER BY MEDIAN_SALARY DESC")

    if salary_df.empty:
        st.warning("No data yet.")
    else:
        fig = go.Figure()
        for _, row in salary_df.iterrows():
            fig.add_trace(go.Box(
                name=row["ROLE_CLUSTER"],
                q1=[row["P25_SALARY"]],
                median=[row["MEDIAN_SALARY"]],
                q3=[row["P75_SALARY"]],
                lowerfence=[row["P10_SALARY"]],
                upperfence=[row["P90_SALARY"]],
                mean=[row["AVG_SALARY"]],
                boxmean=True,
            ))

        fig.update_layout(
            title="Salary Distribution by Role (Annual USD)",
            yaxis_title="Annual Salary (USD)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            salary_df[["ROLE_CLUSTER", "POSTING_COUNT", "MEDIAN_SALARY", "P25_SALARY", "P75_SALARY"]],
            use_container_width=True,
        )

# ── Page: Resume Analyzer ──────────────────────────────────────────────────
elif page == "🤖 Resume Analyzer":
    st.title("🤖 AI Resume Analyzer")

    tab1, tab2 = st.tabs(["Resume vs. JD Match", "Learning Path Generator"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            resume_text = st.text_area("Paste your resume", height=300, placeholder="Paste resume text here...")
        with col2:
            jd_text = st.text_area("Paste job description", height=300, placeholder="Paste JD here...")

        if st.button("Analyze Match", type="primary") and resume_text and jd_text:
            from ai.resume_analyzer import extract_skills_from_resume, extract_skills_from_jd, skill_gap_analysis

            with st.spinner("Analyzing with Claude..."):
                conn = get_snowflake_conn()
                top_skills = query(conn, "SELECT SKILL_ID FROM MARTS.MART_SKILL_DEMAND ORDER BY DEMAND_RANK LIMIT 20")
                market_skills = top_skills["SKILL_ID"].tolist() if not top_skills.empty else []

                resume_profile = extract_skills_from_resume(resume_text)
                jd_profile = extract_skills_from_jd(jd_text)
                gap = skill_gap_analysis(resume_profile, jd_profile, market_skills)

            score = gap.get("match_score", 0)
            color = "green" if score >= 0.7 else "orange" if score >= 0.5 else "red"
            st.markdown(f"### Match Score: :{color}[{score:.0%}]")
            st.write(gap.get("match_summary", ""))

            c1, c2 = st.columns(2)
            with c1:
                st.success("**Matched Skills**")
                for s in gap.get("matched_skills", []):
                    st.write(f"✓ {s}")
            with c2:
                st.error("**Critical Gaps**")
                for s in gap.get("missing_critical", []):
                    st.write(f"✗ {s}")

            st.subheader("Learning Path")
            for item in gap.get("learning_path", []):
                st.markdown(f"**{item['skill']}** ({item['priority']} priority) — {item['reason']}")
                st.caption(f"Resource: {item.get('resource', 'TBD')}")

    with tab2:
        skills_input = st.text_input("Your current skills (comma-separated)", placeholder="Python, SQL, Tableau...")
        target_role = st.selectbox(
            "Target role",
            ["Data Engineer", "Data Scientist", "Data Analyst", "ML Engineer", "AI Engineer"],
        )

        if st.button("Generate Roadmap", type="primary") and skills_input:
            from ai.resume_analyzer import generate_learning_path

            conn = get_snowflake_conn()
            top_skills = query(conn, "SELECT SKILL_ID FROM MARTS.MART_SKILL_DEMAND ORDER BY DEMAND_RANK LIMIT 20")
            market_skills = top_skills["SKILL_ID"].tolist() if not top_skills.empty else []

            with st.spinner("Building your learning roadmap..."):
                result = generate_learning_path(
                    [s.strip() for s in skills_input.split(",")],
                    target_role,
                    market_skills,
                )

            st.metric("Job Readiness", f"{result.get('readiness_score', 0)}%")
            st.write(f"Estimated weeks to job-ready: **{result.get('weeks_to_job_ready', '?')}**")

            for step in result.get("roadmap", []):
                with st.expander(f"Week {step['week_range']}: {step['focus']}"):
                    st.write("**Skills:**", ", ".join(step.get("skills", [])))
                    st.write("**Resources:**")
                    for r in step.get("resources", []):
                        st.write(f"  - {r}")
                    st.success(f"Milestone: {step.get('milestone', '')}")
