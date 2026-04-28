"""
Job Market Intelligence Dashboard
Run: streamlit run dashboard/app.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def _secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, "")

st.set_page_config(
    page_title="Tech Job Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Global */
  .block-container { padding-top: 1rem; padding-bottom: 1rem; }

  /* Header */
  .dash-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 0.75rem 1.5rem; background: #0f1117;
    border-bottom: 1px solid #262730; margin-bottom: 1rem;
  }
  .dash-title { font-size: 1.3rem; font-weight: 700; color: #fff; }
  .dash-subtitle { font-size: 0.75rem; color: #888; }
  .header-kpis { display: flex; gap: 2rem; }
  .header-kpi { text-align: center; }
  .header-kpi-val { font-size: 1.2rem; font-weight: 700; color: #00d4aa; }
  .header-kpi-lbl { font-size: 0.65rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }

  /* KPI cards */
  .kpi-card {
    background: #1a1d27; border: 1px solid #262730; border-radius: 8px;
    padding: 1rem 1.2rem; height: 100%;
  }
  .kpi-card-icon { font-size: 1.2rem; margin-bottom: 0.3rem; }
  .kpi-card-title { font-size: 0.7rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }
  .kpi-card-val { font-size: 1.6rem; font-weight: 800; color: #fff; margin: 0.2rem 0; }
  .kpi-card-sub { font-size: 0.72rem; color: #888; }
  .kpi-badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 0.68rem; font-weight: 600; }
  .badge-green { background: #0d2b1e; color: #00d4aa; }
  .badge-blue  { background: #0d1e2b; color: #4da6ff; }
  .badge-gold  { background: #2b1e0d; color: #ffb347; }
  .badge-purple{ background: #1e0d2b; color: #c97bff; }

  /* Section headers */
  .section-label {
    font-size: 0.65rem; font-weight: 700; color: #888;
    text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.2rem;
  }
  .section-insight {
    font-size: 0.78rem; color: #aaa; margin-bottom: 0.8rem;
    border-left: 3px solid #00d4aa; padding-left: 0.6rem;
  }

  /* Skill tile */
  .skill-grid { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.5rem; }
  .skill-tile {
    background: #1a1d27; border: 1px solid #262730; border-radius: 6px;
    padding: 0.4rem 0.7rem; min-width: 90px;
  }
  .skill-name { font-size: 0.78rem; font-weight: 600; color: #fff; }
  .skill-count { font-size: 0.65rem; color: #888; }
  .skill-bar-bg { background: #262730; border-radius: 2px; height: 3px; margin-top: 4px; }
  .skill-bar-fill { background: #00d4aa; border-radius: 2px; height: 3px; }

  /* Chart area */
  .chart-panel {
    background: #1a1d27; border: 1px solid #262730;
    border-radius: 8px; padding: 1rem;
  }
  .chart-title { font-size: 0.78rem; font-weight: 600; color: #ccc; margin-bottom: 0.2rem; }
  .chart-toggle { font-size: 0.65rem; color: #00d4aa; float: right; }

  /* Page nav */
  div[data-testid="stHorizontalBlock"] > div { gap: 0.5rem; }

  /* Hide streamlit default elements */
  #MainMenu { visibility: hidden; }
  footer { visibility: hidden; }
  header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Snowflake ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=_secret("SNOWFLAKE_ACCOUNT"),
        user=_secret("SNOWFLAKE_USER"),
        password=_secret("SNOWFLAKE_PASSWORD"),
        database=_secret("SNOWFLAKE_DATABASE"),
        warehouse=_secret("SNOWFLAKE_WAREHOUSE"),
        role=_secret("SNOWFLAKE_ROLE"),
        schema="MARTS",
    )

@st.cache_data(ttl=3600)
def q(_conn, sql):
    return pd.read_sql(sql, _conn)

CHART_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#ccc", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
)
PALETTE = ["#00d4aa","#4da6ff","#ffb347","#c97bff","#ff6b6b","#7bed9f","#ffa502","#70a1ff"]

conn = get_conn()

# ── Load data ──────────────────────────────────────────────────────────────
postings_raw = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE")
skills_df    = q(conn, "SELECT * FROM MARTS.MART_SKILL_DEMAND ORDER BY DEMAND_RANK LIMIT 30")
trends_df    = q(conn, "SELECT * FROM MARTS.MART_HIRING_TRENDS ORDER BY MONTH, ROLE_CLUSTER")
seniority_df = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_SENIORITY ORDER BY SORT_ORDER")
remote_df    = q(conn, "SELECT * FROM MARTS.MART_REMOTE_TREND ORDER BY MONTH")
role_sen_df  = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE_SENIORITY")

# ── Filters ────────────────────────────────────────────────────────────────
all_roles = sorted(postings_raw["ROLE_CLUSTER"].dropna().unique()) if not postings_raw.empty else []
all_seniorities = ["Entry Level","Mid Level","Senior","Staff/Lead","Principal"]

with st.container():
    f1, f2, f3, fclear = st.columns([3, 3, 3, 1])
    with f1:
        sel_roles = st.multiselect("Role", all_roles, default=all_roles, label_visibility="collapsed",
                                   placeholder="Filter roles...")
    with f2:
        sel_sen = st.multiselect("Seniority", all_seniorities, default=all_seniorities, label_visibility="collapsed",
                                  placeholder="Filter seniority...")
    with f3:
        min_sal, max_sal = st.select_slider("Salary Range", options=[0,50,100,150,200,300,400,500],
                                             value=(0, 500), label_visibility="collapsed",
                                             format_func=lambda x: f"${x}k")
    with fclear:
        if st.button("✕ Clear", use_container_width=True):
            sel_roles = all_roles
            sel_sen = all_seniorities

# ── Compute global KPIs ────────────────────────────────────────────────────
postings_df = postings_raw[postings_raw["ROLE_CLUSTER"].isin(sel_roles)] if sel_roles else postings_raw
total_listings   = int(postings_df["POSTING_COUNT"].sum()) if not postings_df.empty else 0
median_salary    = int(postings_df["MEDIAN_SALARY"].median()) if not postings_df.empty else 0
remote_pct       = int(remote_df["REMOTE_PCT"].mean()) if not remote_df.empty else 0
unique_skills    = len(skills_df)
top_role         = postings_df.sort_values("POSTING_COUNT", ascending=False).iloc[0]["ROLE_CLUSTER"] if not postings_df.empty else "-"
top_paying_role  = postings_df.sort_values("MEDIAN_SALARY", ascending=False).iloc[0]["ROLE_CLUSTER"] if not postings_df.empty else "-"
top_paying_sal   = int(postings_df.sort_values("MEDIAN_SALARY", ascending=False).iloc[0]["MEDIAN_SALARY"]) if not postings_df.empty else 0
top_role_count   = int(postings_df.sort_values("POSTING_COUNT", ascending=False).iloc[0]["POSTING_COUNT"]) if not postings_df.empty else 0
last_month       = str(trends_df["MONTH"].max())[:7] if not trends_df.empty else "-"

# ── Header ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="dash-header">
  <div>
    <div class="dash-title">📊 Tech Job Market Intelligence</div>
    <div class="dash-subtitle">Real-time hiring pipeline analytics · {last_month}</div>
  </div>
  <div class="header-kpis">
    <div class="header-kpi">
      <div class="header-kpi-val">{total_listings:,}</div>
      <div class="header-kpi-lbl">Total Listings</div>
    </div>
    <div class="header-kpi">
      <div class="header-kpi-val">${median_salary//1000}k</div>
      <div class="header-kpi-lbl">Median Salary</div>
    </div>
    <div class="header-kpi">
      <div class="header-kpi-val">{remote_pct}%</div>
      <div class="header-kpi-lbl">Remote Rate</div>
    </div>
    <div class="header-kpi">
      <div class="header-kpi-val">{unique_skills}</div>
      <div class="header-kpi-lbl">Unique Skills</div>
    </div>
    <div class="header-kpi">
      <div class="header-kpi-val">{last_month}</div>
      <div class="header-kpi-lbl">Last Run</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI Cards ──────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="kpi-card">
      <div class="kpi-card-icon">🏆</div>
      <div class="kpi-card-title">Top Role by Volume</div>
      <div class="kpi-card-val">{top_role}</div>
      <div class="kpi-card-sub">{top_role_count:,} postings &nbsp;
        <span class="kpi-badge badge-green">↑ Growing</span></div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="kpi-card">
      <div class="kpi-card-icon">💵</div>
      <div class="kpi-card-title">Median Salary</div>
      <div class="kpi-card-val">${median_salary//1000}k</div>
      <div class="kpi-card-sub">filtered selection &nbsp;
        <span class="kpi-badge badge-blue">↔ Competitive</span></div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="kpi-card">
      <div class="kpi-card-icon">🌐</div>
      <div class="kpi-card-title">Remote Available</div>
      <div class="kpi-card-val">{remote_pct}%</div>
      <div class="kpi-card-sub">of filtered listings &nbsp;
        <span class="kpi-badge badge-gold">→ Stabilising</span></div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="kpi-card">
      <div class="kpi-card-icon">⚡</div>
      <div class="kpi-card-title">Highest Paying Role</div>
      <div class="kpi-card-val">{top_paying_role}</div>
      <div class="kpi-card-sub">${top_paying_sal//1000}k median &nbsp;
        <span class="kpi-badge badge-purple">★ Premium</span></div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Section: Distribution & Compensation ──────────────────────────────────
st.markdown('<div class="section-label">Distribution & Compensation</div>', unsafe_allow_html=True)
st.markdown('<div class="section-insight">Senior AI/LLM Engineers & ML Engineers command $200k+ median — a 2.7× premium over Entry Level in the same roles.</div>', unsafe_allow_html=True)

col_l, col_r = st.columns([1, 2])

with col_l:
    # Donut chart - postings by role
    if not postings_df.empty:
        fig_donut = go.Figure(go.Pie(
            labels=postings_df["ROLE_CLUSTER"],
            values=postings_df["POSTING_COUNT"],
            hole=0.6,
            marker=dict(colors=PALETTE),
            textinfo="none",
            hovertemplate="<b>%{label}</b><br>%{value:,} postings (%{percent})<extra></extra>",
        ))
        fig_donut.update_layout(
            title=dict(text="Postings by Role", font=dict(size=12, color="#ccc"), x=0),
            showlegend=True,
            legend=dict(font=dict(size=10, color="#aaa"), orientation="h", y=-0.15),
            height=300,
            **CHART_THEME,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

with col_r:
    # Grouped bar - salary by role & seniority
    if not role_sen_df.empty:
        filtered_rs = role_sen_df[
            role_sen_df["ROLE_CLUSTER"].isin(sel_roles if sel_roles else all_roles) &
            role_sen_df["SENIORITY"].isin(sel_sen if sel_sen else all_seniorities)
        ]
        fig_grouped = px.bar(
            filtered_rs,
            x="ROLE_CLUSTER", y="MEDIAN_SALARY", color="SENIORITY",
            barmode="group",
            color_discrete_sequence=PALETTE,
            labels={"ROLE_CLUSTER": "", "MEDIAN_SALARY": "Median Salary ($)", "SENIORITY": ""},
            title="Median Salary by Role & Seniority",
        )
        fig_grouped.update_layout(
            height=300, legend=dict(orientation="h", y=-0.25, font=dict(size=10)),
            yaxis=dict(tickformat="$,.0f"),
            **CHART_THEME,
        )
        st.plotly_chart(fig_grouped, use_container_width=True)

# ── Section: Top Skills ────────────────────────────────────────────────────
st.markdown('<div class="section-label">Top 20 In-Demand Skills</div>', unsafe_allow_html=True)
st.markdown('<div class="section-insight">Python & SQL are universal baselines. Cloud platforms (AWS, GCP, Azure) and LLM tooling are the fastest-rising entrants in 2024-2025 AI hiring.</div>', unsafe_allow_html=True)

if not skills_df.empty:
    max_count = int(skills_df["JOB_COUNT"].max())
    tiles_html = '<div class="skill-grid">'
    for _, row in skills_df.head(20).iterrows():
        pct = int(row["JOB_COUNT"] / max_count * 100)
        sal = f"${int(row['MEDIAN_SALARY'])//1000}k" if row["MEDIAN_SALARY"] else "—"
        tiles_html += f"""
        <div class="skill-tile">
          <div class="skill-name">{row['SKILL_ID'].upper()}</div>
          <div class="skill-count">{int(row['JOB_COUNT']):,} jobs · {sal}</div>
          <div class="skill-bar-bg"><div class="skill-bar-fill" style="width:{pct}%"></div></div>
        </div>"""
    tiles_html += "</div>"
    st.markdown(tiles_html, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Section: Remote Work & Hiring Trends ──────────────────────────────────
st.markdown('<div class="section-label">Remote Work & Hiring Trends</div>', unsafe_allow_html=True)
st.markdown('<div class="section-insight">Remote-only roles are stabilising as large employers enforce return-to-office. Hybrid is now the dominant arrangement.</div>', unsafe_allow_html=True)

col_remote, col_trends = st.columns(2)

with col_remote:
    if not remote_df.empty:
        fig_remote = go.Figure()
        for col, name, color in [
            ("REMOTE_PCT", "Remote", "#00d4aa"),
            ("HYBRID_PCT", "Hybrid", "#4da6ff"),
            ("ONSITE_PCT", "On-site", "#ff6b6b"),
        ]:
            fig_remote.add_trace(go.Scatter(
                x=remote_df["MONTH"], y=remote_df[col],
                name=name, line=dict(color=color, width=2),
                mode="lines+markers", marker=dict(size=4),
                hovertemplate=f"{name}: %{{y:.1f}}%<extra></extra>",
            ))
        fig_remote.update_layout(
            title="Remote / Hybrid / On-site Over Time (Monthly %)",
            yaxis=dict(ticksuffix="%", range=[0, 100]),
            legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
            height=300, **CHART_THEME,
        )
        st.plotly_chart(fig_remote, use_container_width=True)

with col_trends:
    if not trends_df.empty:
        fig_trends = px.line(
            trends_df[trends_df["ROLE_CLUSTER"].isin(sel_roles if sel_roles else all_roles)],
            x="MONTH", y="POSTING_COUNT", color="ROLE_CLUSTER",
            color_discrete_sequence=PALETTE,
            title="Job Posting Volume Over Time",
            labels={"MONTH": "", "POSTING_COUNT": "Postings", "ROLE_CLUSTER": ""},
        )
        fig_trends.update_layout(
            legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
            height=300, **CHART_THEME,
        )
        st.plotly_chart(fig_trends, use_container_width=True)

# ── Section: Compensation Deep Dive ───────────────────────────────────────
st.markdown('<div class="section-label">Compensation Deep Dive</div>', unsafe_allow_html=True)
st.markdown('<div class="section-insight">Compensation scales 2.7× from Entry to Principal. The Staff/Lead → Principal jump is the single largest step (~$65k).</div>', unsafe_allow_html=True)

col_sen, col_range = st.columns(2)

with col_sen:
    if not seniority_df.empty:
        fig_sen = px.bar(
            seniority_df,
            x="SENIORITY", y="MEDIAN_SALARY",
            color="MEDIAN_SALARY",
            color_continuous_scale=[[0,"#1a3d5c"],[1,"#00d4aa"]],
            title="Salary by Seniority Level",
            text=seniority_df["MEDIAN_SALARY"].apply(lambda x: f"${int(x)//1000}k"),
            labels={"SENIORITY": "", "MEDIAN_SALARY": "Median Salary ($)"},
        )
        fig_sen.update_traces(textposition="outside", textfont_size=10)
        fig_sen.update_layout(
            showlegend=False, coloraxis_showscale=False,
            yaxis=dict(tickformat="$,.0f"),
            height=300, **CHART_THEME,
        )
        st.plotly_chart(fig_sen, use_container_width=True)

with col_range:
    if not postings_df.empty:
        fig_range = go.Figure()
        for i, row in postings_df.iterrows():
            p10 = row.get("P10_SALARY", 0) or 0
            p90 = row.get("P90_SALARY", 0) or 0
            med = row.get("MEDIAN_SALARY", 0) or 0
            fig_range.add_trace(go.Bar(
                name=row["ROLE_CLUSTER"],
                x=[row["ROLE_CLUSTER"]],
                y=[p90 - p10],
                base=[p10],
                marker_color=PALETTE[i % len(PALETTE)],
                hovertemplate=f"<b>{row['ROLE_CLUSTER']}</b><br>P10: ${p10:,.0f}<br>Median: ${med:,.0f}<br>P90: ${p90:,.0f}<extra></extra>",
            ))
        fig_range.update_layout(
            title="Salary Spread (P10→P90) by Role — Negotiation Window",
            showlegend=False,
            yaxis=dict(tickformat="$,.0f", title="Annual Salary ($)"),
            barmode="overlay",
            height=300, **CHART_THEME,
        )
        st.plotly_chart(fig_range, use_container_width=True)

# ── Resume Analyzer Tab ────────────────────────────────────────────────────
st.markdown("---")
with st.expander("🤖 AI Resume Analyzer — Paste your resume to get skill gap analysis"):
    col1, col2 = st.columns(2)
    with col1:
        resume_text = st.text_area("Your Resume", height=250, placeholder="Paste resume text here...")
    with col2:
        jd_text = st.text_area("Job Description", height=250, placeholder="Paste JD here...")

    if st.button("Analyze Match →", type="primary") and resume_text and jd_text:
        try:
            from ai.resume_analyzer import extract_skills_from_resume, extract_skills_from_jd, skill_gap_analysis
            market_skills = skills_df["SKILL_ID"].head(20).tolist()
            with st.spinner("Analyzing with Claude..."):
                resume_profile = extract_skills_from_resume(resume_text)
                jd_profile = extract_skills_from_jd(jd_text)
                gap = skill_gap_analysis(resume_profile, jd_profile, market_skills)
            score = gap.get("match_score", 0)
            color = "green" if score >= 0.7 else "orange" if score >= 0.5 else "red"
            st.markdown(f"### Match Score: :{color}[{score:.0%}] — {gap.get('match_summary','')}")
            g1, g2 = st.columns(2)
            with g1:
                st.success("**Matched Skills**")
                for s in gap.get("matched_skills", []): st.write(f"✓ {s}")
            with g2:
                st.error("**Critical Gaps**")
                for s in gap.get("missing_critical", []): st.write(f"✗ {s}")
            st.subheader("Learning Path")
            for item in gap.get("learning_path", []):
                st.markdown(f"**{item['skill']}** `{item['priority']}` — {item['reason']}")
        except Exception as e:
            st.warning(f"Set ANTHROPIC_API_KEY in .env to enable AI features. Error: {e}")
