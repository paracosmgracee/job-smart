"""
Tech Job Market Intelligence Dashboard
4-tab layout: Market Overview | Compensation | Skills | AI Resume Analyzer
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Tech Job Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Helpers ────────────────────────────────────────────────────────────────
def _secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, "")

PALETTE = ["#00d4aa","#4da6ff","#ffb347","#c97bff","#ff6b6b","#7bed9f","#ffa502","#70a1ff","#eccc68","#a29bfe"]
CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#ccc", size=11), margin=dict(l=10, r=10, t=35, b=10),
)

def insight(text):
    st.markdown(f'<div style="font-size:0.78rem;color:#aaa;border-left:3px solid #00d4aa;padding-left:0.6rem;margin-bottom:0.8rem">{text}</div>', unsafe_allow_html=True)

def section(text):
    st.markdown(f'<div style="font-size:0.65rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.3rem">{text}</div>', unsafe_allow_html=True)

# ── Snowflake ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=_secret("SNOWFLAKE_ACCOUNT"), user=_secret("SNOWFLAKE_USER"),
        password=_secret("SNOWFLAKE_PASSWORD"), database=_secret("SNOWFLAKE_DATABASE"),
        warehouse=_secret("SNOWFLAKE_WAREHOUSE"), role=_secret("SNOWFLAKE_ROLE"),
        schema="MARTS",
    )

@st.cache_data(ttl=3600)
def q(_conn, sql):
    return pd.read_sql(sql, _conn)

# ── CSS ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .block-container{padding-top:0.5rem;padding-bottom:1rem}
  #MainMenu,footer,header{visibility:hidden}

  .top-bar{
    display:flex;align-items:center;justify-content:space-between;
    padding:0.6rem 1rem;background:#0f1117;
    border-bottom:1px solid #262730;margin-bottom:1rem;
  }
  .top-title{font-size:1.1rem;font-weight:700;color:#fff}
  .top-sub{font-size:0.7rem;color:#666;margin-top:2px}
  .kpi-strip{display:flex;gap:2rem}
  .kpi-item{text-align:center}
  .kpi-val{font-size:1.1rem;font-weight:800;color:#00d4aa}
  .kpi-lbl{font-size:0.6rem;color:#777;text-transform:uppercase;letter-spacing:.05em}

  .card{background:#1a1d27;border:1px solid #262730;border-radius:8px;padding:1rem}
  .card-icon{font-size:1.1rem;margin-bottom:.3rem}
  .card-label{font-size:.65rem;color:#888;text-transform:uppercase;letter-spacing:.05em}
  .card-val{font-size:1.5rem;font-weight:800;color:#fff;margin:.15rem 0}
  .card-sub{font-size:.7rem;color:#888}
  .badge{display:inline-block;padding:1px 7px;border-radius:20px;font-size:.65rem;font-weight:600}
  .bg{background:#0d2b1e;color:#00d4aa}
  .bb{background:#0d1e2b;color:#4da6ff}
  .bo{background:#2b1e0d;color:#ffb347}
  .bp{background:#1e0d2b;color:#c97bff}

  .skill-grid{display:flex;flex-wrap:wrap;gap:.4rem;margin-top:.5rem}
  .skill-tile{
    background:#1a1d27;border:1px solid #262730;
    border-radius:6px;padding:.4rem .7rem;min-width:110px;max-width:160px;flex:1
  }
  .skill-name{font-size:.78rem;font-weight:600;color:#fff}
  .skill-meta{font-size:.63rem;color:#888;margin-top:2px}
  .bar-bg{background:#262730;border-radius:2px;height:3px;margin-top:5px}
  .bar-fill{background:#00d4aa;border-radius:2px;height:3px}

  div[data-testid="stTabs"] button{font-size:.82rem}
</style>
""", unsafe_allow_html=True)

# ── Load all data ──────────────────────────────────────────────────────────
conn = get_conn()
roles_df    = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE ORDER BY POSTING_COUNT DESC")
trends_df   = q(conn, "SELECT * FROM MARTS.MART_HIRING_TRENDS ORDER BY MONTH, ROLE_CLUSTER")
seniority_df= q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_SENIORITY ORDER BY SORT_ORDER")
role_sen_df = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE_SENIORITY")
skills_df      = q(conn, "SELECT * FROM MARTS.MART_TECH_SKILLS ORDER BY DEMAND_RANK")
skills_role_df = q(conn, "SELECT * FROM MARTS.MART_TECH_SKILLS_BY_ROLE ORDER BY ROLE_CLUSTER, ROLE_RANK")
geo_df      = q(conn, "SELECT * FROM MARTS.MART_JOBS_BY_LOCATION ORDER BY JOB_COUNT DESC")

# ── Global role filter ─────────────────────────────────────────────────────
all_roles   = sorted(roles_df["ROLE_CLUSTER"].dropna().unique().tolist()) if not roles_df.empty else []
role_options = ["All Roles"] + all_roles

# Role selector in a compact row
fcol1, fcol2 = st.columns([2, 5])
with fcol1:
    sel_role = st.selectbox(
        "Filter by role",
        role_options,
        index=0,
        label_visibility="collapsed",
    )

# Apply filter to all relevant dataframes
if sel_role == "All Roles":
    roles_f     = roles_df.copy()
    trends_f    = trends_df.copy()
    role_sen_f  = role_sen_df.copy()
    geo_col     = "JOB_COUNT"
else:
    roles_f     = roles_df[roles_df["ROLE_CLUSTER"] == sel_role]
    trends_f    = trends_df[trends_df["ROLE_CLUSTER"] == sel_role]
    role_sen_f  = role_sen_df[role_sen_df["ROLE_CLUSTER"] == sel_role]
    col_map     = {
        "Data Engineer": "DE_COUNT", "Data Scientist": "DS_COUNT",
        "Data Analyst": "DA_COUNT", "Software Engineer": "SWE_COUNT",
        "ML Engineer": "ML_COUNT",
    }
    geo_col = col_map.get(sel_role, "JOB_COUNT")

# ── Global KPIs (filtered) ──────────────────────────────────────────────────
total      = int(roles_f["POSTING_COUNT"].sum()) if not roles_f.empty else 0
med_sal    = int(roles_f["MEDIAN_SALARY"].median()) if not roles_f.empty else 0
top_role   = roles_f.iloc[0]["ROLE_CLUSTER"] if not roles_f.empty else "-"
top_pay    = roles_f.sort_values("MEDIAN_SALARY", ascending=False).iloc[0]["ROLE_CLUSTER"] if not roles_f.empty else "-"
top_pay_sal= int(roles_f.sort_values("MEDIAN_SALARY", ascending=False).iloc[0]["MEDIAN_SALARY"]) if not roles_f.empty else 0
n_skills   = len(skills_df)
last_run   = str(trends_df["MONTH"].max())[:7] if not trends_f.empty else "-"

# ── Top bar ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="top-bar">
  <div>
    <div class="top-title">📊 Tech Job Market Intelligence</div>
    <div class="top-sub">Analytics pipeline · Kaggle LinkedIn Dataset · {last_run}</div>
  </div>
  <div class="kpi-strip">
    <div class="kpi-item"><div class="kpi-val">{total:,}</div><div class="kpi-lbl">Total Listings</div></div>
    <div class="kpi-item"><div class="kpi-val">${med_sal//1000}k</div><div class="kpi-lbl">Median Salary</div></div>
    <div class="kpi-item"><div class="kpi-val">{n_skills}</div><div class="kpi-lbl">Tech Skills</div></div>
    <div class="kpi-item"><div class="kpi-val">{last_run}</div><div class="kpi-lbl">Last Run</div></div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "🌐  Market Overview",
    "💰  Compensation",
    "🔧  Skills Intelligence",
    "🤖  AI Resume Analyzer",
])

# ══════════════════════════════════════════════════════════════════════════
# TAB 1 — Market Overview
# ══════════════════════════════════════════════════════════════════════════
with tab1:
    # KPI cards
    c1,c2,c3,c4 = st.columns(4)
    cards = [
        (c1,"🏆","Top Role by Volume",top_role,f"{int(roles_f.iloc[0]['POSTING_COUNT']):,} postings","bg"),
        (c2,"💵","Median Salary",f"${med_sal//1000}k","across all roles","bb"),
        (c3,"⚡","Highest Paying",top_pay,f"${top_pay_sal//1000}k median","bp"),
        (c4,"🔧","Skills Tracked",str(n_skills),"technical skills indexed","bo"),
    ]
    for col, icon, label, val, sub, badge_cls in cards:
        col.markdown(f"""<div class="card">
          <div class="card-icon">{icon}</div>
          <div class="card-label">{label}</div>
          <div class="card-val">{val}</div>
          <div class="card-sub"><span class="badge {badge_cls}">{sub}</span></div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Row: Role demand bar + Hiring trends
    section("Demand Landscape")
    insight("Software Engineer leads by raw volume; Data roles (Engineer, Scientist, Analyst) are growing fastest as companies build out data infrastructure.")
    col_l, col_r = st.columns([1, 1])

    with col_l:
        if not roles_f.empty:
            fig = px.bar(
                roles_f.sort_values("POSTING_COUNT"),
                x="POSTING_COUNT", y="ROLE_CLUSTER",
                orientation="h",
                color="MEDIAN_SALARY",
                color_continuous_scale=[[0,"#1a3d5c"],[1,"#00d4aa"]],
                title="Job Postings by Role (color = salary)",
                labels={"POSTING_COUNT":"# Postings","ROLE_CLUSTER":"","MEDIAN_SALARY":"Median Salary"},
                text="POSTING_COUNT",
            )
            fig.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig.update_layout(coloraxis_showscale=False, height=320, **CHART)
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        if not trends_f.empty:
            fig = px.line(
                trends_f, x="MONTH", y="POSTING_COUNT", color="ROLE_CLUSTER",
                color_discrete_sequence=PALETTE,
                title="Hiring Volume Over Time",
                labels={"MONTH":"","POSTING_COUNT":"Postings","ROLE_CLUSTER":""},
                markers=True,
            )
            fig.update_layout(
                legend=dict(orientation="h", y=-0.25, font=dict(size=10)),
                height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Geographic heatmap
    st.markdown("<br>", unsafe_allow_html=True)
    section("Geographic Distribution")
    insight("California, New York, and Texas dominate job volume. Remote-heavy states like WA and MA punch above their weight in AI/ML roles.")

    if not geo_df.empty:
        col_map, col_top = st.columns([2, 1])
        with col_map:
            fig_map = px.choropleth(
                geo_df,
                locations="STATE_CODE",
                locationmode="USA-states",
                color=geo_col,
                scope="usa",
                color_continuous_scale=[[0,"#1a1d27"],[0.3,"#1a3d5c"],[1,"#00d4aa"]],
                hover_data={geo_col:True,"MEDIAN_SALARY":True},
                title=f"Job Postings by State — {sel_role}",
                labels={geo_col:"Postings","MEDIAN_SALARY":"Median Salary"},
            )
            fig_map.update_layout(
                geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)"),
                coloraxis_colorbar=dict(title="Jobs", thickness=12),
                height=380, **CHART,
            )
            st.plotly_chart(fig_map, use_container_width=True)

        with col_top:
            st.markdown("**Top 10 States**")
            top10 = geo_df.sort_values(geo_col, ascending=False).head(10)[["STATE_CODE", geo_col,"MEDIAN_SALARY"]].copy()
            top10.columns = ["State","Jobs","Median Salary"]
            top10["Median Salary"] = top10["Median Salary"].apply(
                lambda x: f"${int(x)//1000}k" if pd.notna(x) and x > 0 else "—"
            )
            top10["Jobs"] = top10["Jobs"].apply(lambda x: f"{int(x):,}")
            st.dataframe(top10, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 2 — Compensation
# ══════════════════════════════════════════════════════════════════════════
with tab2:
    section("Salary by Role × Seniority")
    insight("The Staff/Lead → Principal jump is the single largest step (~$65k). Senior AI/LLM and ML roles command 2.7× the Entry Level median in the same function.")

    # Seniority filter
    all_sen = ["Entry Level","Mid Level","Senior","Staff/Lead","Principal"]
    sel_sen = st.multiselect("Filter seniority", all_sen, default=all_sen, label_visibility="collapsed")

    if not role_sen_f.empty:
        filtered = role_sen_f[role_sen_f["SENIORITY"].isin(sel_sen)] if sel_sen else role_sen_df
        fig = px.bar(
            filtered, x="ROLE_CLUSTER", y="MEDIAN_SALARY", color="SENIORITY",
            barmode="group", color_discrete_sequence=PALETTE,
            labels={"ROLE_CLUSTER":"","MEDIAN_SALARY":"Median Salary ($)","SENIORITY":""},
            title="Median Salary by Role & Seniority Level",
        )
        fig.update_layout(
            yaxis=dict(tickformat="$,.0f"),
            legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
            height=360, **CHART,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_a, col_b = st.columns(2)

    with col_a:
        section("Salary Distribution (Box Plot)")
        insight("Box plot shows median, IQR, and outliers — a better read than averages alone.")
        if not roles_f.empty:
            fig = go.Figure()
            for i, row in roles_f.iterrows():
                p10 = row.get("P10_SALARY") or 0
                p25 = row.get("P25_SALARY") or 0
                med = row.get("MEDIAN_SALARY") or 0
                p75 = row.get("P75_SALARY") or 0
                p90 = row.get("P90_SALARY") or 0
                fig.add_trace(go.Box(
                    name=row["ROLE_CLUSTER"],
                    q1=[p25], median=[med], q3=[p75],
                    lowerfence=[p10], upperfence=[p90],
                    marker_color=PALETTE[i % len(PALETTE)],
                    boxmean=True,
                ))
            fig.update_layout(
                yaxis=dict(tickformat="$,.0f", title="Annual Salary"),
                showlegend=False, height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        section("Negotiation Window (P10 → P90)")
        insight("ML Engineer & AI/LLM roles show the widest comp bands — skilled negotiators can earn 3× the floor at the same level.")
        if not roles_f.empty:
            fig = go.Figure()
            for i, row in roles_f.iterrows():
                p10 = row.get("P10_SALARY") or 0
                p90 = row.get("P90_SALARY") or 0
                med = row.get("MEDIAN_SALARY") or 0
                if p90 > p10:
                    fig.add_trace(go.Bar(
                        name=row["ROLE_CLUSTER"],
                        x=[row["ROLE_CLUSTER"]],
                        y=[p90 - p10], base=[p10],
                        marker_color=PALETTE[i % len(PALETTE)],
                        hovertemplate=f"<b>{row['ROLE_CLUSTER']}</b><br>Floor: ${p10:,.0f}<br>Median: ${med:,.0f}<br>Ceiling: ${p90:,.0f}<extra></extra>",
                    ))
            fig.update_layout(
                showlegend=False, barmode="overlay",
                yaxis=dict(tickformat="$,.0f", title="Annual Salary ($)"),
                height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Geographic salary
    st.markdown("<br>", unsafe_allow_html=True)
    section("Salary by State")
    insight("States with tech hubs (CA, WA, NY, MA) show median salaries 20-35% above national median. Remote roles often benchmark to SF/NYC rates.")
    if not geo_df.empty:
        geo_sal = geo_df[geo_df["MEDIAN_SALARY"] > 0].sort_values("MEDIAN_SALARY", ascending=False).head(20)
        fig = px.bar(
            geo_sal, x="STATE_CODE", y="MEDIAN_SALARY",
            color="MEDIAN_SALARY",
            color_continuous_scale=[[0,"#1a3d5c"],[1,"#00d4aa"]],
            title="Median Annual Salary by State (Top 20)",
            labels={"STATE_CODE":"State","MEDIAN_SALARY":"Median Salary"},
            text=geo_sal["MEDIAN_SALARY"].apply(lambda x: f"${int(x)//1000}k"),
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, yaxis=dict(tickformat="$,.0f"), height=320, **CHART)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 3 — Skills Intelligence
# ══════════════════════════════════════════════════════════════════════════
with tab3:
    role_label = sel_role if sel_role != "All Roles" else "All Roles"
    section(f"Technical Skills — Demand × Salary · {role_label}")
    insight("Top-right quadrant = high demand AND high pay = best ROI to learn. Kubernetes, Kafka, Terraform: niche but premium. SQL/Python: universal baseline, table stakes.")

    # Use role-specific skills if a role is selected
    if sel_role != "All Roles" and not skills_role_df.empty:
        active_skills = skills_role_df[skills_role_df["ROLE_CLUSTER"] == sel_role].copy()
        active_skills = active_skills.rename(columns={"ROLE_RANK": "DEMAND_RANK"})
    else:
        active_skills = skills_df.copy()

    if not active_skills.empty:
        # Scatter: demand vs salary
        scatter_df = active_skills[active_skills["MEDIAN_SALARY"] > 0].copy()
        scatter_df["size"] = scatter_df["JOB_COUNT"].apply(lambda x: max(8, min(40, x/50)))
        scatter_df["label"] = scatter_df["SKILL"].str.title()

        fig = px.scatter(
            scatter_df,
            x="JOB_COUNT", y="MEDIAN_SALARY",
            size="size", color="MEDIAN_SALARY",
            color_continuous_scale=[[0,"#1a3d5c"],[0.5,"#4da6ff"],[1,"#00d4aa"]],
            hover_name="label",
            hover_data={"JOB_COUNT":True,"MEDIAN_SALARY":True,"size":False},
            title="Skill Demand vs. Salary — bubble size = job count",
            labels={"JOB_COUNT":"# Job Postings","MEDIAN_SALARY":"Median Annual Salary ($)"},
            text="label",
        )
        fig.update_traces(textposition="top center", textfont_size=9)
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis=dict(tickformat="$,.0f"),
            height=420, **CHART,
        )
        # Quadrant lines
        med_x = scatter_df["JOB_COUNT"].median()
        med_y = scatter_df["MEDIAN_SALARY"].median()
        fig.add_hline(y=med_y, line_dash="dot", line_color="#444", opacity=0.6)
        fig.add_vline(x=med_x, line_dash="dot", line_color="#444", opacity=0.6)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    section(f"Top 20 Skills — At a Glance · {role_label}")
    insight("Python and SQL appear in virtually every data role. Cloud platforms (AWS, Azure, GCP) are increasingly required, not optional.")

    if not active_skills.empty:
        max_count = int(active_skills["JOB_COUNT"].max())
        tiles = '<div class="skill-grid">'
        for _, row in active_skills.head(20).iterrows():
            name  = row["SKILL"].title()
            count = int(row["JOB_COUNT"])
            sal   = f"${int(row['MEDIAN_SALARY'])//1000}k" if pd.notna(row["MEDIAN_SALARY"]) and row["MEDIAN_SALARY"] > 0 else "—"
            pct   = int(count / max_count * 100)
            rank  = int(row["DEMAND_RANK"])
            tiles += f"""<div class="skill-tile">
              <div class="skill-name">#{rank} {name}</div>
              <div class="skill-meta">{count:,} jobs · {sal}</div>
              <div class="bar-bg"><div class="bar-fill" style="width:{pct}%"></div></div>
            </div>"""
        tiles += "</div>"
        st.markdown(tiles, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    section("Full Ranking Table")
    if not active_skills.empty:
        cols = ["DEMAND_RANK","SKILL","JOB_COUNT","MEDIAN_SALARY"]
        if "SENIOR_COUNT" in active_skills.columns:
            cols.append("SENIOR_COUNT")
        tbl = active_skills[cols].copy()
        tbl.columns = ["Rank","Skill","# Jobs","Median Salary"] + (["Senior Jobs"] if "SENIOR_COUNT" in cols else [])
        tbl["Skill"]         = tbl["Skill"].str.title()
        tbl["Median Salary"] = tbl["Median Salary"].apply(lambda x: f"${int(x):,}" if pd.notna(x) and x > 0 else "—")
        tbl["# Jobs"]        = tbl["# Jobs"].apply(lambda x: f"{int(x):,}")
        if "Senior Jobs" in tbl.columns:
            tbl["Senior Jobs"] = tbl["Senior Jobs"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
        st.dataframe(tbl, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 4 — AI Resume Analyzer
# ══════════════════════════════════════════════════════════════════════════
with tab4:
    section("AI-Powered Resume × JD Match Analysis")
    insight("Powered by Claude API. Paste your resume and a job description to get a match score, skill gap analysis, and personalized learning path — grounded in real market data.")

    col1, col2 = st.columns(2)
    with col1:
        resume_text = st.text_area("📄 Your Resume", height=280, placeholder="Paste your resume text here...")
    with col2:
        jd_text = st.text_area("📋 Job Description", height=280, placeholder="Paste the job description here...")

    if st.button("Analyze Match →", type="primary", use_container_width=True):
        if not resume_text or not jd_text:
            st.warning("Paste both your resume and the job description first.")
        else:
            api_key = _secret("ANTHROPIC_API_KEY")
            if not api_key:
                st.error("Set ANTHROPIC_API_KEY in Streamlit secrets to enable AI features.")
            else:
                try:
                    import sys, os
                    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                    from ai.resume_analyzer import extract_skills_from_resume, extract_skills_from_jd, skill_gap_analysis
                    market_skills = skills_df["SKILL"].head(20).tolist() if not skills_df.empty else []

                    with st.spinner("Analyzing with Claude..."):
                        resume_profile = extract_skills_from_resume(resume_text)
                        jd_profile     = extract_skills_from_jd(jd_text)
                        gap            = skill_gap_analysis(resume_profile, jd_profile, market_skills)

                    score = gap.get("match_score", 0)
                    color = "green" if score >= 0.7 else "orange" if score >= 0.5 else "red"
                    rec   = gap.get("application_recommendation","—").upper()

                    st.markdown(f"### Match Score: :{color}[{score:.0%}] &nbsp;·&nbsp; Recommendation: `{rec}`")
                    st.caption(gap.get("match_summary",""))

                    col_m, col_g, col_s = st.columns(3)
                    with col_m:
                        st.success("**Matched Skills**")
                        for s in gap.get("matched_skills",[]): st.write(f"✓ {s}")
                    with col_g:
                        st.error("**Critical Gaps**")
                        for s in gap.get("missing_critical",[]): st.write(f"✗ {s}")
                    with col_s:
                        st.warning("**Nice-to-Have Gaps**")
                        for s in gap.get("missing_preferred",[]): st.write(f"· {s}")

                    st.subheader("Learning Path")
                    for item in gap.get("learning_path",[]):
                        pri = item.get("priority","")
                        color_map = {"high":"🔴","medium":"🟡","low":"🟢"}
                        st.markdown(f"{color_map.get(pri,'·')} **{item['skill']}** — {item['reason']}")
                        st.caption(f"Resource: {item.get('resource','TBD')}")

                    if gap.get("tailoring_tips"):
                        st.subheader("Resume Tailoring Tips")
                        for tip in gap["tailoring_tips"]:
                            st.markdown(f"• {tip}")

                except Exception as e:
                    st.error(f"Error: {e}")

    st.markdown("---")
    section("Learning Path Generator")
    insight("Don't have a specific JD? Generate a 90-day roadmap based on your current skills and target role.")

    col_skills, col_role = st.columns([2,1])
    with col_skills:
        skills_input = st.text_input("Your current skills", placeholder="Python, SQL, Excel, Tableau...")
    with col_role:
        target_role = st.selectbox("Target role", [
            "Data Analyst","Data Engineer","Data Scientist","ML Engineer","AI Engineer","Analytics Engineer"
        ])

    if st.button("Generate 90-Day Roadmap →", use_container_width=True) and skills_input:
        api_key = _secret("ANTHROPIC_API_KEY")
        if not api_key:
            st.error("Set ANTHROPIC_API_KEY in Streamlit secrets to enable AI features.")
        else:
            try:
                import sys, os
                sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                from ai.resume_analyzer import generate_learning_path
                market_skills = skills_df["SKILL"].head(20).tolist() if not skills_df.empty else []
                with st.spinner("Building your roadmap..."):
                    result = generate_learning_path(
                        [s.strip() for s in skills_input.split(",")],
                        target_role, market_skills
                    )
                col_r1, col_r2 = st.columns(2)
                col_r1.metric("Job Readiness Score", f"{result.get('readiness_score',0)}%")
                col_r2.metric("Weeks to Job-Ready", result.get("weeks_to_job_ready","?"))
                for step in result.get("roadmap",[]):
                    with st.expander(f"Week {step['week_range']}: {step['focus']}"):
                        st.write("**Skills:**", ", ".join(step.get("skills",[])))
                        for r in step.get("resources",[]): st.write(f"· {r}")
                        st.success(f"Milestone: {step.get('milestone','')}")
                if result.get("portfolio_projects"):
                    st.subheader("Portfolio Project Ideas")
                    for proj in result["portfolio_projects"]:
                        st.markdown(f"**{proj['title']}** — {proj['description']}")
                        st.caption("Skills: " + ", ".join(proj.get("skills_demonstrated",[])))
            except Exception as e:
                st.error(f"Error: {e}")
