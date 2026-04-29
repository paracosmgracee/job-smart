import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Job Market Intelligence",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

def _secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, "")

# ── Color system ───────────────────────────────────────────────────────────
C = {
    "bg":       "#0b0b0f",
    "surface":  "#13131a",
    "border":   "#1e1e2a",
    "muted":    "#3a3a50",
    "text":     "#c8c8d8",
    "heading":  "#f0f0f8",
    "accent":   "#4f46e5",   # indigo
    "amber":    "#f59e0b",
    "rose":     "#f43f5e",
    "emerald":  "#10b981",
}
PALETTE = ["#4f46e5","#f59e0b","#10b981","#f43f5e","#8b5cf6","#06b6d4","#84cc16","#fb923c"]

CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=C["text"], family="Inter, sans-serif", size=11),
    margin=dict(l=10, r=10, t=40, b=10),
)

# ── CSS ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
}}

.block-container {{
    padding: 2rem 2.5rem 2rem 2.5rem;
    max-width: 1400px;
}}

#MainMenu, footer, header {{ visibility: hidden }}

/* Sidebar */
section[data-testid="stSidebar"] {{
    background: {C["surface"]};
    border-right: 1px solid {C["border"]};
}}
section[data-testid="stSidebar"] .block-container {{
    padding: 1.5rem 1rem;
}}

/* Page title */
.page-title {{
    font-size: 1.6rem;
    font-weight: 700;
    color: {C["heading"]};
    letter-spacing: -0.02em;
    margin-bottom: 0.2rem;
}}
.page-sub {{
    font-size: 0.8rem;
    color: {C["muted"]};
    margin-bottom: 2rem;
}}

/* KPI row */
.kpi-row {{
    display: flex;
    gap: 1.5rem;
    margin-bottom: 2.5rem;
}}
.kpi {{
    flex: 1;
    border-top: 2px solid {C["accent"]};
    padding-top: 0.7rem;
}}
.kpi-val {{
    font-size: 1.8rem;
    font-weight: 700;
    color: {C["heading"]};
    letter-spacing: -0.03em;
    line-height: 1;
}}
.kpi-lbl {{
    font-size: 0.7rem;
    color: {C["muted"]};
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.3rem;
}}
.kpi-delta {{
    font-size: 0.75rem;
    color: {C["emerald"]};
    margin-top: 0.15rem;
}}

/* Section label */
.sec {{
    font-size: 0.7rem;
    font-weight: 600;
    color: {C["muted"]};
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.75rem;
    margin-top: 0.25rem;
}}

/* Skill chips */
.chip-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-top: 0.5rem;
}}
.chip {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 4px;
    padding: 0.25rem 0.6rem;
    font-size: 0.72rem;
    color: {C["text"]};
    display: flex;
    align-items: center;
    gap: 0.4rem;
}}
.chip-rank {{
    color: {C["muted"]};
    font-size: 0.65rem;
}}
.chip-bar {{
    width: 32px;
    height: 2px;
    background: {C["border"]};
    border-radius: 1px;
    overflow: hidden;
}}
.chip-fill {{
    height: 2px;
    background: {C["accent"]};
    border-radius: 1px;
}}

/* Divider */
.divider {{
    border: none;
    border-top: 1px solid {C["border"]};
    margin: 1.5rem 0;
}}

/* Callout */
.callout {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-left: 3px solid {C["accent"]};
    border-radius: 4px;
    padding: 0.75rem 1rem;
    font-size: 0.78rem;
    color: {C["text"]};
    margin-bottom: 1rem;
}}

/* Pills nav override */
div[data-testid="stPills"] {{
    margin-bottom: 1.5rem;
}}
</style>
""", unsafe_allow_html=True)

# ── Snowflake ──────────────────────────────────────────────────────────────
def _new_conn():
    return snowflake.connector.connect(
        account=_secret("SNOWFLAKE_ACCOUNT"),
        user=_secret("SNOWFLAKE_USER"),
        password=_secret("SNOWFLAKE_PASSWORD"),
        database=_secret("SNOWFLAKE_DATABASE"),
        warehouse=_secret("SNOWFLAKE_WAREHOUSE"),
        role=_secret("SNOWFLAKE_ROLE"),
        schema="MARTS",
        login_timeout=30,
        network_timeout=60,
    )

@st.cache_resource
def get_conn():
    return _new_conn()

@st.cache_data(ttl=3600)
def q(_conn, sql):
    try:
        return pd.read_sql(sql, _conn)
    except Exception:
        # Connection may have gone stale — clear cache and reconnect once
        get_conn.clear()
        fresh = _new_conn()
        return pd.read_sql(sql, fresh)

conn = get_conn()
roles_df       = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE ORDER BY POSTING_COUNT DESC")
trends_df      = q(conn, "SELECT * FROM MARTS.MART_HIRING_TRENDS ORDER BY MONTH, ROLE_CLUSTER")
seniority_df   = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_SENIORITY ORDER BY SORT_ORDER")
role_sen_df    = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_ROLE_SENIORITY")
skills_df      = q(conn, "SELECT * FROM MARTS.MART_TECH_SKILLS ORDER BY DEMAND_RANK")
skills_role_df = q(conn, "SELECT * FROM MARTS.MART_TECH_SKILLS_BY_ROLE ORDER BY ROLE_CLUSTER, ROLE_RANK")
geo_df         = q(conn, "SELECT * FROM MARTS.MART_JOBS_BY_LOCATION ORDER BY JOB_COUNT DESC")
tier_df        = q(conn, "SELECT * FROM MARTS.MART_SALARY_BY_COMPANY_TIER ORDER BY SORT_ORDER")
ai_cooc_df     = q(conn, "SELECT * FROM MARTS.MART_AI_SKILL_COOCCURRENCE ORDER BY SKILL_RANK")

# ── Top header ────────────────────────────────────────────────────────────
hcol1, hcol2 = st.columns([3, 1])
with hcol1:
    st.markdown('<div style="font-size:1.15rem;font-weight:700;color:#f0f0f8;letter-spacing:-0.02em;margin-bottom:0.1rem">Job Market Intelligence</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:0.72rem;color:#3a3a50;margin-bottom:1rem">2023 – 2026 · US Tech</div>', unsafe_allow_html=True)
with hcol2:
    all_roles = sorted(roles_df["ROLE_CLUSTER"].dropna().unique().tolist()) if not roles_df.empty else []
    sel_role = st.selectbox("Role", ["All Roles"] + all_roles, label_visibility="collapsed")

page = st.pills(
    "Navigation",
    ["Market Overview", "Compensation", "Skills", "Resume Analyzer"],
    default="Market Overview",
    label_visibility="collapsed",
)

last_run = str(trends_df["MONTH"].max())[:7] if not trends_df.empty else "—"

# ── Filtered frames ────────────────────────────────────────────────────────
if sel_role == "All Roles":
    roles_f    = roles_df.copy()
    trends_f   = trends_df.copy()
    role_sen_f = role_sen_df.copy()
    geo_col    = "JOB_COUNT"
else:
    roles_f    = roles_df[roles_df["ROLE_CLUSTER"] == sel_role]
    trends_f   = trends_df[trends_df["ROLE_CLUSTER"] == sel_role]
    role_sen_f = role_sen_df[role_sen_df["ROLE_CLUSTER"] == sel_role]
    col_map    = {
        "Data Engineer": "DE_COUNT", "Data Scientist": "DS_COUNT",
        "Data Analyst": "DA_COUNT", "Software Engineer": "SWE_COUNT",
        "ML Engineer": "ML_COUNT",
    }
    geo_col = col_map.get(sel_role, "JOB_COUNT")

if sel_role != "All Roles" and not skills_role_df.empty:
    active_skills = skills_role_df[skills_role_df["ROLE_CLUSTER"] == sel_role].copy()
    active_skills = active_skills.rename(columns={"ROLE_RANK": "DEMAND_RANK"})
else:
    active_skills = skills_df.copy()

med_sal     = int(roles_f["MEDIAN_SALARY"].median()) if not roles_f.empty else 0
top_role    = roles_f.iloc[0]["ROLE_CLUSTER"] if not roles_f.empty else "—"
top_pay_row = roles_f.sort_values("MEDIAN_SALARY", ascending=False).iloc[0] if not roles_f.empty else None
top_pay     = top_pay_row["ROLE_CLUSTER"] if top_pay_row is not None else "—"
top_pay_sal = int(top_pay_row["MEDIAN_SALARY"]) if top_pay_row is not None else 0
n_postings  = int(roles_f["POSTING_COUNT"].sum()) if not roles_f.empty else 0


# ══════════════════════════════════════════════════════════════════════════
# PAGE 1 — Market Overview
# ══════════════════════════════════════════════════════════════════════════
if page == "Market Overview":
    st.markdown(f'<div class="page-title">Market Overview</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">{sel_role} · {n_postings:,} postings</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi">
        <div class="kpi-val">{n_postings:,}</div>
        <div class="kpi-lbl">Total Postings</div>
      </div>
      <div class="kpi" style="border-top-color:{C['amber']}">
        <div class="kpi-val">${med_sal//1000}k</div>
        <div class="kpi-lbl">Median Salary</div>
      </div>
      <div class="kpi" style="border-top-color:{C['emerald']}">
        <div class="kpi-val">{top_role}</div>
        <div class="kpi-lbl">Highest Volume Role</div>
      </div>
      <div class="kpi" style="border-top-color:{C['rose']}">
        <div class="kpi-val">{top_pay}</div>
        <div class="kpi-lbl">Highest Paying · ${top_pay_sal//1000}k</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec">Postings by Role</div>', unsafe_allow_html=True)
    if not roles_f.empty:
        fig = px.bar(
            roles_f.sort_values("POSTING_COUNT"),
            x="POSTING_COUNT", y="ROLE_CLUSTER",
            orientation="h",
            color="MEDIAN_SALARY",
            color_continuous_scale=[[0, "#1e1e3a"], [1, "#4f46e5"]],
            labels={"POSTING_COUNT": "Postings", "ROLE_CLUSTER": "", "MEDIAN_SALARY": "Median Salary"},
            text="POSTING_COUNT",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside", textfont_size=10)
        fig.update_layout(coloraxis_showscale=False, height=320, **CHART)
        fig.update_xaxes(showgrid=False, showticklabels=False)
        fig.update_yaxes(showgrid=False)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Geographic Distribution</div>', unsafe_allow_html=True)

    if not geo_df.empty:
        col_map, col_top = st.columns([3, 1], gap="large")
        with col_map:
            fig_map = px.choropleth(
                geo_df,
                locations="STATE_CODE",
                locationmode="USA-states",
                color=geo_col,
                scope="usa",
                color_continuous_scale=[[0, "#13131a"], [0.3, "#1e1e3a"], [1, "#4f46e5"]],
                hover_data={geo_col: True, "MEDIAN_SALARY": True},
                labels={geo_col: "Postings", "MEDIAN_SALARY": "Median Salary"},
            )
            fig_map.update_layout(
                geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor="rgba(0,0,0,0)", landcolor="#13131a"),
                coloraxis_colorbar=dict(title="Jobs", thickness=10, len=0.6),
                height=360, **CHART,
            )
            st.plotly_chart(fig_map, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        with col_top:
            st.markdown('<div class="sec">Top States</div>', unsafe_allow_html=True)
            top10 = geo_df.sort_values(geo_col, ascending=False).head(10)[["STATE_CODE", geo_col, "MEDIAN_SALARY"]].copy()
            top10.columns = ["State", "Jobs", "Salary"]
            top10["Salary"] = top10["Salary"].apply(lambda x: f"${int(x)//1000}k" if pd.notna(x) and x > 0 else "—")
            top10["Jobs"]   = top10["Jobs"].apply(lambda x: f"{int(x):,}")
            st.dataframe(top10, use_container_width=True, hide_index=True, height=340)


# ══════════════════════════════════════════════════════════════════════════
# PAGE 2 — Compensation
# ══════════════════════════════════════════════════════════════════════════
elif page == "Compensation":
    st.markdown('<div class="page-title">Compensation</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">{sel_role} · salary benchmarks across seniority and geography</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi">
        <div class="kpi-val">${med_sal//1000}k</div>
        <div class="kpi-lbl">Median (All Roles)</div>
      </div>
      <div class="kpi" style="border-top-color:{C['amber']}">
        <div class="kpi-val">${top_pay_sal//1000}k</div>
        <div class="kpi-lbl">Top Role Median · {top_pay}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec">Salary by Role & Seniority</div>', unsafe_allow_html=True)
    all_sen = ["Entry Level", "Mid Level", "Senior", "Staff/Lead", "Principal"]
    sel_sen = st.multiselect("Seniority levels", all_sen, default=all_sen, label_visibility="collapsed")

    if not role_sen_f.empty:
        filtered = role_sen_f[role_sen_f["SENIORITY"].isin(sel_sen)] if sel_sen else role_sen_df
        fig = px.bar(
            filtered, x="ROLE_CLUSTER", y="MEDIAN_SALARY", color="SENIORITY",
            barmode="group",
            color_discrete_sequence=PALETTE,
            labels={"ROLE_CLUSTER": "", "MEDIAN_SALARY": "Median Salary ($)", "SENIORITY": ""},
        )
        fig.update_layout(
            yaxis=dict(tickformat="$,.0f", showgrid=True, gridcolor=C["border"]),
            xaxis=dict(showgrid=False),
            legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
            height=360, **CHART,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    col_a, col_b = st.columns(2, gap="large")

    with col_a:
        st.markdown('<div class="sec">Salary Range by Role</div>', unsafe_allow_html=True)
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
                yaxis=dict(tickformat="$,.0f", title="Annual Salary", showgrid=True, gridcolor=C["border"]),
                showlegend=False, height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    with col_b:
        st.markdown('<div class="sec">Negotiation Window (P10 → P90)</div>', unsafe_allow_html=True)
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
                yaxis=dict(tickformat="$,.0f", title="Annual Salary ($)", showgrid=True, gridcolor=C["border"]),
                xaxis=dict(showgrid=False),
                height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Median Salary by State (Top 20)</div>', unsafe_allow_html=True)

    if not geo_df.empty:
        geo_sal = geo_df[geo_df["MEDIAN_SALARY"] > 0].sort_values("MEDIAN_SALARY", ascending=False).head(20)
        fig = px.bar(
            geo_sal, x="STATE_CODE", y="MEDIAN_SALARY",
            color="MEDIAN_SALARY",
            color_continuous_scale=[[0, "#1e1e3a"], [1, "#4f46e5"]],
            labels={"STATE_CODE": "State", "MEDIAN_SALARY": "Median Salary"},
            text=geo_sal["MEDIAN_SALARY"].apply(lambda x: f"${int(x)//1000}k"),
        )
        fig.update_traces(textposition="outside", textfont_size=9)
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis=dict(tickformat="$,.0f", showgrid=True, gridcolor=C["border"]),
            xaxis=dict(showgrid=False),
            height=320, **CHART,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Median Salary by Company Tier</div>', unsafe_allow_html=True)

    if not tier_df.empty:
        TIER_COLORS = {"FAANG+": "#f59e0b", "Scale-up": "#4f46e5", "Enterprise": "#10b981", "Startup / Other": "#f43f5e"}
        tier_df["color"]      = tier_df["COMPANY_TIER"].map(TIER_COLORS)
        tier_df["salary_fmt"] = tier_df["MEDIAN_SALARY"].apply(lambda x: f"${int(x)//1000}k")
        tier_df["range_fmt"]  = tier_df.apply(
            lambda r: f"P25 ${int(r['P25_SALARY'])//1000}k — P75 ${int(r['P75_SALARY'])//1000}k", axis=1
        )

        col_tier, col_tier_r = st.columns([2, 1], gap="large")
        with col_tier:
            fig = go.Figure()
            for _, row in tier_df.iterrows():
                fig.add_trace(go.Bar(
                    x=[row["COMPANY_TIER"]],
                    y=[row["MEDIAN_SALARY"]],
                    name=row["COMPANY_TIER"],
                    marker_color=TIER_COLORS.get(row["COMPANY_TIER"], "#888"),
                    text=row["salary_fmt"],
                    textposition="outside",
                    hovertemplate=(
                        f"<b>{row['COMPANY_TIER']}</b><br>"
                        f"Median: ${int(row['MEDIAN_SALARY']):,}<br>"
                        f"P25–P75: ${int(row['P25_SALARY']):,} – ${int(row['P75_SALARY']):,}<br>"
                        f"Jobs: {int(row['JOB_COUNT']):,}<extra></extra>"
                    ),
                ))
            fig.update_layout(
                showlegend=False,
                yaxis=dict(tickformat="$,.0f", showgrid=True, gridcolor=C["border"]),
                xaxis=dict(showgrid=False),
                height=320, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        with col_tier_r:
            st.markdown('<div class="sec">By the numbers</div>', unsafe_allow_html=True)
            tbl = tier_df[["COMPANY_TIER", "MEDIAN_SALARY", "JOB_COUNT"]].copy()
            tbl.columns = ["Tier", "Median Salary", "Jobs"]
            tbl["Median Salary"] = tbl["Median Salary"].apply(lambda x: f"${int(x):,}")
            tbl["Jobs"] = tbl["Jobs"].apply(lambda x: f"{int(x):,}")
            st.dataframe(tbl, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# PAGE 3 — Skills
# ══════════════════════════════════════════════════════════════════════════
elif page == "Skills":
    role_label = sel_role if sel_role != "All Roles" else "All Roles"
    st.markdown('<div class="page-title">Skills Intelligence</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">{role_label} · demand and salary signal by technology</div>', unsafe_allow_html=True)

    NO_BAR = {"displayModeBar": False, "scrollZoom": False}

    if not active_skills.empty:
        bar_df = active_skills[active_skills["MEDIAN_SALARY"] > 0].copy()
        bar_df["skill_label"] = bar_df["SKILL"].str.title()
        bar_df["salary_fmt"]  = bar_df["MEDIAN_SALARY"].apply(lambda x: f"${int(x)//1000}k")
        bar_df = bar_df.sort_values("JOB_COUNT", ascending=True)

        n      = len(bar_df)
        FIXED_H = max(400, n * 18 + 100)   # ~18px per bar keeps it compact

        col_chart, col_sal = st.columns([2, 3], gap="large")

        with col_chart:
            st.markdown('<div class="sec">Demand — Job Count</div>', unsafe_allow_html=True)
            x_max  = bar_df["JOB_COUNT"].max()
            dtick  = 20000 if x_max > 40000 else 10000 if x_max > 10000 else 2000
            bar_df["count_fmt"] = bar_df["JOB_COUNT"].apply(
                lambda x: f"{int(x/1000)}k" if x >= 1000 else str(int(x))
            )
            fig = px.bar(
                bar_df,
                x="JOB_COUNT", y="skill_label",
                orientation="h",
                color="JOB_COUNT",
                color_continuous_scale=[[0, "#1e1e3a"], [1, "#4f46e5"]],
                labels={"JOB_COUNT": "Job Postings", "skill_label": ""},
                text="count_fmt",
                hover_data={"MEDIAN_SALARY": True, "JOB_COUNT": True, "salary_fmt": False, "count_fmt": False},
            )
            fig.update_traces(textposition="outside", textfont_size=9, marker_line_width=0)
            fig.update_layout(
                coloraxis_showscale=False,
                bargap=0.25,
                xaxis=dict(showgrid=True, gridcolor=C["border"], dtick=dtick,
                           tickformat=".0s"),
                yaxis=dict(showgrid=False, tickfont=dict(size=9)),
                height=FIXED_H, **CHART,
            )
            st.plotly_chart(fig, use_container_width=True, config=NO_BAR)

        with col_sal:
            st.markdown('<div class="sec">Median Salary by Skill</div>', unsafe_allow_html=True)
            sal_df = bar_df.sort_values("MEDIAN_SALARY", ascending=True)
            fig2 = px.bar(
                sal_df,
                x="MEDIAN_SALARY", y="skill_label",
                orientation="h",
                color="MEDIAN_SALARY",
                color_continuous_scale=[[0, "#1e2a1e"], [1, "#f59e0b"]],
                labels={"MEDIAN_SALARY": "Median Salary ($)", "skill_label": ""},
                text="salary_fmt",
            )
            fig2.update_traces(textposition="outside", textfont_size=9, marker_line_width=0)
            fig2.update_layout(
                coloraxis_showscale=False,
                bargap=0.25,
                xaxis=dict(tickformat="$,.0s", showgrid=True, gridcolor=C["border"],
                           dtick=50000),
                yaxis=dict(showgrid=False, tickfont=dict(size=9)),
                height=FIXED_H, **CHART,
            )
            st.plotly_chart(fig2, use_container_width=True, config=NO_BAR)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown(f'<div class="sec">Top 20 Skills — {role_label}</div>', unsafe_allow_html=True)

    if not active_skills.empty:
        max_count = int(active_skills["JOB_COUNT"].max())
        chips = '<div class="chip-row">'
        for _, row in active_skills.head(20).iterrows():
            name  = row["SKILL"].title()
            count = int(row["JOB_COUNT"])
            sal   = f"${int(row['MEDIAN_SALARY'])//1000}k" if pd.notna(row["MEDIAN_SALARY"]) and row["MEDIAN_SALARY"] > 0 else "—"
            pct   = int(count / max_count * 100)
            rank  = int(row["DEMAND_RANK"])
            chips += f"""<div class="chip">
              <span class="chip-rank">#{rank}</span>
              <span>{name}</span>
              <div class="chip-bar"><div class="chip-fill" style="width:{pct}%"></div></div>
              <span style="color:#3a3a50;font-size:0.65rem">{sal}</span>
            </div>"""
        chips += "</div>"
        st.markdown(chips, unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Full Table</div>', unsafe_allow_html=True)

    if not active_skills.empty:
        cols = ["DEMAND_RANK", "SKILL", "JOB_COUNT", "MEDIAN_SALARY"]
        tbl = active_skills[cols].copy()
        tbl.columns = ["Rank", "Skill", "# Jobs", "Median Salary"]
        tbl["Skill"]         = tbl["Skill"].str.title()
        tbl["Median Salary"] = tbl["Median Salary"].apply(lambda x: f"${int(x):,}" if pd.notna(x) and x > 0 else "—")
        tbl["# Jobs"]        = tbl["# Jobs"].apply(lambda x: f"{int(x):,}")
        st.dataframe(tbl, use_container_width=True, hide_index=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Skills Paired with AI / LLM Requirements</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:0.75rem;color:#3a3a50;margin-bottom:0.8rem">Among job postings that mention LLM, RAG, generative AI or vector databases — which skills appear most often alongside them?</div>', unsafe_allow_html=True)

    if not ai_cooc_df.empty:
        ai_df = ai_cooc_df.head(20).copy()
        ai_df["skill_label"] = ai_df["SKILL"].str.title()
        ai_df["pct_fmt"]     = ai_df["PCT_OF_AI_JOBS"].apply(lambda x: f"{x:.0f}%")
        ai_df = ai_df.sort_values("AI_JOB_COUNT", ascending=True)

        col_ai, col_ai_r = st.columns([3, 2], gap="large")
        with col_ai:
            fig = px.bar(
                ai_df,
                x="PCT_OF_AI_JOBS", y="skill_label",
                orientation="h",
                color="MEDIAN_SALARY",
                color_continuous_scale=[[0, "#1e1e3a"], [0.5, "#4f46e5"], [1, "#f59e0b"]],
                text="pct_fmt",
                labels={"PCT_OF_AI_JOBS": "% of AI/LLM Jobs", "skill_label": "", "MEDIAN_SALARY": "Median Salary"},
            )
            fig.update_traces(textposition="outside", textfont_size=9, marker_line_width=0)
            n_ai = len(ai_df)
            fig.update_layout(
                coloraxis_showscale=False,
                xaxis=dict(showgrid=True, gridcolor=C["border"], ticksuffix="%"),
                yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                height=max(380, n_ai * 20 + 80), **CHART,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        with col_ai_r:
            st.markdown('<div class="sec">Top Skills in AI/LLM Jobs</div>', unsafe_allow_html=True)
            ai_tbl = ai_df.sort_values("AI_JOB_COUNT", ascending=False)[
                ["skill_label", "PCT_OF_AI_JOBS", "MEDIAN_SALARY"]
            ].copy()
            ai_tbl.columns = ["Skill", "% AI Jobs", "Median Salary"]
            ai_tbl["% AI Jobs"]     = ai_tbl["% AI Jobs"].apply(lambda x: f"{x:.0f}%")
            ai_tbl["Median Salary"] = ai_tbl["Median Salary"].apply(
                lambda x: f"${int(x)//1000}k" if pd.notna(x) and x > 0 else "—"
            )
            st.dataframe(ai_tbl, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# PAGE 4 — Resume Analyzer
# ══════════════════════════════════════════════════════════════════════════
elif page == "Resume Analyzer":
    st.markdown('<div class="page-title">Resume Analyzer</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Paste your resume and a job description to get a match score and skill gap analysis.</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2, gap="large")
    with col1:
        st.markdown('<div class="sec">Your Resume</div>', unsafe_allow_html=True)
        resume_text = st.text_area("resume", height=300, placeholder="Paste your resume here...", label_visibility="collapsed")
    with col2:
        st.markdown('<div class="sec">Job Description</div>', unsafe_allow_html=True)
        jd_text = st.text_area("jd", height=300, placeholder="Paste the job description here...", label_visibility="collapsed")

    if st.button("Analyze Match", type="primary", use_container_width=True):
        if not resume_text or not jd_text:
            st.warning("Paste both your resume and the job description first.")
        else:
            api_key = _secret("ANTHROPIC_API_KEY")
            if not api_key or api_key == "your_anthropic_key_here":
                st.info("Add your Anthropic API key to enable AI analysis. Set ANTHROPIC_API_KEY in Streamlit secrets.")
            else:
                try:
                    import sys
                    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                    from ai.resume_analyzer import extract_skills_from_resume, extract_skills_from_jd, skill_gap_analysis
                    market_skills = skills_df["SKILL"].head(20).tolist() if not skills_df.empty else []

                    with st.spinner("Analyzing..."):
                        resume_profile = extract_skills_from_resume(resume_text)
                        jd_profile     = extract_skills_from_jd(jd_text)
                        gap            = skill_gap_analysis(resume_profile, jd_profile, market_skills)

                    score = gap.get("match_score", 0)
                    rec   = gap.get("application_recommendation", "—").upper()
                    score_color = C["emerald"] if score >= 0.7 else C["amber"] if score >= 0.5 else C["rose"]

                    st.markdown(f"""
                    <div class="kpi-row" style="margin-top:1.5rem">
                      <div class="kpi" style="border-top-color:{score_color}">
                        <div class="kpi-val">{score:.0%}</div>
                        <div class="kpi-lbl">Match Score</div>
                      </div>
                      <div class="kpi">
                        <div class="kpi-val">{rec}</div>
                        <div class="kpi-lbl">Recommendation</div>
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                    st.caption(gap.get("match_summary", ""))

                    col_m, col_g, col_s = st.columns(3)
                    with col_m:
                        st.markdown('<div class="sec">Matched Skills</div>', unsafe_allow_html=True)
                        for s in gap.get("matched_skills", []):
                            st.markdown(f"✓ {s}")
                    with col_g:
                        st.markdown('<div class="sec">Critical Gaps</div>', unsafe_allow_html=True)
                        for s in gap.get("missing_critical", []):
                            st.markdown(f"✗ {s}")
                    with col_s:
                        st.markdown('<div class="sec">Nice-to-Have</div>', unsafe_allow_html=True)
                        for s in gap.get("missing_preferred", []):
                            st.markdown(f"· {s}")

                    if gap.get("learning_path"):
                        st.markdown('<hr class="divider">', unsafe_allow_html=True)
                        st.markdown('<div class="sec">Learning Path</div>', unsafe_allow_html=True)
                        for item in gap["learning_path"]:
                            pri = item.get("priority", "")
                            dot = {"high": "●", "medium": "◑", "low": "○"}.get(pri, "·")
                            st.markdown(f"{dot} **{item['skill']}** — {item['reason']}")
                            st.caption(f"Resource: {item.get('resource', 'TBD')}")

                    if gap.get("tailoring_tips"):
                        st.markdown('<hr class="divider">', unsafe_allow_html=True)
                        st.markdown('<div class="sec">Resume Tailoring Tips</div>', unsafe_allow_html=True)
                        for tip in gap["tailoring_tips"]:
                            st.markdown(f"· {tip}")

                except Exception as e:
                    st.error(f"Error: {e}")

    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="sec">Learning Path Generator</div>', unsafe_allow_html=True)

    col_skills, col_role_t = st.columns([2, 1], gap="large")
    with col_skills:
        skills_input = st.text_input("Your current skills", placeholder="Python, SQL, Excel, Tableau...")
    with col_role_t:
        target_role = st.selectbox("Target role", [
            "Data Analyst", "Data Engineer", "Data Scientist",
            "ML Engineer", "AI Engineer", "Analytics Engineer"
        ])

    if st.button("Generate 90-Day Roadmap", use_container_width=True) and skills_input:
        api_key = _secret("ANTHROPIC_API_KEY")
        if not api_key or api_key == "your_anthropic_key_here":
            st.info("Add your Anthropic API key to Streamlit secrets to enable this feature.")
        else:
            try:
                import sys
                sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                from ai.resume_analyzer import generate_learning_path
                market_skills = skills_df["SKILL"].head(20).tolist() if not skills_df.empty else []
                with st.spinner("Building roadmap..."):
                    result = generate_learning_path(
                        [s.strip() for s in skills_input.split(",")],
                        target_role, market_skills
                    )

                col_r1, col_r2 = st.columns(2)
                col_r1.metric("Job Readiness", f"{result.get('readiness_score', 0)}%")
                col_r2.metric("Weeks to Job-Ready", result.get("weeks_to_job_ready", "?"))

                for step in result.get("roadmap", []):
                    with st.expander(f"Week {step['week_range']} — {step['focus']}"):
                        st.write("**Skills:**", ", ".join(step.get("skills", [])))
                        for r in step.get("resources", []):
                            st.write(f"· {r}")
                        st.success(f"Milestone: {step.get('milestone', '')}")

                if result.get("portfolio_projects"):
                    st.markdown('<div class="sec" style="margin-top:1rem">Portfolio Project Ideas</div>', unsafe_allow_html=True)
                    for proj in result["portfolio_projects"]:
                        st.markdown(f"**{proj['title']}** — {proj['description']}")
                        st.caption("Skills: " + ", ".join(proj.get("skills_demonstrated", [])))

            except Exception as e:
                st.error(f"Error: {e}")
