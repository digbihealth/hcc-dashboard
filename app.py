import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import json
import concurrent.futures
import threading

st.set_page_config(page_title="High Cost Claimant Dashboard", layout="wide", page_icon="💊")

# ── Force light mode ──────────────────────────────────────────────────────────
st.markdown("""
<style>
  [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
    background-color: #ffffff;
  }
  [data-testid="metric-container"] { background: transparent; }
  .stDataFrame thead tr th { background-color: #f0f2f6 !important; }
  div[data-testid="stMetricValue"] > div { font-size: 2rem; font-weight: 700; }
  div[data-testid="stMetricLabel"] > div { font-size: 0.78rem; color: #555; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL           = "https://api.iterable.com/api"
LIST_ALL_HCCS      = 8865061   # Digbi_preEnrollment — all HCCs
LIST_ENROLLED_HCCS = 8865109   # Digbi Health — enrolled HCCs
CUTOFF_2026        = pd.Timestamp("2026-01-01")

# ── Auth ──────────────────────────────────────────────────────────────────────
def get_headers(project: str) -> dict:
    key_map = {
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH", ""),
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
    }
    return {"Api-Key": key_map[project]}

# ── Fetch emails from a list ──────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_list_emails(project: str, list_id: int) -> list:
    resp = requests.get(
        f"{BASE_URL}/lists/getUsers",
        headers=get_headers(project),
        params={"listId": list_id},
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    emails = []
    for line in resp.iter_lines():
        if line:
            decoded = line.decode("utf-8") if isinstance(line, bytes) else line
            decoded = decoded.strip()
            if decoded:
                try:
                    obj = json.loads(decoded)
                    email = obj.get("email", "")
                    if email:
                        emails.append(email)
                except json.JSONDecodeError:
                    emails.append(decoded)
    return emails

# ── Fetch profiles — threaded GET /users/{email}, fields at top level ─────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(project: str, emails: tuple, fields: tuple) -> list:
    headers    = get_headers(project)
    email_list = list(emails)
    results    = []
    progress   = st.progress(0, text="Loading user profiles...")
    lock       = threading.Lock()
    completed  = [0]

    def fetch_one(email):
        try:
            r = requests.get(
                f"{BASE_URL}/users/{requests.utils.quote(email, safe='')}",
                headers=headers,
                timeout=15,
            )
            if r.status_code == 200:
                user = r.json().get("user", {})
                # Fields are inside dataFields when using GET /users/{email}
                data_fields = user.get("dataFields", {})
                row = {"email": email}
                for f in fields:
                    row[f] = data_fields.get(f)
                return row
        except Exception:
            pass
        return {"email": email}

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_one, e): e for e in email_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            with lock:
                completed[0] += 1
                pct = completed[0] / len(email_list)
                progress.progress(pct, text=f"Loading user profiles... {completed[0]:,}/{len(email_list):,}")

    progress.empty()
    return results

# ── Parse enrollmentDate — UNIX milliseconds integer e.g. 1768499984000 ───────
# NOTE: Never use enrollmentDateFormatted — unreliable across users in Iterable
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "enrollmentDate" not in df.columns:
        df["date"]  = pd.NaT
        df["month"] = None
        return df
    def coerce_ms(val):
        """Convert UNIX ms integer to Timestamp safely — avoids FloatingPointError on overflow."""
        try:
            ms = float(val)
            if pd.isna(ms):
                return pd.NaT
            return pd.Timestamp(ms, unit="ms")
        except Exception:
            return pd.NaT

    df["enrollmentDate"] = df["enrollmentDate"].apply(coerce_ms)
    df["date"]  = df["enrollmentDate"].dt.normalize()
    df["month"] = df["enrollmentDate"].dt.to_period("M").astype(str)
    return df

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data cached for 30 min.")

# ── Title ─────────────────────────────────────────────────────────────────────
st.title("💊 High Cost Claimant Dashboard")

# ── Key check ─────────────────────────────────────────────────────────────────
missing = []
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):   missing.append("`ITERABLE_KEY_DIGBI_HEALTH`")
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"):  missing.append("`ITERABLE_KEY_PREENROLLMENT`")
if missing:
    st.error(f"Missing API key(s): {', '.join(missing)} — add to Streamlit secrets.")
    st.stop()

# ── Fetch email lists ─────────────────────────────────────────────────────────
with st.spinner("Fetching all HCC emails (preEnrollment)..."):
    try:
        all_hcc_emails = fetch_list_emails("preenrollment", LIST_ALL_HCCS)
    except Exception as e:
        st.error(f"Error fetching HCC list: {e}")
        st.stop()

with st.spinner("Fetching enrolled HCC emails (Digbi Health)..."):
    try:
        enrolled_emails = fetch_list_emails("digbi_health", LIST_ENROLLED_HCCS)
    except Exception as e:
        st.error(f"Error fetching enrolled list: {e}")
        st.stop()

total_hccs     = len(all_hcc_emails)
total_enrolled = len(enrolled_emails)
enrolled_rate  = (total_enrolled / total_hccs * 100) if total_hccs else 0.0

# ── Fetch enrolled profiles ───────────────────────────────────────────────────
enrolled_profiles = fetch_user_fields(
    "digbi_health",
    tuple(enrolled_emails),
    ("enrollmentDate", "employerName"),
)

df_enrolled = pd.DataFrame(enrolled_profiles) if enrolled_profiles else pd.DataFrame()

# ── DEBUG — remove once data is confirmed correct ─────────────────────────────
with st.expander("🔍 Debug: Raw profile data (first 5 rows)", expanded=True):
    st.write(f"Total profiles returned: {len(df_enrolled)}")
    st.write(f"Columns present: {list(df_enrolled.columns)}")
    if not df_enrolled.empty:
        st.dataframe(df_enrolled.head(5))
# ── END DEBUG ─────────────────────────────────────────────────────────────────

if not df_enrolled.empty:
    df_enrolled["employerName"] = df_enrolled.get("employerName", pd.Series(dtype=str))
    df_enrolled["employerName"] = df_enrolled["employerName"].fillna("Unknown").astype(str).str.strip()
    df_enrolled = parse_dates(df_enrolled)
else:
    df_enrolled["employerName"] = "Unknown"

# 2026 enrolled — only rows with a known date >= Jan 1 2026
df_2026 = df_enrolled[
    df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_2026)
] if not df_enrolled.empty else pd.DataFrame()

enrolled_2026      = len(df_2026)
enrolled_2026_rate = (enrolled_2026 / total_hccs * 100) if total_hccs else 0.0
total_employers    = df_enrolled["employerName"].nunique() if not df_enrolled.empty else 0

has_dates = not df_enrolled.empty and "date" in df_enrolled.columns and df_enrolled["date"].notna().any()

# ── KPI Tiles ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Employers",          f"{total_employers:,}")
c2.metric("Total HCCs in Database",   f"{total_hccs:,}")
c3.metric("Enrolled HCCs (All-time)", f"{total_enrolled:,}", f"{enrolled_rate:.1f}% enrollment rate")
c4.metric("Enrolled HCCs 2026",       f"{enrolled_2026:,}",  f"{enrolled_2026_rate:.1f}% enrollment rate")

st.markdown("---")

# ── Employer Table ────────────────────────────────────────────────────────────
st.markdown("### By Employer")

if not df_enrolled.empty:
    emp_all = (
        df_enrolled.groupby("employerName").size().reset_index(name="Total Enrolled")
    )
    emp_2026 = (
        df_2026.groupby("employerName").size().reset_index(name="Total Enrolled 2026")
    ) if not df_2026.empty else pd.DataFrame(columns=["employerName", "Total Enrolled 2026"])

    emp_df = emp_all.merge(emp_2026, on="employerName", how="left")
    emp_df["Total Enrolled 2026"] = emp_df["Total Enrolled 2026"].fillna(0).astype(int)
    emp_df["2026 HCC Enrollment Target (30%)"] = (emp_df["Total Enrolled"] * 0.30).round(0).astype(int)
    emp_df["Total Enrolled 2026 %"] = (
        emp_df["Total Enrolled 2026"] / emp_df["Total Enrolled"].replace(0, pd.NA) * 100
    ).round(1).astype(str) + "%"
    emp_df = emp_df.sort_values("Total Enrolled", ascending=False).reset_index(drop=True)
    emp_df = emp_df.rename(columns={"employerName": "Employer Name"})

    totals_row = pd.DataFrame([{
        "Employer Name":                   "TOTAL",
        "Total Enrolled":                  emp_df["Total Enrolled"].sum(),
        "2026 HCC Enrollment Target (30%)": emp_df["2026 HCC Enrollment Target (30%)"].sum(),
        "Total Enrolled 2026":             emp_df["Total Enrolled 2026"].sum(),
        "Total Enrolled 2026 %":           f"{enrolled_2026_rate:.1f}%",
    }])
    emp_display = pd.concat([emp_df, totals_row], ignore_index=True)

    st.dataframe(
        emp_display[[
            "Employer Name", "Total Enrolled",
            "2026 HCC Enrollment Target (30%)", "Total Enrolled 2026",
            "Total Enrolled 2026 %",
        ]],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(emp_df)} employers")

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📅 Enrollments by Month", "📆 Enrollments by Day (Current Month)"])

# ── TAB 1: By Month ───────────────────────────────────────────────────────────
with tab1:
    if not has_dates or df_2026.empty:
        st.info("No 2026 enrollment date data available.")
    else:
        monthly = (
            df_2026.dropna(subset=["month"])
            .groupby("month").size()
            .reset_index(name="Enrolled")
            .sort_values("month")
        )
        monthly["Cumulative Enrolled"] = monthly["Enrolled"].cumsum()
        monthly["Month"] = pd.to_datetime(monthly["month"]).dt.strftime("%B %Y")

        st.markdown("#### Enrollments by Month")
        st.dataframe(
            monthly[["Month", "Enrolled", "Cumulative Enrolled"]],
            use_container_width=True, hide_index=True,
        )

        col_a, col_b = st.columns(2)
        with col_a:
            fig_bar = px.bar(monthly, x="Month", y="Enrolled",
                             title="Monthly HCC Enrollments",
                             color_discrete_sequence=["#4F86C6"])
            fig_bar.update_layout(xaxis_title="", yaxis_title="Enrolled",
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_bar, use_container_width=True)
        with col_b:
            fig_cum = px.area(monthly, x="Month", y="Cumulative Enrolled",
                              title="Cumulative HCC Enrollments",
                              color_discrete_sequence=["#5CB85C"])
            fig_cum.update_layout(xaxis_title="", yaxis_title="Cumulative",
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_cum, use_container_width=True)

# ── TAB 2: By Day (Current Month) ────────────────────────────────────────────
with tab2:
    today       = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    month_label = today.strftime("%B %Y")

    if not has_dates:
        st.info("No enrollment date data available.")
    else:
        df_month = df_2026[
            df_2026["date"].notna() &
            (df_2026["date"] >= month_start) &
            (df_2026["date"] <= today)
        ].copy() if not df_2026.empty else pd.DataFrame()

        all_days = pd.date_range(month_start, today, freq="D")
        if not df_month.empty:
            day_counts = (
                df_month.groupby("date").size()
                .reindex(all_days, fill_value=0)
                .reset_index()
            )
        else:
            day_counts = pd.DataFrame({"index": all_days, 0: 0}).rename(columns={"index": "date", 0: "Enrolled"})
        day_counts.columns = ["date", "Enrolled"]
        day_counts["Cumulative Enrolled"] = day_counts["Enrolled"].cumsum()
        day_counts["Day"] = day_counts["date"].dt.strftime("%a, %b %d")

        st.markdown(f"#### Daily Enrollments — {month_label}")
        st.dataframe(
            day_counts[["Day", "Enrolled", "Cumulative Enrolled"]],
            use_container_width=True, hide_index=True,
        )

        col_a, col_b = st.columns(2)
        with col_a:
            fig_day = px.bar(day_counts, x="Day", y="Enrolled",
                             title=f"Daily HCC Enrollments — {month_label}",
                             color_discrete_sequence=["#4F86C6"])
            fig_day.update_layout(xaxis_title="", yaxis_title="Enrolled",
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                  xaxis_tickangle=-45)
            st.plotly_chart(fig_day, use_container_width=True)
        with col_b:
            fig_day_cum = px.area(day_counts, x="Day", y="Cumulative Enrolled",
                                  title=f"Cumulative HCC Enrollments — {month_label}",
                                  color_discrete_sequence=["#5CB85C"])
            fig_day_cum.update_layout(xaxis_title="", yaxis_title="Cumulative",
                                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                      xaxis_tickangle=-45)
            st.plotly_chart(fig_day_cum, use_container_width=True)

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
