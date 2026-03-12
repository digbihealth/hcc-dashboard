import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="High Cost Claimant Dashboard", layout="wide")

# ── Force light mode ─────────────────────────────────────────────────────────
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
BASE_URL              = "https://api.iterable.com/api"
LIST_ALL_HCCS         = 8865061   # Digbi_preEnrollment — all HCCs
LIST_ENROLLED_HCCS    = 8865109   # Digbi Health       — enrolled HCCs
CUTOFF_2026           = pd.Timestamp("2026-01-01")

# ── Auth ──────────────────────────────────────────────────────────────────────
def get_headers(project: str) -> dict:
    key_map = {
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH", ""),
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
    }
    return {"Api-Key": key_map[project], "Content-Type": "application/json"}

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

# ── Batch fetch user profiles ─────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(project: str, emails: tuple, fields: tuple) -> list:
    headers    = get_headers(project)
    results    = []
    batch_size = 100
    email_list = list(emails)
    progress   = st.progress(0, text="Loading user profiles…")

    for i in range(0, len(email_list), batch_size):
        batch = email_list[i:i + batch_size]
        try:
            resp = requests.post(
                f"{BASE_URL}/users/bulkGet",
                headers=headers,
                json={"emails": batch, "fields": list(fields)},
                timeout=30,
            )
            if resp.status_code == 200:
                for u in resp.json().get("users", []):
                    row = {"email": u.get("email", "")}
                    row.update(u.get("dataFields", {}))
                    results.append(row)
            else:
                for email in batch:
                    results.append({"email": email})
        except Exception:
            for email in batch:
                results.append({"email": email})

        progress.progress(
            min(1.0, (i + batch_size) / max(len(email_list), 1)),
            text=f"Loading profiles… {min(i + batch_size, len(email_list))}/{len(email_list)}"
        )

    progress.empty()
    return results

# ── Parse UNIX ms enrollment dates ───────────────────────────────────────────
def parse_enrollment_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "enrollmentDate" not in df.columns:
        df["enrollmentDate"] = pd.NaT
        df["date"]           = pd.NaT
        df["month"]          = None
        df["day_of_month"]   = None
        return df

    def coerce(val):
        if pd.isna(val) or val == "" or val is None:
            return pd.NaT
        try:
            ms = float(val)
            return pd.Timestamp(ms, unit="ms")
        except Exception:
            try:
                return pd.to_datetime(val, errors="coerce")
            except Exception:
                return pd.NaT

    df["enrollmentDate"] = df["enrollmentDate"].apply(coerce)
    df["date"]           = df["enrollmentDate"].dt.normalize()
    df["month"]          = df["enrollmentDate"].dt.to_period("M").astype(str)
    df["day_of_month"]   = df["enrollmentDate"].dt.day
    return df

# ── Load data ─────────────────────────────────────────────────────────────────
def load_data():
    with st.spinner("Fetching HCC list from Iterable…"):
        all_hcc_emails      = fetch_list_emails("preenrollment", LIST_ALL_HCCS)
        enrolled_hcc_emails = fetch_list_emails("digbi_health",  LIST_ENROLLED_HCCS)

    # Profile fields for enrolled users
    enrolled_fields = ("enrollmentDate", "employerName", "claimCost")
    with st.spinner("Fetching enrolled HCC profiles…"):
        enrolled_profiles = fetch_user_fields(
            "digbi_health",
            tuple(enrolled_hcc_emails),
            enrolled_fields,
        )

    # Profile fields for ALL HCCs (employer breakdown)
    all_fields = ("employerName", "claimCost")
    with st.spinner("Fetching all HCC profiles…"):
        all_profiles = fetch_user_fields(
            "preenrollment",
            tuple(all_hcc_emails),
            all_fields,
        )

    df_all      = pd.DataFrame(all_profiles)      if all_profiles      else pd.DataFrame()
    df_enrolled = pd.DataFrame(enrolled_profiles) if enrolled_profiles else pd.DataFrame()

    # Clean employer name
    for df in [df_all, df_enrolled]:
        if "employerName" not in df.columns:
            df["employerName"] = "Unknown"
        df["employerName"] = df["employerName"].fillna("Unknown").astype(str).str.strip()

    # Parse dates on enrolled
    df_enrolled = parse_enrollment_dates(df_enrolled)

    # 2026 filter — only users with a known enrollment date on or after Jan 1 2026
    df_enrolled_2026 = df_enrolled[
        df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_2026)
    ]

    return df_all, df_enrolled, df_enrolled_2026

# ── Main ──────────────────────────────────────────────────────────────────────
st.title("💊 High Cost Claimant Dashboard")

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ── Load ──────────────────────────────────────────────────────────────────────
df_all, df_enrolled, df_enrolled_2026 = load_data()

total_hccs        = len(df_all)
total_enrolled    = len(df_enrolled)
enrolled_rate     = (total_enrolled / total_hccs * 100) if total_hccs else 0.0
enrolled_2026     = len(df_enrolled_2026)
enrolled_2026_rate = (enrolled_2026 / total_hccs * 100) if total_hccs else 0.0
# Employer count comes from Digbi Health enrolled profiles — preEnrollment doesn't carry employerName
total_employers   = df_enrolled["employerName"].nunique() if not df_enrolled.empty else 0

# ── KPI Tiles ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Employers",          f"{total_employers:,}")
c2.metric("Total HCCs in Database",   f"{total_hccs:,}")
c3.metric("Enrolled HCCs (All-time)", f"{total_enrolled:,}",  f"{enrolled_rate:.1f}% enrollment rate")
c4.metric("Enrolled HCCs 2026",       f"{enrolled_2026:,}",   f"{enrolled_2026_rate:.1f}% enrollment rate")

st.markdown("---")

# ── Client Summary Table ──────────────────────────────────────────────────────
st.markdown("### By Employer")

if not df_enrolled.empty:
    # Base: all enrolled HCCs grouped by employer (Digbi Health carries employerName)
    employer_total = (
        df_enrolled.groupby("employerName")
        .size()
        .reset_index(name="Total Enrolled")
    )

    # Enrolled 2026 per employer
    employer_enrolled_2026 = (
        df_enrolled_2026.groupby("employerName")
        .size()
        .reset_index(name="Total Enrolled 2026")
    ) if not df_enrolled_2026.empty else pd.DataFrame(columns=["employerName", "Total Enrolled 2026"])

    employer_df = employer_total.merge(employer_enrolled_2026, on="employerName", how="left")
    employer_df["Total Enrolled 2026"] = employer_df["Total Enrolled 2026"].fillna(0).astype(int)
    employer_df["2026 Enrollment Target (30%)"] = (employer_df["Total Enrolled"] * 0.30).round(0).astype(int)
    employer_df["Total Enrolled 2026 %"] = (
        employer_df["Total Enrolled 2026"] / employer_df["Total Enrolled"] * 100
    ).round(1).astype(str) + "%"

    employer_df = employer_df.sort_values("Total Enrolled", ascending=False).reset_index(drop=True)
    employer_df = employer_df.rename(columns={"employerName": "Employer Name"})

    # Totals row
    totals = pd.DataFrame([{
        "Employer Name":                "TOTAL",
        "Total Enrolled":               employer_df["Total Enrolled"].sum(),
        "Total Enrolled 2026":          employer_df["Total Enrolled 2026"].sum(),
        "2026 Enrollment Target (30%)": employer_df["2026 Enrollment Target (30%)"].sum(),
        "Total Enrolled 2026 %":        f"{enrolled_2026_rate:.1f}%",
    }])
    employer_display = pd.concat([employer_df, totals], ignore_index=True)

    display_cols = [
        "Employer Name", "Total Enrolled",
        "2026 Enrollment Target (30%)", "Total Enrolled 2026",
        "Total Enrolled 2026 %",
    ]
    st.dataframe(
        employer_display[display_cols],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(employer_df)} employers")

st.markdown("---")

# ── Tabs: By Month | By Day of Month ─────────────────────────────────────────
tab1, tab2 = st.tabs(["📅 Enrollments by Month", "📆 Enrollments by Day (Current Month)"])

# ── TAB 1: By Month ───────────────────────────────────────────────────────────
with tab1:
    if df_enrolled_2026.empty or "month" not in df_enrolled_2026.columns:
        st.info("No enrollment date data available.")
    else:
        monthly = (
            df_enrolled_2026.dropna(subset=["month"])
            .groupby("month")
            .size()
            .reset_index(name="Enrolled")
            .sort_values("month")
        )
        monthly["Cumulative Enrolled"] = monthly["Enrolled"].cumsum()
        monthly["Month"] = pd.to_datetime(monthly["month"]).dt.strftime("%B %Y")

        display_monthly = monthly[["Month", "Enrolled", "Cumulative Enrolled"]].copy()
        st.markdown("#### Enrollments by Month")
        st.dataframe(display_monthly, use_container_width=True, hide_index=True)

        col_a, col_b = st.columns(2)
        with col_a:
            fig_bar = px.bar(
                monthly, x="Month", y="Enrolled",
                title="Monthly HCC Enrollments",
                color_discrete_sequence=["#4C78A8"],
            )
            fig_bar.update_layout(xaxis_title="", yaxis_title="Enrolled", plot_bgcolor="white")
            st.plotly_chart(fig_bar, use_container_width=True)
        with col_b:
            fig_cum = px.area(
                monthly, x="Month", y="Cumulative Enrolled",
                title="Cumulative HCC Enrollments",
                color_discrete_sequence=["#72B7B2"],
            )
            fig_cum.update_layout(xaxis_title="", yaxis_title="Cumulative", plot_bgcolor="white")
            st.plotly_chart(fig_cum, use_container_width=True)

# ── TAB 2: By Day (Current Month) ────────────────────────────────────────────
with tab2:
    today      = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    month_label = today.strftime("%B %Y")

    if df_enrolled_2026.empty or "date" not in df_enrolled_2026.columns:
        st.info("No enrollment date data available.")
    else:
        df_month = df_enrolled_2026[
            df_enrolled_2026["date"].notna() &
            (df_enrolled_2026["date"] >= month_start) &
            (df_enrolled_2026["date"] <= today)
        ].copy()

        # Build full day range for current month up to today
        all_days = pd.date_range(month_start, today, freq="D")
        day_counts = (
            df_month.groupby("date").size()
            .reindex(all_days, fill_value=0)
            .reset_index()
        )
        day_counts.columns = ["date", "Enrolled"]
        day_counts["Cumulative Enrolled"] = day_counts["Enrolled"].cumsum()
        day_counts["Day"] = day_counts["date"].dt.strftime("%a, %b %d")

        st.markdown(f"#### Daily Enrollments — {month_label}")
        display_daily = day_counts[["Day", "Enrolled", "Cumulative Enrolled"]].copy()
        st.dataframe(display_daily, use_container_width=True, hide_index=True)

        col_a, col_b = st.columns(2)
        with col_a:
            fig_day_bar = px.bar(
                day_counts, x="Day", y="Enrolled",
                title=f"Daily HCC Enrollments — {month_label}",
                color_discrete_sequence=["#4C78A8"],
            )
            fig_day_bar.update_layout(
                xaxis_title="", yaxis_title="Enrolled",
                plot_bgcolor="white",
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig_day_bar, use_container_width=True)
        with col_b:
            fig_day_cum = px.area(
                day_counts, x="Day", y="Cumulative Enrolled",
                title=f"Cumulative HCC Enrollments — {month_label}",
                color_discrete_sequence=["#72B7B2"],
            )
            fig_day_cum.update_layout(
                xaxis_title="", yaxis_title="Cumulative",
                plot_bgcolor="white",
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig_day_cum, use_container_width=True)
