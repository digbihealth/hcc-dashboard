import streamlit as st
import requests
import pandas as pd
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

# ── Fetch profiles — threaded, fields inside dataFields ──────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(project: str, emails: tuple, fields: tuple, label: str = "user profiles") -> list:
    headers    = get_headers(project)
    email_list = list(emails)
    results    = []
    progress   = st.progress(0, text=f"Loading {label}...")
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
                user        = r.json().get("user", {})
                data_fields = user.get("dataFields", {})  # all custom fields live here
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
                progress.progress(pct, text=f"Loading {label}... {completed[0]:,}/{len(email_list):,}")

    progress.empty()
    return results

# ── Parse enrollmentDate — UNIX ms integer e.g. 1768499984000 ─────────────────
# NEVER use enrollmentDateFormatted — it is not reliably updated across users
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "enrollmentDate" not in df.columns:
        df["date"]  = pd.NaT
        df["month"] = None
        return df

    def coerce_ms(val):
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
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):  missing.append("`ITERABLE_KEY_DIGBI_HEALTH`")
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"): missing.append("`ITERABLE_KEY_PREENROLLMENT`")
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

# ── Fetch profiles for enrolled HCCs (Digbi Health) ──────────────────────────
enrolled_profiles = fetch_user_fields(
    "digbi_health",
    tuple(enrolled_emails),
    ("enrollmentDate", "employerName"),
    label="enrolled HCC profiles",
)
df_enrolled = pd.DataFrame(enrolled_profiles) if enrolled_profiles else pd.DataFrame()
if not df_enrolled.empty:
    df_enrolled["employerName"] = df_enrolled["employerName"].fillna("Unknown").astype(str).str.strip()
    df_enrolled = parse_dates(df_enrolled)

# ── Fetch profiles for ALL HCCs (preEnrollment) — for employer HCC counts ────
all_hcc_profiles = fetch_user_fields(
    "preenrollment",
    tuple(all_hcc_emails),
    ("employerName",),
    label="all HCC profiles",
)
df_all = pd.DataFrame(all_hcc_profiles) if all_hcc_profiles else pd.DataFrame()
if not df_all.empty:
    if "employerName" not in df_all.columns:
        df_all["employerName"] = "Unknown"
    df_all["employerName"] = df_all["employerName"].fillna("Unknown").astype(str).str.strip()

# ── 2026 enrolled ─────────────────────────────────────────────────────────────
df_2026 = df_enrolled[
    df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_2026)
] if not df_enrolled.empty else pd.DataFrame()

enrolled_2026      = len(df_2026)
enrolled_2026_rate = (enrolled_2026 / total_hccs * 100) if total_hccs else 0.0
total_employers    = df_all["employerName"].nunique() if not df_all.empty else 0
has_dates          = not df_enrolled.empty and df_enrolled["date"].notna().any()

# ── KPI Tiles ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Employers",          f"{total_employers:,}")
c2.metric("Total HCCs in Database",   f"{total_hccs:,}")
c3.metric("Enrolled HCCs (All-time)", f"{total_enrolled:,}", f"{enrolled_rate:.1f}% enrollment rate")
c4.metric("Enrolled HCCs 2026",       f"{enrolled_2026:,}",  f"{enrolled_2026_rate:.1f}% enrollment rate")

st.markdown("---")

# ── Employer Table ────────────────────────────────────────────────────────────
st.markdown("### By Employer")

if not df_all.empty and not df_enrolled.empty:
    # Total HCCs per employer — from preEnrollment (full HCC universe)
    hcc_by_employer = df_all.groupby("employerName").size().reset_index(name="Total HCCs")

    # All-time enrolled per employer — from Digbi Health
    enrolled_by_employer = df_enrolled.groupby("employerName").size().reset_index(name="Total Enrolled (All-time)")

    # 2026 enrolled per employer
    enrolled_2026_by_employer = (
        df_2026.groupby("employerName").size().reset_index(name="Total Enrolled 2026")
    ) if not df_2026.empty else pd.DataFrame(columns=["employerName", "Total Enrolled 2026"])

    emp_df = (
        hcc_by_employer
        .merge(enrolled_by_employer,      on="employerName", how="left")
        .merge(enrolled_2026_by_employer, on="employerName", how="left")
    )
    emp_df["Total Enrolled (All-time)"] = emp_df["Total Enrolled (All-time)"].fillna(0).astype(int)
    emp_df["Total Enrolled 2026"]       = emp_df["Total Enrolled 2026"].fillna(0).astype(int)
    emp_df["2026 HCC Enrollment Target (30%)"] = (emp_df["Total HCCs"] * 0.30).round(0).astype(int)
    emp_df["Total Enrolled 2026 %"] = (
        emp_df["Total Enrolled 2026"] / emp_df["Total HCCs"].replace(0, pd.NA) * 100
    ).round(1).astype(str) + "%"

    emp_df = emp_df.sort_values("Total HCCs", ascending=False).reset_index(drop=True)
    emp_df = emp_df.rename(columns={"employerName": "Employer Name"})

    totals_row = pd.DataFrame([{
        "Employer Name":                    "TOTAL",
        "Total HCCs":                       emp_df["Total HCCs"].sum(),
        "2026 HCC Enrollment Target (30%)": emp_df["2026 HCC Enrollment Target (30%)"].sum(),
        "Total Enrolled (All-time)":        emp_df["Total Enrolled (All-time)"].sum(),
        "Total Enrolled 2026":              emp_df["Total Enrolled 2026"].sum(),
        "Total Enrolled 2026 %":            f"{enrolled_2026_rate:.1f}%",
    }])
    emp_display = pd.concat([emp_df, totals_row], ignore_index=True)

    st.dataframe(
        emp_display[[
            "Employer Name", "Total HCCs",
            "2026 HCC Enrollment Target (30%)",
            "Total Enrolled 2026", "Total Enrolled 2026 %",
            "Total Enrolled (All-time)",
        ]],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(emp_df)} employers")

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📅 Enrollments by Month", "📆 Enrollments by Day"])

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

        st.markdown("#### Enrollments by Month (2026)")
        st.dataframe(
            monthly[["Month", "Enrolled", "Cumulative Enrolled"]],
            use_container_width=True, hide_index=True,
        )

# ── TAB 2: By Day (month selector) ───────────────────────────────────────────
with tab2:
    if not has_dates or df_2026.empty:
        st.info("No 2026 enrollment date data available.")
    else:
        # Build list of months that have any data
        available_months = sorted(df_2026.dropna(subset=["month"])["month"].unique())
        today            = pd.Timestamp.today().normalize()
        current_month    = today.to_period("M").strftime("%Y-%m")

        # Default to current month if available, else latest month with data
        default_month = current_month if current_month in available_months else (available_months[-1] if available_months else current_month)
        month_labels  = {m: pd.to_datetime(m).strftime("%B %Y") for m in available_months}

        selected_month = st.selectbox(
            "Select month",
            options=available_months,
            index=available_months.index(default_month) if default_month in available_months else len(available_months) - 1,
            format_func=lambda m: month_labels[m],
        )

        sel_start = pd.Timestamp(selected_month + "-01")
        sel_end   = (sel_start + pd.offsets.MonthEnd(0)).normalize()
        # Don't show future days
        sel_end   = min(sel_end, today)

        df_day = df_2026[
            df_2026["date"].notna() &
            (df_2026["date"] >= sel_start) &
            (df_2026["date"] <= sel_end)
        ].copy()

        all_days = pd.date_range(sel_start, sel_end, freq="D")
        day_counts = (
            df_day.groupby("date").size()
            .reindex(all_days, fill_value=0)
            .reset_index()
        )
        day_counts.columns = ["date", "Enrolled"]
        day_counts["Cumulative Enrolled"] = day_counts["Enrolled"].cumsum()
        day_counts["Day"] = day_counts["date"].dt.strftime("%a, %b %d")

        st.markdown(f"#### Daily Enrollments — {month_labels[selected_month]}")
        st.dataframe(
            day_counts[["Day", "Enrolled", "Cumulative Enrolled"]],
            use_container_width=True, hide_index=True,
        )

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
