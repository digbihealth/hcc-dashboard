import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import json
import concurrent.futures
import threading

st.set_page_config(page_title="High Cost Claimant Dashboard", layout="wide", page_icon="💊")

st.markdown("""
<style>
  [data-testid="stAppViewContainer"], [data-testid="stHeader"] { background-color: #ffffff; }
  [data-testid="metric-container"] { background: transparent; }
  .stDataFrame thead tr th { background-color: #f0f2f6 !important; }
  div[data-testid="stMetricValue"] > div { font-size: 2rem; font-weight: 700; }
  div[data-testid="stMetricLabel"] > div { font-size: 0.78rem; color: #555; }
</style>
""", unsafe_allow_html=True)

BASE_URL           = "https://api.iterable.com/api"
LIST_ALL_HCCS      = 8865061
LIST_ENROLLED_HCCS = 8865109
CUTOFF_2026        = pd.Timestamp("2026-01-01")

def get_headers(project):
    key_map = {
        "digbi_health":  st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH", ""),
        "preenrollment": st.secrets.get("ITERABLE_KEY_PREENROLLMENT", ""),
    }
    return {"Api-Key": key_map[project]}

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_list_emails(project, list_id):
    resp = requests.get(f"{BASE_URL}/lists/getUsers", headers=get_headers(project),
                        params={"listId": list_id}, stream=True, timeout=300)
    resp.raise_for_status()
    emails = []
    for line in resp.iter_lines():
        if line:
            decoded = line.decode("utf-8") if isinstance(line, bytes) else line
            decoded = decoded.strip()
            if decoded:
                try:
                    emails.append(json.loads(decoded).get("email", ""))
                except json.JSONDecodeError:
                    emails.append(decoded)
    return [e for e in emails if e]

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(project, emails, fields):
    headers, email_list, results = get_headers(project), list(emails), []
    progress = st.progress(0, text="Loading user profiles...")
    lock, completed = threading.Lock(), [0]

    def fetch_one(email):
        try:
            r = requests.get(f"{BASE_URL}/users/{requests.utils.quote(email, safe='')}",
                             headers=headers, timeout=15)
            if r.status_code == 200:
                data_fields = r.json().get("user", {}).get("dataFields", {})
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
                progress.progress(completed[0] / len(email_list),
                                  text=f"Loading user profiles... {completed[0]:,}/{len(email_list):,}")
    progress.empty()
    return results

def parse_dates(df):
    df = df.copy()
    if "enrollmentDate" not in df.columns:
        df["date"], df["month"] = pd.NaT, None
        return df
    def coerce_ms(val):
        try:
            ms = float(val)
            return pd.NaT if pd.isna(ms) else pd.Timestamp(ms, unit="ms")
        except Exception:
            return pd.NaT
    df["enrollmentDate"] = df["enrollmentDate"].apply(coerce_ms)
    df["date"]  = df["enrollmentDate"].dt.normalize()
    df["month"] = df["enrollmentDate"].dt.to_period("M").astype(str)
    return df

with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data cached for 30 min.")

st.title("💊 High Cost Claimant Dashboard")

missing = []
if not st.secrets.get("ITERABLE_KEY_DIGBI_HEALTH"):  missing.append("`ITERABLE_KEY_DIGBI_HEALTH`")
if not st.secrets.get("ITERABLE_KEY_PREENROLLMENT"): missing.append("`ITERABLE_KEY_PREENROLLMENT`")
if missing:
    st.error(f"Missing API key(s): {', '.join(missing)} — add to Streamlit secrets.")
    st.stop()

with st.spinner("Fetching all HCC emails..."):
    all_hcc_emails = fetch_list_emails("preenrollment", LIST_ALL_HCCS)

with st.spinner("Fetching enrolled HCC emails..."):
    enrolled_emails = fetch_list_emails("digbi_health", LIST_ENROLLED_HCCS)

total_hccs     = len(all_hcc_emails)
total_enrolled = len(enrolled_emails)
enrolled_rate  = (total_enrolled / total_hccs * 100) if total_hccs else 0.0

enrolled_profiles = fetch_user_fields("digbi_health", tuple(enrolled_emails), ("enrollmentDate", "employerName"))
df_enrolled = pd.DataFrame(enrolled_profiles) if enrolled_profiles else pd.DataFrame()

if not df_enrolled.empty:
    if "employerName" not in df_enrolled.columns:
        df_enrolled["employerName"] = "Unknown"
    df_enrolled["employerName"] = df_enrolled["employerName"].fillna("Unknown").astype(str).str.strip()
    df_enrolled = parse_dates(df_enrolled)

df_2026 = df_enrolled[df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_2026)] \
          if not df_enrolled.empty else pd.DataFrame()

enrolled_2026      = len(df_2026)
enrolled_2026_rate = (enrolled_2026 / total_hccs * 100) if total_hccs else 0.0
total_employers    = df_enrolled["employerName"].nunique() if not df_enrolled.empty else 0
has_dates          = not df_enrolled.empty and df_enrolled["date"].notna().any()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Employers",          f"{total_employers:,}")
c2.metric("Total HCCs in Database",   f"{total_hccs:,}")
c3.metric("Enrolled HCCs (All-time)", f"{total_enrolled:,}", f"{enrolled_rate:.1f}% enrollment rate")
c4.metric("Enrolled HCCs 2026",       f"{enrolled_2026:,}",  f"{enrolled_2026_rate:.1f}% enrollment rate")

st.markdown("---")
st.markdown("### By Employer")

if not df_enrolled.empty:
    emp_all  = df_enrolled.groupby("employerName").size().reset_index(name="Total Enrolled (All-time)")
    emp_2026 = df_2026.groupby("employerName").size().reset_index(name="Total Enrolled 2026") \
               if not df_2026.empty else pd.DataFrame(columns=["employerName", "Total Enrolled 2026"])

    emp_df = emp_all.merge(emp_2026, on="employerName", how="left")
    emp_df["Total Enrolled 2026"]          = emp_df["Total Enrolled 2026"].fillna(0).astype(int)
    emp_df["2026 Enrollment Target (30%)"] = (emp_df["Total Enrolled (All-time)"] * 0.30).round(0).astype(int)
    emp_df["Total Enrolled 2026 %"]        = (
        emp_df["Total Enrolled 2026"] / emp_df["Total Enrolled (All-time)"].replace(0, pd.NA) * 100
    ).round(1).astype(str) + "%"
    emp_df = emp_df.sort_values("Total Enrolled (All-time)", ascending=False).reset_index(drop=True)
    emp_df = emp_df.rename(columns={"employerName": "Employer Name"})

    totals_row = pd.DataFrame([{
        "Employer Name":               "TOTAL",
        "Total Enrolled (All-time)":   emp_df["Total Enrolled (All-time)"].sum(),
        "2026 Enrollment Target (30%)": emp_df["2026 Enrollment Target (30%)"].sum(),
        "Total Enrolled 2026":         emp_df["Total Enrolled 2026"].sum(),
        "Total Enrolled 2026 %":       f"{enrolled_2026_rate:.1f}%",
    }])
    emp_display = pd.concat([emp_df, totals_row], ignore_index=True)
    st.dataframe(emp_display[["Employer Name","Total Enrolled (All-time)",
                               "2026 Enrollment Target (30%)","Total Enrolled 2026",
                               "Total Enrolled 2026 %"]],
                 use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(emp_df)} employers")

st.markdown("---")
tab1, tab2 = st.tabs(["📅 Enrollments by Month", "📆 Enrollments by Day"])

with tab1:
    if not has_dates or df_2026.empty:
        st.info("No 2026 enrollment date data available.")
    else:
        monthly = (df_2026.dropna(subset=["month"]).groupby("month").size()
                   .reset_index(name="Enrolled").sort_values("month"))
        monthly["Cumulative Enrolled"] = monthly["Enrolled"].cumsum()
        monthly["Month"] = pd.to_datetime(monthly["month"]).dt.strftime("%B %Y")
        st.markdown("#### Enrollments by Month (2026)")
        st.dataframe(monthly[["Month","Enrolled","Cumulative Enrolled"]],
                     use_container_width=True, hide_index=True)

with tab2:
    if not has_dates or df_2026.empty:
        st.info("No 2026 enrollment date data available.")
    else:
        available_months = sorted(df_2026.dropna(subset=["month"])["month"].unique())
        today         = pd.Timestamp.today().normalize()
        current_month = today.to_period("M").strftime("%Y-%m")
        default_month = current_month if current_month in available_months else available_months[-1]
        month_labels  = {m: pd.to_datetime(m).strftime("%B %Y") for m in available_months}

        selected_month = st.selectbox("Select month", options=available_months,
                                      index=available_months.index(default_month) if default_month in available_months else len(available_months)-1,
                                      format_func=lambda m: month_labels[m])

        sel_start  = pd.Timestamp(selected_month + "-01")
        sel_end    = min((sel_start + pd.offsets.MonthEnd(0)).normalize(), today)
        df_day     = df_2026[df_2026["date"].notna() & (df_2026["date"] >= sel_start) & (df_2026["date"] <= sel_end)].copy()
        all_days   = pd.date_range(sel_start, sel_end, freq="D")
        day_counts = df_day.groupby("date").size().reindex(all_days, fill_value=0).reset_index()
        day_counts.columns = ["date", "Enrolled"]
        day_counts["Cumulative Enrolled"] = day_counts["Enrolled"].cumsum()
        day_counts["Day"] = day_counts["date"].dt.strftime("%a, %b %d")

        st.markdown(f"#### Daily Enrollments — {month_labels[selected_month]}")
        st.dataframe(day_counts[["Day","Enrolled","Cumulative Enrolled"]],
                     use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(f"Last loaded: {datetime.now().strftime('%b %d, %Y %I:%M %p')} · Source: Iterable API")
