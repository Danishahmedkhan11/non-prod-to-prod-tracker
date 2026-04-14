import streamlit as st
import pandas as pd
import datetime
from databricks import sql
import os
import sys
import subprocess

# --- SELF-STARTING WRAPPER ---
# If this script is run via 'python app.py', relaunch it with 'streamlit run app.py'
def ensure_streamlit():
    if "streamlit" not in sys.argv[0] and "run" not in sys.argv:
        print("Relaunching app with streamlit...")
        subprocess.run([
            "streamlit", "run", sys.argv[0], 
            "--server.port", "8080", 
            "--server.address", "0.0.0.0"
        ])
        sys.exit()

if __name__ == "__main__":
    if "STREAMLIT_SERVER_PORT" not in os.environ:
        ensure_streamlit()
# -----------------------------

# Set page configuration
st.set_page_config(layout="wide", page_title="Non-Prod to Prod Connection Tracker")

# App Header
st.title("🛡️ Non-Prod to Prod Connection Tracker")
st.markdown("""
This app monitors and highlights access patterns where users in **Non-Production** workspaces (Dev, QA, DR) 
are querying **Production** catalogs. 
- **Cross-Env Access**: Access from `-d-` (Dev) or `-q-` (QA) to `*_prod`.
- **Monitored (DR)**: Access from `-r-` (DR) to `*_prod`.
""")

# Sidebar Filters
st.sidebar.header("Filter Options")
today = datetime.date.today()
start_date = st.sidebar.date_input("Start Date", today - datetime.timedelta(days=30))
end_date = st.sidebar.date_input("End Date", today)

env_filter = st.sidebar.multiselect(
    "Source Environments",
    ["DEV", "QA", "DR"],
    default=["DEV", "QA", "DR"]
)

# SQL Query Construction
# Note: workspace name split_part(workspace_name, '-', 3) extracts 'd', 'q', or 'r'
QUERY = """
WITH workspace_env AS (
    SELECT 
        workspace_id,
        workspace_name,
        CASE 
            WHEN split_part(workspace_name, '-', 3) = 'd' THEN 'DEV'
            WHEN split_part(workspace_name, '-', 3) = 'q' THEN 'QA'
            WHEN split_part(workspace_name, '-', 3) = 'r' THEN 'DR'
            WHEN split_part(workspace_name, '-', 3) = 'p' THEN 'PROD'
            ELSE 'OTHER'
        END AS env_type
    FROM system.access.workspaces_latest
),
lineage_access AS (
    SELECT 
        l.event_time,
        l.workspace_id AS source_workspace_id,
        l.user_identity.email AS user_email,
        split_part(l.source_table_full_name, '.', 1) AS catalog_name,
        l.source_table_full_name AS table_name
    FROM system.access.table_lineage l
    WHERE split_part(l.source_table_full_name, '.', 1) LIKE '%_prod'
      AND l.event_time BETWEEN :start_date AND :end_date
)
SELECT 
    a.event_time,
    w.workspace_name AS source_workspace,
    w.env_type AS source_env,
    a.user_email,
    a.catalog_name,
    a.table_name,
    CASE 
        WHEN w.env_type = 'DR' THEN 'Monitored (DR)'
        ELSE 'Cross-Env Access'
    END AS status
FROM lineage_access a
JOIN workspace_env w ON a.source_workspace_id = w.workspace_id
WHERE w.env_type IN ('DEV', 'QA', 'DR')
ORDER BY a.event_time DESC
"""

def get_data():
    try:
        # These environment variables are automatically set in a Databricks App
        host = os.getenv("DATABRICKS_HOST")
        http_path = os.getenv("DATABRICKS_HTTP_PATH") # The SQL Warehouse to use
        token = os.getenv("DATABRICKS_TOKEN")

        if not host or not http_path:
            st.warning("⚠️ **Missing Configuration**: Please set the `DATABRICKS_HTTP_PATH` environment variable in the App settings (e.g., `/sql/1.0/warehouses/123456789`).")
            return pd.DataFrame()

        # Connect using the Service Principal identity provided to the app
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(QUERY, {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d")
                })
                result = cursor.fetchall()
                if not result:
                    return pd.DataFrame()
                df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
                return df
    except Exception as e:
        st.error(f"❌ **Connection Error**: {e}")
        return pd.DataFrame()

# Data Fetching
@st.cache_data(ttl=300) # Cache data for 5 minutes
def cached_fetch(start, end):
    return get_data()

df = cached_fetch(start_date, end_date)

if not df.empty:
    # Client-side environment filter
    filtered_df = df[df['source_env'].isin(env_filter)]

    # KPI Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Connections", len(filtered_df))
    c2.metric("Cross-Env Access (D/Q)", len(filtered_df[filtered_df['source_env'].isin(['DEV', 'QA'])]))
    c3.metric("Monitored DR Access", len(filtered_df[filtered_df['source_env'] == 'DR']))

    # Visualizations
    st.subheader("📊 Connection Trends")
    filtered_df['date'] = pd.to_datetime(filtered_df['event_time']).dt.date
    trend_data = filtered_df.groupby(['date', 'status']).size().reset_index(name='count')
    st.line_chart(trend_data.pivot(index='date', columns='status', values='count').fillna(0))

    # Detailed Table
    st.subheader("🔍 Detailed Access Logs")
    st.dataframe(filtered_df, use_container_width=True)

    # Top Offenders / Active Users
    st.subheader("👤 Top Users by Environment")
    col1, col2 = st.columns(2)
    with col1:
        st.write("Top Cross-Env Users (Dev/QA)")
        top_users = filtered_df[filtered_df['source_env'].isin(['DEV', 'QA'])]['user_email'].value_counts().head(5)
        st.bar_chart(top_users)
    with col2:
        st.write("Most Accessed Prod Catalogs")
        top_catalogs = filtered_df['catalog_name'].value_counts().head(5)
        st.bar_chart(top_catalogs)

else:
    st.info("No connections found matching the criteria in the selected date range.")

st.sidebar.markdown("---")
st.sidebar.info("Developed for Databricks Non-Prod to Prod Monitoring.")
