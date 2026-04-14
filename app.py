import streamlit as st
import pandas as pd
import datetime
from databricks import sql
import os
import plotly.express as px

# Set page configuration for a wide, professional layout
st.set_page_config(layout="wide", page_title="Non-Prod to Prod Bridge Tracker", page_icon="🛡️")

# --- CUSTOM CSS FOR STYLING ---
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    div[data-testid="stExpander"] {
        background-color: #ffffff;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# --- APP HEADER ---
st.title("🛡️ Non-Prod to Prod Bridge Tracker")
st.info("""
This dashboard identifies users active in both **Non-Production** and **Production** workspaces. 
Bridging environments can be a security risk if not strictly managed.
""")

# --- SIDEBAR FILTERS ---
st.sidebar.header("🗓️ Filter Options")
days_range = st.sidebar.slider("Analysis Window (Days)", 1, 90, 90)

# Search filter for specific users
user_search = st.sidebar.text_input("🔍 Search User Email", "")

# --- THE VERIFIED SQL QUERY ---
# We parameterize the interval (days) to match the slider
QUERY = f"""
WITH v_workspace_environment AS (
    SELECT
        account_id,
        CAST(workspace_id AS STRING) AS workspace_id,
        workspace_name,
        workspace_url,
        status,
        CASE
            WHEN lower(workspace_name) RLIKE '(^|[-_])(p|prod|production)($|[-_])'
                THEN 'PROD'
            WHEN lower(workspace_name) RLIKE '(^|[-_])(d|dev|development|q|qa|test|tst|uat|stage|stg|b)($|[-_])'
                THEN 'NON_PROD'
            ELSE 'UNKNOWN'
        END AS env_type
    FROM system.access.workspaces_latest
    WHERE status = 'RUNNING'
),
v_user_workspace_activity_90d AS (
    SELECT
        a.account_id,
        CAST(a.workspace_id AS STRING) AS workspace_id,
        a.event_time,
        a.user_identity.email AS user_email,
        a.service_name,
        a.action_name,
        w.workspace_name,
        w.workspace_url,
        w.env_type
    FROM system.access.audit a
    INNER JOIN v_workspace_environment w
        ON CAST(a.workspace_id AS STRING) = w.workspace_id
    WHERE a.user_identity.email IS NOT NULL
      AND CAST(a.workspace_id AS STRING) <> '0'
      AND w.env_type IN ('PROD', 'NON_PROD')
      AND a.event_time >= current_timestamp() - INTERVAL {days_range} DAYS
),
v_user_env_summary_90d AS (
    SELECT
        user_email,
        env_type,
        COLLECT_SET(workspace_name) AS workspace_names,
        MIN(event_time) AS first_seen,
        MAX(event_time) AS last_seen,
        COUNT(*) AS total_events,
        COUNT(DISTINCT workspace_id) AS workspace_count
    FROM v_user_workspace_activity_90d
    GROUP BY user_email, env_type
)
SELECT
    np.user_email,
    np.workspace_names AS non_prod_workspaces,
    p.workspace_names  AS prod_workspaces,
    np.workspace_count AS non_prod_workspace_count,
    p.workspace_count  AS prod_workspace_count,
    np.total_events    AS non_prod_events,
    p.total_events     AS prod_events,
    np.first_seen      AS non_prod_first_seen,
    np.last_seen       AS non_prod_last_seen,
    p.first_seen       AS prod_first_seen,
    p.last_seen        AS prod_last_seen
FROM v_user_env_summary_90d np
INNER JOIN v_user_env_summary_90d p
    ON np.user_email = p.user_email
WHERE np.env_type = 'NON_PROD'
  AND p.env_type = 'PROD'
ORDER BY prod_last_seen DESC, prod_events DESC
"""

@st.cache_data(ttl=300)
def get_bridging_data():
    try:
        host = os.getenv("DATABRICKS_HOST")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        token = os.getenv("DATABRICKS_TOKEN")

        if not host or not http_path:
            st.warning("⚠️ **App Configuration Required**: Please set the `DATABRICKS_HTTP_PATH` environment variable.")
            return pd.DataFrame()

        with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute(QUERY)
                result = cursor.fetchall()
                if not result:
                    return pd.DataFrame()
                df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
                return df
    except Exception as e:
        st.error(f"❌ **Connection Error**: {e}")
        return pd.DataFrame()

# Fetch and filter data
df = get_bridging_data()

if not df.empty:
    if user_search:
        df = df[df['user_email'].str.contains(user_search, case=False)]

    # --- TOP KPI METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Bridging Users", len(df))
    col2.metric("Total Prod Events", f"{df['prod_events'].sum():,}")
    col3.metric("Avg Prod WS/User", round(df['prod_workspace_count'].mean(), 1))
    col4.metric("Avg Non-Prod WS/User", round(df['non_prod_workspace_count'].mean(), 1))

    st.markdown("---")

    # --- VISUALIZATIONS ---
    v1, v2 = st.columns(2)

    with v1:
        st.subheader("🔥 Top 10 High-Activity Bridgers")
        # Bubble chart showing Prod Events vs Non-Prod Events
        fig = px.scatter(df.head(10), 
                         x="non_prod_events", 
                         y="prod_events", 
                         size="prod_workspace_count", 
                         hover_name="user_email",
                         color="prod_workspace_count",
                         labels={"non_prod_events": "Non-Prod Actions", "prod_events": "Prod Actions"},
                         title="Activity Density (Prod vs Non-Prod)")
        st.plotly_chart(fig, use_container_width=True)

    with v2:
        st.subheader("🌐 Workspace Distribution")
        # Bar chart showing count of Prod Workspaces per user
        ws_dist = df['prod_workspace_count'].value_counts().reset_index()
        ws_dist.columns = ['Workspaces', 'User Count']
        fig2 = px.bar(ws_dist, x='Workspaces', y='User Count', 
                      title="Users by # of Prod Workspaces Accessed",
                      color_discrete_sequence=['#ff4b4b'])
        st.plotly_chart(fig2, use_container_width=True)

    # --- DATA EXPLORATION TABLE ---
    st.subheader("🔍 Bridging User Details")
    st.markdown("Sorted by **Latest Production Activity**")
    
    # Format dates for display
    display_df = df.copy()
    display_df['prod_last_seen'] = pd.to_datetime(display_df['prod_last_seen']).dt.strftime('%Y-%m-%d %H:%M')
    
    st.dataframe(display_df[[
        'user_email', 'prod_last_seen', 'prod_events', 'non_prod_events', 
        'prod_workspaces', 'non_prod_workspaces'
    ]], use_container_width=True)

    # --- USER SEARCH / DEEP DIVE ---
    if not df.empty:
        st.sidebar.markdown("---")
        selected_user = st.sidebar.selectbox("🎯 Select User for Deep Dive", df['user_email'].tolist())
        
        if selected_user:
            user_data = df[df['user_email'] == selected_user].iloc[0]
            with st.expander(f"📌 Detailed Timeline for {selected_user}", expanded=False):
                d1, d2 = st.columns(2)
                with d1:
                    st.write("**Production Activity**")
                    st.write(f"First Seen: {user_data['prod_first_seen']}")
                    st.write(f"Last Seen: {user_data['prod_last_seen']}")
                    st.write(f"Workspaces: `{user_data['prod_workspaces']}`")
                with d2:
                    st.write("**Non-Production Activity**")
                    st.write(f"First Seen: {user_data['non_prod_first_seen']}")
                    st.write(f"Last Seen: {user_data['non_prod_last_seen']}")
                    st.write(f"Workspaces: `{user_data['non_prod_workspaces']}`")

else:
    st.info("✅ No bridging activity detected in the selected timeframe.")

st.sidebar.markdown("---")
st.sidebar.caption("Powered by Databricks System Tables (system.access.audit)")
