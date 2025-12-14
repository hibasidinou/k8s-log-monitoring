"""
Main Streamlit application for Kubernetes log monitoring dashboard.
"""

import streamlit as st
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dashboard.utils.data_loader import DataLoader
from src.dashboard.components.metrics import (
    display_kpi_cards,
    display_server_health_summary,
    display_time_series_metrics
)
from src.dashboard.components.alerts_panel import (
    display_alerts_panel,
    display_anomalies_table
)
from src.dashboard.components.timeline import (
    display_incident_timeline,
    display_hourly_incident_heatmap
)
from src.dashboard.components.server_map import (
    display_server_map,
    display_server_details
)
from src.dashboard.components.creative_visualizations import (
    create_3d_scatter,
    create_animated_timeline,
    create_sunburst_chart,
    create_heatmap_calendar,
    create_network_graph,
    create_radar_chart
)


# Page configuration
st.set_page_config(
    page_title="K8s Log Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern, creative styling
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .main-header {
        font-size: 3rem;
        font-weight: 900;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 2rem;
        text-shadow: 0 2px 10px rgba(102, 126, 234, 0.3);
    }
    
    .stMetric {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
        transition: transform 0.3s ease;
    }
    
    .stMetric:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
    }
    
    .stMetric label {
        font-size: 0.9rem;
        font-weight: 600;
        color: #4a5568;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stMetric [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #2d3748;
    }
    
    .main .block-container {
        padding-top: 3rem;
        padding-bottom: 3rem;
    }
    
    h1, h2, h3 {
        color: #2d3748;
        font-weight: 700;
    }
    
    .stSidebar {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    .stSidebar .stSelectbox label,
    .stSidebar .stRadio label,
    .stSidebar .stCheckbox label {
        color: white !important;
        font-weight: 600;
    }
    
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    /* Animated background */
    body {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        background-attachment: fixed;
    }
    
    /* Card styling */
    [data-testid="stExpander"] {
        background: white;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 5px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    </style>
""", unsafe_allow_html=True)


def main():
    """Main application function."""
    
    # Initialize data loader
    data_loader = DataLoader()
    
    # Creative Header with gradient
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 15px; margin-bottom: 2rem; box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);">
        <h1 style="color: white; font-size: 3rem; font-weight: 900; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.2);">
            📊 Kubernetes Log Monitoring Dashboard
        </h1>
        <p style="color: rgba(255,255,255,0.9); font-size: 1.2rem; margin-top: 0.5rem;">
            Real-time monitoring • Anomaly detection • Performance analytics
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar navigation
    st.sidebar.title("🚀 Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Creative Views", "Server Map", "Alerts", "Timeline", "Metrics", "Server Details"]
    )
    
    # Sidebar filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("Filters")
    
    # Server filter
    servers = data_loader.get_server_list()
    selected_server = st.sidebar.selectbox(
        "Select Server",
        ["All"] + servers
    )
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Load data
    with st.spinner("Loading data..."):
        anomalies_df = data_loader.load_anomalies()
        server_status_df = data_loader.load_server_status()
        alerts = data_loader.load_latest_alerts()
        metrics_df = data_loader.load_aggregated_metrics()
        statistics = data_loader.load_statistics()
    
    # Route to appropriate page
    if page == "Overview":
        show_overview_page(data_loader, anomalies_df, server_status_df, alerts, metrics_df, statistics)
    elif page == "Creative Views":
        show_creative_views_page(server_status_df, anomalies_df, metrics_df)
    elif page == "Server Map":
        show_server_map_page(data_loader, server_status_df, anomalies_df, selected_server)
    elif page == "Alerts":
        show_alerts_page(data_loader, alerts, anomalies_df)
    elif page == "Timeline":
        show_timeline_page(data_loader, anomalies_df, selected_server)
    elif page == "Metrics":
        show_metrics_page(data_loader, metrics_df, server_status_df, selected_server)
    elif page == "Server Details":
        show_server_details_page(data_loader, server_status_df, anomalies_df, metrics_df, selected_server)


def show_overview_page(data_loader, anomalies_df, server_status_df, alerts, metrics_df, statistics):
    """Display overview page with key metrics and summary."""
    st.header("📊 Overview")
    
    # KPI Cards
    display_kpi_cards(statistics, anomalies_df, server_status_df)
    
    st.markdown("---")
    
    # Server Health Summary
    col1, col2 = st.columns([2, 1])
    
    with col1:
        display_server_health_summary(server_status_df)
    
    with col2:
        st.subheader("Quick Stats")
        if not anomalies_df.empty:
            if 'severity' in anomalies_df.columns:
                severity_counts = anomalies_df['severity'].value_counts()
                st.bar_chart(severity_counts)
    
    st.markdown("---")
    
    # Recent Alerts
    if alerts:
        st.subheader("🚨 Recent Alerts")
        display_alerts_panel(alerts[:10], max_display=10)
    
    # Time Series Overview
    if not metrics_df.empty:
        st.markdown("---")
        display_time_series_metrics(metrics_df)


def show_creative_views_page(server_status_df, anomalies_df, metrics_df):
    """Display creative visualizations page."""
    st.header("🎨 Creative Visualizations")
    st.markdown("### Explore your data through innovative and interactive visualizations")
    
    # 3D Scatter Plot
    st.subheader("🌐 3D Server Status Visualization")
    st.markdown("Interactive 3D view of server metrics (Errors, Warnings, Anomalies)")
    fig_3d = create_3d_scatter(server_status_df, anomalies_df)
    if fig_3d:
        st.plotly_chart(fig_3d, width='stretch')
    else:
        st.info("No server data available for 3D visualization.")
    
    st.markdown("---")
    
    # Network Graph and Sunburst
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🕸️ Server Network Topology")
        st.markdown("Network view of server relationships")
        fig_network = create_network_graph(server_status_df, anomalies_df)
        if fig_network:
            st.plotly_chart(fig_network, width='stretch')
        else:
            st.info("No server data available.")
    
    with col2:
        st.subheader("☀️ Server Hierarchy Sunburst")
        st.markdown("Hierarchical view of servers by status")
        fig_sunburst = create_sunburst_chart(server_status_df, anomalies_df)
        if fig_sunburst:
            st.plotly_chart(fig_sunburst, width='stretch')
        else:
            st.info("No server data available.")
    
    st.markdown("---")
    
    # Animated Timeline
    st.subheader("⏱️ Animated Incident Timeline")
    st.markdown("Dynamic timeline with bubble sizes representing severity")
    fig_timeline = create_animated_timeline(anomalies_df)
    if fig_timeline:
        st.plotly_chart(fig_timeline, width='stretch')
    else:
        st.info("No anomaly data available for timeline.")
    
    st.markdown("---")
    
    # Calendar Heatmap
    st.subheader("📅 Anomaly Calendar Heatmap")
    st.markdown("Daily anomaly distribution across time")
    fig_calendar = create_heatmap_calendar(anomalies_df)
    if fig_calendar:
        st.plotly_chart(fig_calendar, width='stretch')
    else:
        st.info("No anomaly data available for calendar heatmap.")
    
    # Radar Chart for server performance (if we have metrics)
    if not server_status_df.empty and len(server_status_df) > 0:
        st.markdown("---")
        st.subheader("📡 Server Performance Radar")
        
        # Select a server for radar chart
        servers = server_status_df['server_id'].tolist() if 'server_id' in server_status_df.columns else []
        if servers:
            selected_server_radar = st.selectbox("Select server for performance radar", servers)
            
            if selected_server_radar:
                server_row = server_status_df[server_status_df['server_id'] == selected_server_radar]
                if not server_row.empty:
                    # Create performance metrics
                    perf_data = {
                        'Availability': min(100, (1 - server_row.iloc[0].get('error_count', 0) / max(server_row.iloc[0].get('total_logs', 1), 1)) * 100),
                        'Stability': max(0, 100 - server_row.iloc[0].get('error_count', 0) * 10),
                        'Performance': max(0, 100 - server_row.iloc[0].get('warning_count', 0) * 5),
                        'Health': 100 if server_row.iloc[0].get('status', '') == 'healthy' else 50
                    }
                    
                    fig_radar = create_radar_chart(perf_data, f"Performance: {selected_server_radar}")
                    st.plotly_chart(fig_radar, width='stretch')


def show_server_map_page(data_loader, server_status_df, anomalies_df, selected_server):
    """Display server map page with enhanced visualizations."""
    st.header("🗺️ Server Map")
    st.markdown("### Interactive server status visualization")
    
    # Add 3D view option
    view_type = st.radio("View Type", ["2D Map", "3D Visualization"], horizontal=True)
    
    if view_type == "3D Visualization":
        fig_3d = create_3d_scatter(server_status_df, anomalies_df)
        if fig_3d:
            st.plotly_chart(fig_3d, width='stretch')
        else:
            st.info("No server data available.")
    else:
        display_server_map(server_status_df, anomalies_df)
    
    st.markdown("---")
    
    # Server status table with better styling
    st.subheader("📋 Server Status Table")
    if not server_status_df.empty:
        st.dataframe(
            server_status_df.style.background_gradient(subset=['error_count', 'warning_count'], cmap='Reds'),
            width='stretch',
            hide_index=True
        )


def show_alerts_page(data_loader, alerts, anomalies_df):
    """Display alerts page."""
    st.header("🚨 Alerts & Anomalies")
    
    # Alerts Panel
    display_alerts_panel(alerts)
    
    st.markdown("---")
    
    # Anomalies Table
    display_anomalies_table(anomalies_df)


def show_timeline_page(data_loader, anomalies_df, selected_server):
    """Display timeline page with enhanced visualizations."""
    st.header("📅 Incident Timeline")
    st.markdown("### Visualize incidents and anomalies over time")
    
    # Filter by server if selected
    server_filter = None if selected_server == "All" else selected_server
    
    # View type selector
    view_type = st.radio("Timeline View", ["Standard Timeline", "Animated Timeline", "Calendar Heatmap"], horizontal=True)
    
    if view_type == "Animated Timeline":
        # Filter anomalies if server selected
        if server_filter and not anomalies_df.empty and 'server_id' in anomalies_df.columns:
            filtered_anomalies = anomalies_df[anomalies_df['server_id'] == server_filter]
        else:
            filtered_anomalies = anomalies_df
        
        fig_animated = create_animated_timeline(filtered_anomalies)
        if fig_animated:
            st.plotly_chart(fig_animated, width='stretch')
        else:
            st.info("No anomaly data available.")
    elif view_type == "Calendar Heatmap":
        # Filter anomalies if server selected
        if server_filter and not anomalies_df.empty and 'server_id' in anomalies_df.columns:
            filtered_anomalies = anomalies_df[anomalies_df['server_id'] == server_filter]
        else:
            filtered_anomalies = anomalies_df
        
        fig_calendar = create_heatmap_calendar(filtered_anomalies)
        if fig_calendar:
            st.plotly_chart(fig_calendar, width='stretch')
        else:
            st.info("No anomaly data available.")
    else:
        # Standard timeline
        display_incident_timeline(anomalies_df, server_filter)
    
    st.markdown("---")
    
    # Hourly Heatmap
    display_hourly_incident_heatmap(anomalies_df)


def show_metrics_page(data_loader, metrics_df, server_status_df, selected_server):
    """Display metrics page."""
    st.header("📈 Metrics & Analytics")
    
    # Filter by server if selected
    server_filter = None if selected_server == "All" else selected_server
    
    # Time Series Metrics
    display_time_series_metrics(metrics_df, server_filter)
    
    st.markdown("---")
    
    # Additional metrics visualizations
    if not metrics_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Log Count Over Time")
            if 'log_count' in metrics_df.columns and 'timestamp' in metrics_df.columns:
                st.line_chart(metrics_df.set_index('timestamp')['log_count'])
        
        with col2:
            st.subheader("Error Rate Over Time")
            if 'error_rate' in metrics_df.columns:
                st.line_chart(metrics_df.set_index('timestamp')['error_rate'])


def show_server_details_page(data_loader, server_status_df, anomalies_df, metrics_df, selected_server):
    """Display server details page."""
    if selected_server == "All":
        st.info("Please select a specific server from the sidebar to view details.")
        return
    
    display_server_details(selected_server, server_status_df, anomalies_df, metrics_df)


if __name__ == "__main__":
    main()

