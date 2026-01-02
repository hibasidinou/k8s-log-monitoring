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
# Removed creative visualizations - keeping dashboard simple and focused on Spark data


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
    
    # Check Spark/Kafka connection status
    spark_status = check_spark_connection(data_loader)
    
    # Sidebar navigation
    st.sidebar.title("🚀 Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Alerts", "Server Status"]
    )
    
    # Display Spark/Kafka status
    st.sidebar.markdown("---")
    st.sidebar.subheader("📡 Pipeline Status")
    if spark_status['spark_active']:
        st.sidebar.success(f"✅ Spark: {spark_status['status']}")
        if spark_status['last_update']:
            st.sidebar.caption(f"Last update: {spark_status['last_update']}")
    else:
        st.sidebar.error("❌ Spark: Not receiving data")
        st.sidebar.caption("Start Spark pipeline to see data")
    
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
        show_overview_page(data_loader, anomalies_df, server_status_df, alerts, metrics_df, statistics, spark_status)
    elif page == "Alerts":
        show_alerts_page(data_loader, alerts, anomalies_df, spark_status)
    elif page == "Server Status":
        show_server_status_page(data_loader, server_status_df, anomalies_df, selected_server, spark_status)


def check_spark_connection(data_loader):
    """Check if Spark is actively processing data."""
    from pathlib import Path
    import os
    from datetime import datetime
    
    output_dir = Path("data/output")
    status = {
        'spark_active': False,
        'status': 'No data',
        'last_update': None,
        'anomalies_count': 0,
        'servers_count': 0
    }
    
    # Check if files exist and have recent modifications
    anomalies_file = output_dir / "anomalies_detected.json"
    server_status_file = output_dir / "dashboard" / "server_status.parquet"
    
    if anomalies_file.exists():
        try:
            import json
            with open(anomalies_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
                    if isinstance(data, list) and len(data) > 0:
                        status['anomalies_count'] = len(data)
                        status['spark_active'] = True
                        status['status'] = f'Active ({len(data)} anomalies)'
                        
                        # Get file modification time
                        mod_time = os.path.getmtime(anomalies_file)
                        status['last_update'] = datetime.fromtimestamp(mod_time).strftime("%H:%M:%S")
        except:
            pass
    
    if server_status_file.exists():
        try:
            import pandas as pd
            df = pd.read_parquet(server_status_file)
            if not df.empty:
                status['servers_count'] = len(df)
                if not status['spark_active']:
                    status['spark_active'] = True
                    status['status'] = f'Active ({len(df)} servers)'
        except:
            pass
    
    return status


def show_overview_page(data_loader, anomalies_df, server_status_df, alerts, metrics_df, statistics, spark_status):
    """Display overview page with key metrics and summary."""
    st.header("📊 Overview")
    
    # Spark Status Banner
    if spark_status['spark_active']:
        st.success(f"✅ **Spark Pipeline Active** - {spark_status['anomalies_count']} anomalies, {spark_status['servers_count']} servers | Last update: {spark_status['last_update'] or 'N/A'}")
    else:
        st.warning("⚠️ **Spark Pipeline Not Active** - Start Spark streaming to see real-time data from Kafka")
        st.info("💡 **Tip:** Run `python scripts/run_spark_streaming.py` to start processing Kafka logs")
    
    st.markdown("---")
    
    # KPI Cards
    display_kpi_cards(statistics, anomalies_df, server_status_df)
    
    st.markdown("---")
    
    # Server Health Summary
    if not server_status_df.empty:
        st.subheader("🖥️ Server Status")
        display_server_health_summary(server_status_df)
    else:
        st.info("No server data available. Spark needs to process data from Kafka first.")
    
    st.markdown("---")
    
    # Recent Alerts
    if alerts:
        st.subheader("🚨 Recent Alerts")
        display_alerts_panel(alerts[:10], max_display=10)
    else:
        st.info("No alerts yet. Alerts will appear here when Spark detects anomalies.")


def show_server_status_page(data_loader, server_status_df, anomalies_df, selected_server, spark_status):
    """Display server status page."""
    st.header("🖥️ Server Status")
    
    # Show Spark status
    if spark_status['spark_active']:
        st.success(f"✅ Spark processing data | {spark_status['servers_count']} servers monitored")
    else:
        st.warning("⚠️ Spark not active - Server status not updating")
    
    st.markdown("---")
    
    if not server_status_df.empty:
        # Server Map
        display_server_map(server_status_df, anomalies_df)
        
        st.markdown("---")
        
        # Server Status Table
        st.subheader("📋 Server Status Details")
        st.dataframe(server_status_df, width='stretch', hide_index=True)
        
        # Server Details
        if selected_server != "All":
            st.markdown("---")
            display_server_details(selected_server, server_status_df, anomalies_df)
    else:
        st.warning("⚠️ **Aucune donnée serveur disponible**")
        st.markdown("""
        Spark doit traiter des données depuis Kafka pour générer le statut des serveurs.
        
        **Étapes pour démarrer :**
        
        1. **Vérifier Kafka** (déjà fait ✅) :
           ```powershell
           docker ps --filter "name=kafka"
           ```
        
        2. **Démarrer Spark Streaming** (Nouveau Terminal) :
           ```powershell
           python scripts/run_spark_streaming.py
           ```
           ⚠️ **Laissez ce terminal ouvert !**
        
        3. **Envoyer des logs à Kafka** (Nouveau Terminal) :
           ```powershell
           python src/data_collection/stream_logs.py
           ```
        
        **Résultat attendu :**
        - Spark traite les logs en temps réel
        - Les statuts des serveurs apparaissent ici
        - Les anomalies sont détectées automatiquement
        """)


# Removed show_creative_views_page and show_server_map_page - simplified to show_server_status_page


def show_alerts_page(data_loader, alerts, anomalies_df, spark_status):
    """Display alerts page."""
    st.header("🚨 Alerts & Anomalies")
    
    # Show Spark status
    if spark_status['spark_active']:
        st.success(f"✅ Receiving data from Spark | {spark_status['anomalies_count']} anomalies detected")
    else:
        st.warning("⚠️ Spark not active - No new data from Kafka")
    
    st.markdown("---")
    
    # Alerts Panel
    if alerts:
        display_alerts_panel(alerts)
    else:
        st.info("""
        **Aucune alerte disponible.** 
        
        Les alertes sont générées par Spark lorsqu'il traite les logs depuis Kafka.
        
        **Solution :** Démarrer Spark Streaming et envoyer des logs à Kafka (voir instructions ci-dessus).
        """)
    
    st.markdown("---")
    
    # Anomalies Table
    if not anomalies_df.empty:
        st.subheader("📋 All Anomalies")
        display_anomalies_table(anomalies_df)
    else:
        st.warning("⚠️ **Aucune anomalie détectée**")
        st.markdown("""
        **Pourquoi ce message ?**
        
        Spark n'a pas encore traité de données depuis Kafka. Pour voir des anomalies :
        
        1. **Démarrer Spark Streaming** (Terminal 2) :
           ```bash
           python scripts/run_spark_streaming.py
           ```
        
        2. **Envoyer des logs à Kafka** (Terminal 3) :
           ```bash
           python src/data_collection/stream_logs.py
           ```
        
        3. **Attendre quelques secondes** - Spark traitera les logs automatiquement
        
        **Vérification :**
        - Kafka doit être démarré : `docker ps --filter "name=kafka"`
        - Spark doit être en cours d'exécution (Terminal 2 ouvert)
        - Les logs doivent être envoyés (Terminal 3 ouvert)
        """)


# Removed show_timeline_page, show_metrics_page, show_server_details_page - simplified dashboard


if __name__ == "__main__":
    main()

