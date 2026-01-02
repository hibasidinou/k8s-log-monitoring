"""
Metrics widgets for displaying key performance indicators.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional
import plotly.graph_objects as go


def create_gauge_chart(value: float, max_value: float, title: str, color: str = "#667eea"):
    """Create a modern gauge/speedometer chart."""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title, 'font': {'size': 16, 'color': '#2d3748'}},
        delta = {'reference': max_value * 0.7},
        gauge = {
            'axis': {'range': [None, max_value]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, max_value * 0.5], 'color': "lightgray"},
                {'range': [max_value * 0.5, max_value * 0.8], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    fig.update_layout(
        paper_bgcolor="white",
        font={'color': "#2d3748", 'family': "Inter"},
        height=200,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig


def display_kpi_cards(metrics: Dict, anomalies_df: pd.DataFrame, server_status_df: pd.DataFrame):
    """
    Display modern KPI cards with key metrics and gauge charts.
    
    Args:
        metrics: Dictionary with aggregated metrics
        anomalies_df: DataFrame with anomalies
        server_status_df: DataFrame with server status
    """
    col1, col2, col3, col4 = st.columns(4)
    
    # Total Logs Processed - Utiliser server_status_df en priorité
    if not server_status_df.empty and 'total_logs' in server_status_df.columns:
        total_logs = int(server_status_df['total_logs'].sum())
    elif not server_status_df.empty and 'anomaly_count' in server_status_df.columns:
        # Estimer total_logs basé sur anomaly_count (approximation: ~10 logs par anomalie)
        total_logs = int(server_status_df['anomaly_count'].sum() * 10)
    else:
        total_logs = metrics.get('total_logs', 0)
    
    with col1:
        st.markdown("### 📊 Total Logs")
        st.markdown(f"<h1 style='text-align: center; color: #667eea; font-size: 2.5rem; margin: 0;'>{total_logs:,}</h1>", unsafe_allow_html=True)
        if total_logs > 0:
            gauge = create_gauge_chart(min(total_logs, 1000000), 1000000, "", "#667eea")
            st.plotly_chart(gauge, width='stretch', config={'displayModeBar': False})
    
    # Total Anomalies Detected - Utiliser anomaly_count de server_status_df en priorité
    if not server_status_df.empty and 'anomaly_count' in server_status_df.columns:
        total_anomalies = int(server_status_df['anomaly_count'].sum())
    elif not anomalies_df.empty:
        total_anomalies = len(anomalies_df)
    else:
        total_anomalies = metrics.get('total_anomalies', 0)
    
    with col2:
        st.markdown("### ⚠️ Anomalies")
        st.markdown(f"<h1 style='text-align: center; color: #ff0044; font-size: 2.5rem; margin: 0;'>{total_anomalies:,}</h1>", unsafe_allow_html=True)
        if total_anomalies > 0:
            max_anomalies = max(total_anomalies * 2, 1000)
            gauge = create_gauge_chart(total_anomalies, max_anomalies, "", "#ff0044")
            st.plotly_chart(gauge, width='stretch', config={'displayModeBar': False})
    
    # Servers Monitored
    servers_count = len(server_status_df) if not server_status_df.empty else metrics.get('servers_monitored', 10)
    with col3:
        st.markdown("### 🖥️ Servers")
        st.markdown(f"<h1 style='text-align: center; color: #00ff88; font-size: 2.5rem; margin: 0;'>{servers_count}</h1>", unsafe_allow_html=True)
        if servers_count > 0:
            gauge = create_gauge_chart(servers_count, 20, "", "#00ff88")
            st.plotly_chart(gauge, width='stretch', config={'displayModeBar': False})
    
    # Error Rate - Calculer avec les vraies données
    if not server_status_df.empty:
        if 'error_count' in server_status_df.columns and 'total_logs' in server_status_df.columns:
            total_errors = server_status_df['error_count'].sum()
            total_logs_for_rate = server_status_df['total_logs'].sum()
            error_rate = (total_errors / total_logs_for_rate * 100) if total_logs_for_rate > 0 else 0
        elif 'error_count' in server_status_df.columns:
            # Estimer error_rate basé sur error_count et anomaly_count
            total_errors = server_status_df['error_count'].sum()
            estimated_logs = total_logs if total_logs > 0 else (server_status_df['anomaly_count'].sum() * 10)
            error_rate = (total_errors / estimated_logs * 100) if estimated_logs > 0 else 0
        elif 'anomaly_count' in server_status_df.columns:
            # Estimer error_rate basé sur anomaly_count (approximation: 30% des anomalies sont des erreurs)
            estimated_errors = server_status_df['anomaly_count'].sum() * 0.3
            estimated_logs = total_logs if total_logs > 0 else (server_status_df['anomaly_count'].sum() * 10)
            error_rate = (estimated_errors / estimated_logs * 100) if estimated_logs > 0 else 0
        else:
            error_rate = 0
    else:
        error_rate = 0
    
    with col4:
        st.markdown("### 📈 Error Rate")
        st.markdown(f"<h1 style='text-align: center; color: #ffaa00; font-size: 2.5rem; margin: 0;'>{error_rate:.2f}%</h1>", unsafe_allow_html=True)
        gauge = create_gauge_chart(error_rate, 100, "", "#ffaa00")
        st.plotly_chart(gauge, width='stretch', config={'displayModeBar': False})


def display_server_health_summary(server_status_df: pd.DataFrame):
    """
    Display summary of server health status.
    
    Args:
        server_status_df: DataFrame with server status information
    """
    if server_status_df.empty:
        st.info("No server status data available.")
        return
    
    st.subheader("🖥️ Server Health Summary")
    
    # Count servers by status
    if 'status' in server_status_df.columns:
        status_counts = server_status_df['status'].value_counts()
        
        col1, col2, col3 = st.columns(3)
        
        healthy = status_counts.get('healthy', 0)
        warning = status_counts.get('warning', 0)
        critical = status_counts.get('critical', 0)
        
        with col1:
            st.metric("✅ Healthy", healthy)
        with col2:
            st.metric("⚠️ Warning", warning)
        with col3:
            st.metric("🔴 Critical", critical)
    
    # Display server status table
    display_cols = ['server_id', 'status', 'total_logs', 'error_count', 'warning_count']
    available_cols = [col for col in display_cols if col in server_status_df.columns]
    
    if available_cols:
        st.dataframe(
            server_status_df[available_cols],
            width='stretch',
            hide_index=True
        )


def display_time_series_metrics(metrics_df: pd.DataFrame, server_id: Optional[str] = None):
    """
    Display time series metrics chart.
    
    Args:
        metrics_df: DataFrame with time series metrics
        server_id: Optional server ID to filter
    """
    if metrics_df.empty:
        st.info("No metrics data available.")
        return
    
    st.subheader("📊 Time Series Metrics")
    
    # Filter by server if specified
    if server_id and 'server_id' in metrics_df.columns:
        filtered_df = metrics_df[metrics_df['server_id'] == server_id].copy()
    else:
        filtered_df = metrics_df.copy()
    
    if filtered_df.empty:
        st.warning(f"No data available for server: {server_id}")
        return
    
    # Ensure timestamp column exists
    if 'timestamp' not in filtered_df.columns:
        st.error("Timestamp column not found in metrics data.")
        return
    
    # Convert timestamp to datetime if needed
    filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'])
    filtered_df = filtered_df.sort_values('timestamp')
    
    # Create time series chart
    chart_data = filtered_df.set_index('timestamp')
    
    # Select metrics to display
    metric_cols = [col for col in ['log_count', 'error_rate', 'avg_response_time'] 
                   if col in chart_data.columns]
    
    if metric_cols:
        st.line_chart(chart_data[metric_cols], width='stretch')
    else:
        st.info("No time series metrics available to display.")

