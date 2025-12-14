"""
Alerts panel component for displaying real-time alerts with color coding.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional


def get_severity_color(severity: str) -> str:
    """
    Get color code based on severity level.
    
    Args:
        severity: Severity level (critical, warning, info, etc.)
        
    Returns:
        Color code in hex format
    """
    severity_lower = severity.lower() if isinstance(severity, str) else str(severity).lower()
    
    color_map = {
        'critical': '#FF0000',      # Red
        'high': '#FF4500',          # Orange Red
        'warning': '#FFA500',       # Orange
        'medium': '#FFD700',        # Gold
        'low': '#FFFF00',           # Yellow
        'info': '#00BFFF',          # Deep Sky Blue
        'healthy': '#00FF00',       # Green
        'normal': '#90EE90'         # Light Green
    }
    
    return color_map.get(severity_lower, '#808080')  # Default gray


def display_alerts_panel(alerts: List[Dict], max_display: int = 20):
    """
    Display alerts panel with color-coded severity levels.
    
    Args:
        alerts: List of alert dictionaries
        max_display: Maximum number of alerts to display
    """
    st.subheader("🚨 Alerts Panel")
    
    if not alerts:
        st.info("No alerts to display. All systems operational.")
        return
    
    # Limit number of alerts
    display_alerts = alerts[:max_display]
    
    # Group alerts by severity
    severity_groups = {}
    for alert in display_alerts:
        severity = alert.get('severity', 'info').lower()
        if severity not in severity_groups:
            severity_groups[severity] = []
        severity_groups[severity].append(alert)
    
    # Display alerts grouped by severity
    for severity in ['critical', 'high', 'warning', 'medium', 'low', 'info']:
        if severity in severity_groups:
            severity_alerts = severity_groups[severity]
            color = get_severity_color(severity)
            
            with st.expander(
                f"🔴 {severity.upper()} ({len(severity_alerts)})",
                expanded=(severity in ['critical', 'high', 'warning'])
            ):
                for alert in severity_alerts:
                    display_single_alert(alert, color)


def display_single_alert(alert: Dict, color: str):
    """
    Display a single alert card with modern styling.
    
    Args:
        alert: Alert dictionary
        color: Color code for the alert
    """
    # Create alert card
    timestamp = alert.get('timestamp', 'Unknown')
    server_id = alert.get('server_id', 'Unknown')
    message = alert.get('message', alert.get('description', 'No message'))
    anomaly_type = alert.get('anomaly_type', alert.get('type', 'Unknown'))
    
    # Format timestamp
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            formatted_time = timestamp
    else:
        formatted_time = str(timestamp)
    
    # Create modern styled alert box with gradient
    st.markdown(
        f"""
        <div style="
            border-left: 5px solid {color};
            padding: 15px;
            margin: 10px 0;
            background: linear-gradient(135deg, rgba(255,255,255,0.9) 0%, rgba(240,240,240,0.9) 100%);
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        ">
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <div style="width: 12px; height: 12px; background-color: {color}; border-radius: 50%; margin-right: 10px; box-shadow: 0 0 10px {color};"></div>
                <strong style="color: #2d3748; font-size: 1.1rem;">🖥️ {server_id}</strong>
            </div>
            <div style="color: #4a5568; margin: 5px 0;">
                <strong>⏰</strong> {formatted_time}
            </div>
            <div style="color: #4a5568; margin: 5px 0;">
                <strong>📋</strong> {anomaly_type}
            </div>
            <div style="color: #2d3748; margin-top: 10px; padding-top: 10px; border-top: 1px solid #e2e8f0;">
                <strong>💬</strong> {message[:200]}{'...' if len(message) > 200 else ''}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def display_anomalies_table(anomalies_df: pd.DataFrame, max_rows: int = 100):
    """
    Display anomalies in a table format with filtering options.
    
    Args:
        anomalies_df: DataFrame with anomalies
        max_rows: Maximum number of rows to display
    """
    if anomalies_df.empty:
        st.info("No anomalies detected.")
        return
    
    st.subheader("📋 Anomalies Table")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'server_id' in anomalies_df.columns:
            servers = ['All'] + sorted(anomalies_df['server_id'].unique().tolist())
            selected_server = st.selectbox("Filter by Server", servers)
            if selected_server != 'All':
                anomalies_df = anomalies_df[anomalies_df['server_id'] == selected_server]
    
    with col2:
        if 'severity' in anomalies_df.columns:
            severities = ['All'] + sorted(anomalies_df['severity'].unique().tolist())
            selected_severity = st.selectbox("Filter by Severity", severities)
            if selected_severity != 'All':
                anomalies_df = anomalies_df[anomalies_df['severity'] == selected_severity]
    
    with col3:
        if 'anomaly_type' in anomalies_df.columns:
            types = ['All'] + sorted(anomalies_df['anomaly_type'].unique().tolist())
            selected_type = st.selectbox("Filter by Type", types)
            if selected_type != 'All':
                anomalies_df = anomalies_df[anomalies_df['anomaly_type'] == selected_type]
    
    # Display table
    display_df = anomalies_df.head(max_rows)
    
    if not display_df.empty:
        # Add color coding to severity column if it exists
        if 'severity' in display_df.columns:
            def color_severity(val):
                color = get_severity_color(val)
                return f'background-color: {color}'
            
            styled_df = display_df.style.applymap(
                color_severity,
                subset=['severity']
            )
            st.dataframe(styled_df, width='stretch', hide_index=True)
        else:
            st.dataframe(display_df, width='stretch', hide_index=True)
        
        if len(anomalies_df) > max_rows:
            st.info(f"Showing {max_rows} of {len(anomalies_df)} anomalies. Use filters to narrow down results.")
    else:
        st.warning("No anomalies match the selected filters.")

