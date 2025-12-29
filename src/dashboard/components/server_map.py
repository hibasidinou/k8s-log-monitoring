"""
Server map component for visualizing server locations and status.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional, Dict, List


def display_server_map(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Display interactive map of servers with status indicators.
    
    Args:
        server_status_df: DataFrame with server status information
        anomalies_df: Optional DataFrame with anomalies for additional context
    """
    if server_status_df.empty:
        st.info("No server data available.")
        return
    
    st.subheader("🗺️ Server Map")
    
    # Create server status visualization
    create_server_status_visualization(server_status_df, anomalies_df)


def create_server_status_visualization(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create visualization of server status.
    
    Args:
        server_status_df: DataFrame with server status
        anomalies_df: Optional anomalies DataFrame
    """
    # Add anomaly counts if available
    if anomalies_df is not None and not anomalies_df.empty and 'server_id' in anomalies_df.columns:
        anomaly_counts = anomalies_df['server_id'].value_counts().to_dict()
        server_status_df['anomaly_count'] = server_status_df['server_id'].map(anomaly_counts).fillna(0)
    else:
        server_status_df['anomaly_count'] = 0
    
    # Create status color mapping
    def get_status_color(status: str) -> str:
        status_lower = str(status).lower()
        color_map = {
            'healthy': '#00FF00',
            'warning': '#FFA500',
            'critical': '#FF0000',
            'error': '#FF0000',
            'normal': '#90EE90'
        }
        return color_map.get(status_lower, '#808080')
    
    # Create scatter plot for servers
    if 'status' in server_status_df.columns:
        server_status_df['color'] = server_status_df['status'].apply(get_status_color)
    else:
        server_status_df['color'] = '#808080'
    
    # Create visualization
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Create interactive scatter plot
        fig = go.Figure()
        
        # Get status column or use default
        if 'status' in server_status_df.columns:
            statuses = server_status_df['status'].unique()
        else:
            statuses = ['unknown']
            server_status_df['status'] = 'unknown'
        
        # Group by status
        for status in statuses:
            status_df = server_status_df[server_status_df['status'] == status].copy()
            
            if len(status_df) == 0:
                continue
            
            # Prepare data for plotting
            x_values = list(range(len(status_df)))
            
            # Get error_count column
            if 'error_count' in status_df.columns:
                y_values = status_df['error_count'].tolist()
            else:
                y_values = [0] * len(status_df)
            
            # Get anomaly_count for marker size
            if 'anomaly_count' in status_df.columns:
                sizes = (status_df['anomaly_count'] * 5 + 20).tolist()
            else:
                sizes = [20] * len(status_df)
            
            # Get colors
            if 'color' in status_df.columns:
                colors = status_df['color'].tolist()
            else:
                colors = ['#808080'] * len(status_df)
            
            fig.add_trace(go.Scatter(
                x=x_values,
                y=y_values,
                mode='markers+text',
                name=status.upper(),
                text=status_df['server_id'].tolist(),
                textposition='top center',
                marker=dict(
                    size=sizes,
                    color=colors,
                    line=dict(width=2, color='white')
                ),
                hovertemplate='<b>%{text}</b><br>Status: ' + str(status) + '<br>Errors: %{y}<extra></extra>'
            ))
        
        fig.update_layout(
            title='Server Status Map',
            xaxis_title='Server Index',
            yaxis_title='Error Count',
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        # Display server list with status
        st.subheader("Server List")
        
        for _, row in server_status_df.iterrows():
            server_id = row.get('server_id', 'Unknown')
            status = row.get('status', 'unknown')
            error_count = row.get('error_count', 0)
            anomaly_count = row.get('anomaly_count', 0)
            
            color = get_status_color(status)
            
            st.markdown(
                f"""
                <div style="
                    border-left: 4px solid {color};
                    padding: 8px;
                    margin: 5px 0;
                    background-color: #f0f0f0;
                    border-radius: 4px;
                ">
                    <strong>{server_id}</strong><br>
                    Status: {status}<br>
                    Errors: {error_count}<br>
                    Anomalies: {anomaly_count}
                </div>
                """,
                unsafe_allow_html=True
            )


def display_server_details(server_id: str, server_status_df: pd.DataFrame, 
                          anomalies_df: Optional[pd.DataFrame] = None,
                          metrics_df: Optional[pd.DataFrame] = None):
    """
    Display detailed information for a specific server.
    
    Args:
        server_id: ID of the server to display
        server_status_df: DataFrame with server status
        anomalies_df: Optional DataFrame with anomalies
        metrics_df: Optional DataFrame with metrics
    """
    st.subheader(f"🖥️ Server Details: {server_id}")
    
    # Get server status
    server_info = server_status_df[server_status_df['server_id'] == server_id]
    
    if server_info.empty:
        st.warning(f"Server {server_id} not found.")
        return
    
    # Display server information
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        status = server_info.iloc[0].get('status', 'unknown')
        st.metric("Status", status)
    
    with col2:
        total_logs = server_info.iloc[0].get('total_logs', 0)
        st.metric("Total Logs", f"{total_logs:,}")
    
    with col3:
        error_count = server_info.iloc[0].get('error_count', 0)
        st.metric("Errors", error_count)
    
    with col4:
        warning_count = server_info.iloc[0].get('warning_count', 0)
        st.metric("Warnings", warning_count)
    
    # Display server-specific anomalies
    if anomalies_df is not None and not anomalies_df.empty:
        server_anomalies = anomalies_df[anomalies_df['server_id'] == server_id]
        
        if not server_anomalies.empty:
            st.subheader(f"Anomalies for {server_id}")
            st.dataframe(
                server_anomalies[['timestamp', 'severity', 'anomaly_type', 'message']].head(20),
                width='stretch',
                hide_index=True
            )
    
    # Display server-specific metrics
    if metrics_df is not None and not metrics_df.empty and 'server_id' in metrics_df.columns:
        server_metrics = metrics_df[metrics_df['server_id'] == server_id]
        
        if not server_metrics.empty:
            st.subheader(f"Metrics for {server_id}")
            st.line_chart(
                server_metrics.set_index('timestamp') if 'timestamp' in server_metrics.columns else server_metrics,
                width='stretch'
            )

