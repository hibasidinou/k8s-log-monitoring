"""
Timeline component for visualizing incidents and events over time.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Optional


def display_incident_timeline(anomalies_df: pd.DataFrame, server_id: Optional[str] = None):
    """
    Display interactive timeline of incidents.
    
    Args:
        anomalies_df: DataFrame with anomalies/incidents
        server_id: Optional server ID to filter
    """
    if anomalies_df.empty:
        st.info("No incident data available for timeline.")
        return
    
    st.subheader("📅 Incident Timeline")
    
    # Filter by server if specified
    if server_id and 'server_id' in anomalies_df.columns:
        filtered_df = anomalies_df[anomalies_df['server_id'] == server_id].copy()
    else:
        filtered_df = anomalies_df.copy()
    
    if filtered_df.empty:
        st.warning(f"No incidents found for server: {server_id}")
        return
    
    # Ensure timestamp column exists
    if 'timestamp' not in filtered_df.columns:
        st.error("Timestamp column not found in anomalies data.")
        return
    
    # Convert timestamp to datetime
    filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'])
    filtered_df = filtered_df.sort_values('timestamp')
    
    # Create timeline visualization
    create_timeline_chart(filtered_df)


def create_timeline_chart(df: pd.DataFrame):
    """
    Create interactive timeline chart using Plotly.
    
    Args:
        df: DataFrame with timestamp and severity information
    """
    # Color mapping for severity
    severity_colors = {
        'critical': '#FF0000',
        'high': '#FF4500',
        'warning': '#FFA500',
        'medium': '#FFD700',
        'low': '#FFFF00',
        'info': '#00BFFF'
    }
    
    # Prepare data
    if 'severity' not in df.columns:
        df['severity'] = 'info'
    
    # Create figure
    fig = go.Figure()
    
    # Group by severity and server
    for severity in df['severity'].unique():
        severity_df = df[df['severity'] == severity]
        color = severity_colors.get(severity.lower(), '#808080')
        
        # Add scatter plot for each severity level
        fig.add_trace(go.Scatter(
            x=severity_df['timestamp'],
            y=[severity] * len(severity_df),
            mode='markers',
            name=severity.upper(),
            marker=dict(
                size=10,
                color=color,
                line=dict(width=1, color='white')
            ),
            text=severity_df.get('message', ''),
            hovertemplate='<b>%{y}</b><br>Time: %{x}<br>%{text}<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title='Incident Timeline',
        xaxis_title='Time',
        yaxis_title='Severity',
        hovermode='closest',
        height=400,
        showlegend=True
    )
    
    st.plotly_chart(fig, width='stretch')
    
    # Display timeline statistics
    display_timeline_stats(df)


def display_timeline_stats(df: pd.DataFrame):
    """
    Display statistics about the timeline.
    
    Args:
        df: DataFrame with timeline data
    """
    col1, col2, col3, col4 = st.columns(4)
    
    # Time range
    if 'timestamp' in df.columns:
        time_range = df['timestamp'].max() - df['timestamp'].min()
        with col1:
            st.metric("Time Range", f"{time_range.days} days")
        
        # First incident
        first_incident = df['timestamp'].min()
        with col2:
            st.metric("First Incident", first_incident.strftime('%Y-%m-%d'))
        
        # Last incident
        last_incident = df['timestamp'].max()
        with col3:
            st.metric("Last Incident", last_incident.strftime('%Y-%m-%d'))
        
        # Total incidents
        with col4:
            st.metric("Total Incidents", len(df))
    
    # Severity distribution
    if 'severity' in df.columns:
        st.subheader("📊 Severity Distribution")
        severity_counts = df['severity'].value_counts()
        
        # Create bar chart
        fig = px.bar(
            x=severity_counts.index,
            y=severity_counts.values,
            labels={'x': 'Severity', 'y': 'Count'},
            title='Incidents by Severity'
        )
        st.plotly_chart(fig, width='stretch')


def display_hourly_incident_heatmap(anomalies_df: pd.DataFrame):
    """
    Display heatmap of incidents by hour and day.
    
    Args:
        anomalies_df: DataFrame with anomalies
    """
    if anomalies_df.empty or 'timestamp' not in anomalies_df.columns:
        st.info("No data available for heatmap.")
        return
    
    st.subheader("🔥 Incident Heatmap (Hourly Distribution)")
    
    # Convert timestamp
    df = anomalies_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Extract hour and day of week
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.day_name()
    
    # Create pivot table
    heatmap_data = df.groupby(['day_of_week', 'hour']).size().reset_index(name='count')
    
    # Create heatmap
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='hour', values='count')
    heatmap_pivot = heatmap_pivot.reindex([d for d in days_order if d in heatmap_pivot.index])
    
    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Hour of Day", y="Day of Week", color="Incidents"),
        title="Incident Distribution by Day and Hour",
        aspect="auto"
    )
    
    st.plotly_chart(fig, width='stretch')





