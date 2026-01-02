"""
Creative and interactive visualizations for the K8s log monitoring dashboard.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional, Dict
from datetime import datetime


def create_3d_scatter(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a 3D scatter plot showing server metrics.
    
    Args:
        server_status_df: DataFrame with server status data
        anomalies_df: Optional DataFrame with anomalies data
    
    Returns:
        plotly.graph_objects.Figure or None
    """
    if server_status_df.empty or 'server_id' not in server_status_df.columns:
        return None
    
    # Prepare data
    df = server_status_df.copy()
    
    # Get error_count, warning_count, and total_logs
    error_count = df.get('error_count', pd.Series([0] * len(df)))
    warning_count = df.get('warning_count', pd.Series([0] * len(df)))
    total_logs = df.get('total_logs', pd.Series([1] * len(df)))
    
    # Calculate anomaly count if anomalies_df is provided
    if anomalies_df is not None and not anomalies_df.empty and 'server_id' in anomalies_df.columns:
        anomaly_counts = anomalies_df.groupby('server_id').size().to_dict()
        df['anomaly_count'] = df['server_id'].map(anomaly_counts).fillna(0)
    else:
        df['anomaly_count'] = 0
    
    # Create 3D scatter plot
    fig = go.Figure(data=go.Scatter3d(
        x=error_count,
        y=warning_count,
        z=df['anomaly_count'],
        mode='markers',
        marker=dict(
            size=total_logs / max(total_logs.max(), 1) * 20 + 5,
            color=error_count,
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title="Error Count"),
            line=dict(width=1, color='darkgray')
        ),
        text=df['server_id'].tolist() if 'server_id' in df.columns else None,
        hovertemplate='<b>%{text}</b><br>' +
                      'Errors: %{x}<br>' +
                      'Warnings: %{y}<br>' +
                      'Anomalies: %{z}<extra></extra>'
    ))
    
    fig.update_layout(
        title="3D Server Status Visualization",
        scene=dict(
            xaxis_title="Error Count",
            yaxis_title="Warning Count",
            zaxis_title="Anomaly Count"
        ),
        height=600,
        margin=dict(l=0, r=0, t=50, b=0)
    )
    
    return fig


def create_animated_timeline(anomalies_df: pd.DataFrame):
    """
    Create an animated timeline of anomalies.
    
    Args:
        anomalies_df: DataFrame with anomalies data
    
    Returns:
        plotly.graph_objects.Figure or None
    """
    if anomalies_df.empty:
        return None
    
    df = anomalies_df.copy()
    
    # Ensure timestamp column exists
    if 'timestamp' not in df.columns:
        return None
    
    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['timestamp'])
    
    if df.empty:
        return None
    
    # Get severity for color mapping
    if 'severity' in df.columns:
        severity_map = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1, 'info': 0}
        df['severity_num'] = df['severity'].map(severity_map).fillna(0)
    else:
        df['severity_num'] = 0
    
    # Create animated scatter plot
    fig = go.Figure()
    
    if 'severity' in df.columns:
        for severity in df['severity'].unique():
            severity_df = df[df['severity'] == severity]
            fig.add_trace(go.Scatter(
                x=severity_df['timestamp'],
                y=[1] * len(severity_df),  # Y position (can be customized)
                mode='markers',
                name=severity,
                marker=dict(
                    size=severity_df['severity_num'] * 10 + 10,
                    opacity=0.7
                ),
                text=severity_df.get('message', ''),
                hovertemplate='<b>%{text}</b><br>Time: %{x}<extra></extra>'
            ))
    else:
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=[1] * len(df),
            mode='markers',
            name='Anomalies',
            marker=dict(size=15, opacity=0.7),
            text=df.get('message', ''),
            hovertemplate='<b>%{text}</b><br>Time: %{x}<extra></extra>'
        ))
    
    fig.update_layout(
        title="Animated Incident Timeline",
        xaxis_title="Time",
        yaxis_title="",
        height=400,
        hovermode='closest',
        showlegend=True
    )
    
    return fig


def create_sunburst_chart(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a sunburst chart showing server hierarchy by status.
    
    Args:
        server_status_df: DataFrame with server status data
        anomalies_df: Optional DataFrame with anomalies data
    
    Returns:
        plotly.graph_objects.Figure or None
    """
    if server_status_df.empty or 'server_id' not in server_status_df.columns:
        return None
    
    df = server_status_df.copy()
    
    # Get status column
    if 'status' not in df.columns:
        df['status'] = 'unknown'
    
    # Create hierarchy: Status -> Server
    labels = []
    parents = []
    values = []
    
    # Add status level
    status_counts = df.groupby('status').size()
    for status, count in status_counts.items():
        labels.append(status)
        parents.append("")
        values.append(count)
    
    # Add server level
    for _, row in df.iterrows():
        labels.append(row['server_id'])
        parents.append(row['status'])
        values.append(1)
    
    fig = go.Figure(go.Sunburst(
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total"
    ))
    
    fig.update_layout(
        title="Server Hierarchy by Status",
        height=500
    )
    
    return fig


def create_heatmap_calendar(anomalies_df: pd.DataFrame):
    """
    Create a calendar heatmap showing anomaly distribution.
    
    Args:
        anomalies_df: DataFrame with anomalies data
    
    Returns:
        plotly.graph_objects.Figure or None
    """
    if anomalies_df.empty:
        return None
    
    df = anomalies_df.copy()
    
    # Ensure timestamp column exists
    if 'timestamp' not in df.columns:
        return None
    
    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['timestamp'])
    
    if df.empty:
        return None
    
    # Extract date and hour
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    
    # Count anomalies by date and hour
    heatmap_data = df.groupby(['date', 'hour']).size().reset_index(name='count')
    
    # Create pivot table for heatmap
    pivot_data = heatmap_data.pivot(index='date', columns='hour', values='count').fillna(0)
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=[str(d) for d in pivot_data.index],
        colorscale='Reds',
        showscale=True
    ))
    
    fig.update_layout(
        title="Anomaly Calendar Heatmap",
        xaxis_title="Hour of Day",
        yaxis_title="Date",
        height=600
    )
    
    return fig


def create_network_graph(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a network graph showing server relationships.
    
    Args:
        server_status_df: DataFrame with server status data
        anomalies_df: Optional DataFrame with anomalies data
    
    Returns:
        plotly.graph_objects.Figure or None
    """
    if server_status_df.empty or 'server_id' not in server_status_df.columns:
        return None
    
    df = server_status_df.copy()
    
    # Create nodes (servers)
    node_trace = go.Scatter(
        x=[],
        y=[],
        mode='markers+text',
        text=[],
        textposition="middle center",
        hovertext=[],
        marker=dict(
            size=[],
            color=[],
            colorscale='Viridis',
            showscale=True
        )
    )
    
    # Position nodes in a circle
    n_servers = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        angle = 2 * 3.14159 * i / n_servers
        node_trace['x'] += (0.5 * (1 + 0.3 * (i % 3)) * (1 if i % 2 == 0 else -1),)
        node_trace['y'] += (0.5 * (1 + 0.3 * ((i + 1) % 3)) * (1 if (i // 2) % 2 == 0 else -1),)
        node_trace['text'] += (row['server_id'],)
        
        error_count = row.get('error_count', 0)
        node_trace['marker']['size'] += (max(20, error_count * 5 + 20),)
        node_trace['marker']['color'] += (error_count,)
        node_trace['hovertext'] += (
            f"Server: {row['server_id']}<br>"
            f"Errors: {error_count}<br>"
            f"Status: {row.get('status', 'unknown')}",
        )
    
    # Create edges (connections between servers)
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=1, color='gray'),
        hoverinfo='none',
        mode='lines'
    )
    
    # Create simple connections (each server connected to next)
    for i in range(n_servers):
        if i < n_servers - 1:
            edge_trace['x'] += (node_trace['x'][i], node_trace['x'][i + 1], None)
            edge_trace['y'] += (node_trace['y'][i], node_trace['y'][i + 1], None)
    
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title="Server Network Topology",
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=500
        )
    )
    
    return fig


def create_radar_chart(perf_data: Dict[str, float], title: str = "Performance Radar"):
    """
    Create a radar chart for performance metrics.
    
    Args:
        perf_data: Dictionary with performance metrics (e.g., {'Availability': 95, 'Stability': 90})
        title: Chart title
    
    Returns:
        plotly.graph_objects.Figure
    """
    categories = list(perf_data.keys())
    values = list(perf_data.values())
    
    # Close the radar chart by adding first value at the end
    values += values[:1]
    categories += categories[:1]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='Performance',
        line=dict(color='rgb(102, 126, 234)')
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        showlegend=True,
        title=title,
        height=500
    )
    
    return fig
