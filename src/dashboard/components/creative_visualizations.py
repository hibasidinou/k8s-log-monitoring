"""
Creative and modern visualizations for the dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from typing import Optional, Dict, List


def create_gauge_chart(value: float, max_value: float, title: str, color: str = "#667eea"):
    """
    Create a modern gauge/speedometer chart.
    
    Args:
        value: Current value
        max_value: Maximum value
        title: Chart title
        color: Color scheme
    """
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title, 'font': {'size': 20, 'color': '#2d3748'}},
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
        height=300
    )
    
    return fig


def create_radar_chart(server_data: Dict, title: str = "Server Performance"):
    """
    Create a radar/spider chart for server performance metrics.
    
    Args:
        server_data: Dictionary with server metrics
        title: Chart title
    """
    categories = list(server_data.keys())
    values = list(server_data.values())
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='Performance',
        line_color='#667eea'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        title=title,
        font={'color': "#2d3748", 'family': "Inter"},
        paper_bgcolor="white",
        height=400
    )
    
    return fig


def create_3d_scatter(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a 3D scatter plot of servers.
    
    Args:
        server_status_df: DataFrame with server status
        anomalies_df: Optional DataFrame with anomalies
    """
    if server_status_df.empty:
        return None
    
    # Prepare data
    if 'error_count' not in server_status_df.columns:
        server_status_df['error_count'] = 0
    if 'warning_count' not in server_status_df.columns:
        server_status_df['warning_count'] = 0
    if 'total_logs' not in server_status_df.columns:
        server_status_df['total_logs'] = 0
    
    # Add anomaly counts
    if anomalies_df is not None and not anomalies_df.empty and 'server_id' in anomalies_df.columns:
        anomaly_counts = anomalies_df['server_id'].value_counts().to_dict()
        server_status_df['anomaly_count'] = server_status_df['server_id'].map(anomaly_counts).fillna(0)
    else:
        server_status_df['anomaly_count'] = 0
    
    # Color mapping
    def get_status_color(status):
        color_map = {
            'healthy': '#00ff88',
            'warning': '#ffaa00',
            'critical': '#ff0044',
            'error': '#ff0044'
        }
        return color_map.get(str(status).lower(), '#808080')
    
    if 'status' in server_status_df.columns:
        server_status_df['color'] = server_status_df['status'].apply(get_status_color)
    else:
        server_status_df['color'] = '#808080'
    
    # Create 3D scatter
    fig = go.Figure(data=go.Scatter3d(
        x=server_status_df['error_count'].tolist(),
        y=server_status_df['warning_count'].tolist(),
        z=server_status_df['anomaly_count'].tolist(),
        mode='markers+text',
        text=server_status_df['server_id'].tolist(),
        marker=dict(
            size=(server_status_df['total_logs'] / 100).clip(10, 50).tolist(),
            color=server_status_df['color'].tolist(),
            opacity=0.8,
            line=dict(width=2, color='white')
        ),
        hovertemplate='<b>%{text}</b><br>Errors: %{x}<br>Warnings: %{y}<br>Anomalies: %{z}<extra></extra>'
    ))
    
    fig.update_layout(
        title='3D Server Status Visualization',
        scene=dict(
            xaxis_title='Error Count',
            yaxis_title='Warning Count',
            zaxis_title='Anomaly Count',
            bgcolor='white'
        ),
        font={'color': "#2d3748", 'family': "Inter"},
        height=600
    )
    
    return fig


def create_animated_timeline(anomalies_df: pd.DataFrame):
    """
    Create an animated timeline with bubbles.
    
    Args:
        anomalies_df: DataFrame with anomalies
    """
    if anomalies_df.empty or 'timestamp' not in anomalies_df.columns:
        return None
    
    df = anomalies_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Severity size mapping
    severity_sizes = {
        'critical': 30,
        'high': 25,
        'warning': 20,
        'medium': 15,
        'low': 10,
        'info': 5
    }
    
    if 'severity' in df.columns:
        df['size'] = df['severity'].str.lower().map(severity_sizes).fillna(10)
    else:
        df['size'] = 15
    
    # Color mapping
    severity_colors = {
        'critical': '#ff0044',
        'high': '#ff6600',
        'warning': '#ffaa00',
        'medium': '#ffdd00',
        'low': '#aaff00',
        'info': '#00aaff'
    }
    
    if 'severity' in df.columns:
        df['color'] = df['severity'].str.lower().map(severity_colors).fillna('#808080')
    else:
        df['color'] = '#808080'
    
    fig = go.Figure()
    
    # Group by severity for better visualization
    for severity in df.get('severity', ['unknown']).unique():
        severity_df = df[df.get('severity', 'unknown') == severity]
        
        fig.add_trace(go.Scatter(
            x=severity_df['timestamp'],
            y=[severity] * len(severity_df),
            mode='markers',
            name=str(severity).upper(),
            marker=dict(
                size=severity_df['size'].tolist(),
                color=severity_df['color'].tolist(),
                line=dict(width=2, color='white'),
                opacity=0.7
            ),
            text=severity_df.get('message', ''),
            hovertemplate='<b>%{y}</b><br>Time: %{x}<br>%{text}<extra></extra>'
        ))
    
    fig.update_layout(
        title='Animated Incident Timeline',
        xaxis_title='Time',
        yaxis_title='Severity',
        hovermode='closest',
        height=500,
        font={'color': "#2d3748", 'family': "Inter"},
        paper_bgcolor="white",
        plot_bgcolor="white"
    )
    
    return fig


def create_sunburst_chart(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a sunburst chart showing server hierarchy and status.
    
    Args:
        server_status_df: DataFrame with server status
        anomalies_df: Optional DataFrame with anomalies
    """
    if server_status_df.empty:
        return None
    
    # Prepare data for sunburst
    data = []
    
    # Root
    data.append(dict(
        ids='root',
        labels='All Servers',
        parents='',
        values=len(server_status_df)
    ))
    
    # By status
    if 'status' in server_status_df.columns:
        for status in server_status_df['status'].unique():
            count = len(server_status_df[server_status_df['status'] == status])
            data.append(dict(
                ids=f'status_{status}',
                labels=status.upper(),
                parents='root',
                values=count
            ))
            
            # Servers in this status
            status_servers = server_status_df[server_status_df['status'] == status]
            for _, server in status_servers.iterrows():
                server_id = server.get('server_id', 'unknown')
                data.append(dict(
                    ids=f'server_{server_id}',
                    labels=server_id,
                    parents=f'status_{status}',
                    values=1
                ))
    
    df_sunburst = pd.DataFrame(data)
    
    fig = go.Figure(go.Sunburst(
        ids=df_sunburst['ids'],
        labels=df_sunburst['labels'],
        parents=df_sunburst['parents'],
        values=df_sunburst['values'],
        branchvalues="total",
        hovertemplate='<b>%{label}</b><br>Count: %{value}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Server Hierarchy Sunburst',
        font={'color': "#2d3748", 'family': "Inter"},
        height=500
    )
    
    return fig


def create_heatmap_calendar(anomalies_df: pd.DataFrame):
    """
    Create a calendar heatmap showing anomalies by day.
    
    Args:
        anomalies_df: DataFrame with anomalies
    """
    if anomalies_df.empty or 'timestamp' not in anomalies_df.columns:
        return None
    
    df = anomalies_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['week'] = df['timestamp'].dt.isocalendar().week
    df['year'] = df['timestamp'].dt.year
    
    # Count anomalies per day
    daily_counts = df.groupby('date').size().reset_index(name='count')
    
    # Create calendar structure
    calendar_data = []
    for _, row in daily_counts.iterrows():
        date = row['date']
        count = row['count']
        calendar_data.append({
            'date': date,
            'count': count,
            'day': date.strftime('%A'),
            'week': pd.Timestamp(date).isocalendar().week
        })
    
    if not calendar_data:
        return None
    
    cal_df = pd.DataFrame(calendar_data)
    
    # Create heatmap
    fig = px.density_heatmap(
        cal_df,
        x='week',
        y='day',
        z='count',
        histfunc='sum',
        title='Anomaly Calendar Heatmap',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        font={'color': "#2d3748", 'family': "Inter"},
        height=400
    )
    
    return fig


def create_network_graph(server_status_df: pd.DataFrame, anomalies_df: Optional[pd.DataFrame] = None):
    """
    Create a network graph showing relationships between servers.
    
    Args:
        server_status_df: DataFrame with server status
        anomalies_df: Optional DataFrame with anomalies
    """
    if server_status_df.empty:
        return None
    
    # Create node positions in a circle
    n_servers = len(server_status_df)
    angles = np.linspace(0, 2*np.pi, n_servers, endpoint=False)
    
    node_x = np.cos(angles)
    node_y = np.sin(angles)
    
    # Create edges (connect all servers in a network)
    edge_x = []
    edge_y = []
    
    for i in range(n_servers):
        for j in range(i+1, n_servers):
            edge_x.extend([node_x[i], node_x[j], None])
            edge_y.extend([node_y[i], node_y[j], None])
    
    # Color nodes by status
    def get_status_color(status):
        color_map = {
            'healthy': '#00ff88',
            'warning': '#ffaa00',
            'critical': '#ff0044'
        }
        return color_map.get(str(status).lower(), '#808080')
    
    if 'status' in server_status_df.columns:
        node_colors = server_status_df['status'].apply(get_status_color).tolist()
    else:
        node_colors = ['#808080'] * n_servers
    
    # Create figure
    fig = go.Figure()
    
    # Add edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    ))
    
    # Add nodes
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        name='Servers',
        marker=dict(
            size=30,
            color=node_colors,
            line=dict(width=2, color='white')
        ),
        text=server_status_df['server_id'].tolist(),
        textposition="middle center",
        hovertemplate='<b>%{text}</b><extra></extra>'
    ))
    
    fig.update_layout(
        title='Server Network Topology',
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=40),
        annotations=[
            dict(
                text="Server Network",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                xanchor="left", yanchor="bottom",
                font=dict(color="#2d3748", size=16)
            )
        ],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        font={'color': "#2d3748", 'family': "Inter"},
        height=500,
        paper_bgcolor="white",
        plot_bgcolor="white"
    )
    
    return fig





