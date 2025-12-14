"""
Data loader utility for dashboard.
Handles loading and caching of processed data from Spark pipeline.
"""

import json
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Dict, List, Optional
import os


class DataLoader:
    """Loads and caches data for the dashboard."""
    
    def __init__(self, data_dir: str = "data/output"):
        """
        Initialize data loader.
        
        Args:
            data_dir: Base directory for processed data
        """
        self.data_dir = Path(data_dir)
        self.dashboard_dir = self.data_dir / "dashboard"
        self.dashboard_dir.mkdir(parents=True, exist_ok=True)
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_anomalies(_self) -> pd.DataFrame:
        """
        Load detected anomalies from JSON file.
        
        Returns:
            DataFrame with anomaly data
        """
        anomalies_file = _self.data_dir / "anomalies_detected.json"
        
        if not anomalies_file.exists():
            # Return empty DataFrame with expected structure
            return pd.DataFrame(columns=[
                'timestamp', 'server_id', 'severity', 'message', 
                'anomaly_type', 'log_level'
            ])
        
        try:
            with open(anomalies_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict) and 'anomalies' in data:
                return pd.DataFrame(data['anomalies'])
            else:
                return pd.DataFrame([data])
        except Exception as e:
            st.warning(f"Error loading anomalies: {e}")
            return pd.DataFrame(columns=[
                'timestamp', 'server_id', 'severity', 'message', 
                'anomaly_type', 'log_level'
            ])
    
    @st.cache_data(ttl=300)
    def load_server_status(_self) -> pd.DataFrame:
        """
        Load server status data.
        
        Returns:
            DataFrame with server status information
        """
        status_file = _self.dashboard_dir / "server_status.parquet"
        
        if not status_file.exists():
            # Return sample data structure
            return pd.DataFrame({
                'server_id': [f'server_{i:02d}' for i in range(1, 11)],
                'status': ['healthy'] * 10,
                'last_update': [pd.Timestamp.now()] * 10,
                'total_logs': [0] * 10,
                'error_count': [0] * 10,
                'warning_count': [0] * 10
            })
        
        try:
            return pd.read_parquet(status_file)
        except Exception as e:
            st.warning(f"Error loading server status: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_latest_alerts(_self, limit: int = 50) -> List[Dict]:
        """
        Load latest alerts for the alerts panel.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of alert dictionaries
        """
        alerts_file = _self.dashboard_dir / "latest_alerts.json"
        
        if not alerts_file.exists():
            return []
        
        try:
            with open(alerts_file, 'r', encoding='utf-8') as f:
                alerts = json.load(f)
            
            if isinstance(alerts, list):
                return alerts[:limit]
            elif isinstance(alerts, dict) and 'alerts' in alerts:
                return alerts['alerts'][:limit]
            else:
                return []
        except Exception as e:
            st.warning(f"Error loading alerts: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def load_aggregated_metrics(_self) -> pd.DataFrame:
        """
        Load aggregated metrics from Spark processing.
        
        Returns:
            DataFrame with hourly/daily metrics
        """
        metrics_file = _self.data_dir / "aggregated_metrics.parquet"
        
        if not metrics_file.exists():
            # Return sample structure
            return pd.DataFrame({
                'timestamp': pd.date_range(end=pd.Timestamp.now(), periods=24, freq='h'),
                'server_id': ['all'] * 24,
                'log_count': [0] * 24,
                'error_rate': [0.0] * 24,
                'avg_response_time': [0.0] * 24
            })
        
        try:
            return pd.read_parquet(metrics_file)
        except Exception as e:
            st.warning(f"Error loading metrics: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_statistics(_self) -> Dict:
        """
        Load statistics from processing pipeline.
        
        Returns:
            Dictionary with statistics
        """
        stats_dir = _self.data_dir / "statistics"
        
        if not stats_dir.exists():
            return {
                'total_logs': 0,
                'total_anomalies': 0,
                'servers_monitored': 10,
                'time_range': {'start': None, 'end': None}
            }
        
        try:
            # Try to load from JSON files in statistics directory
            stats_files = list(stats_dir.glob("*.json"))
            if stats_files:
                with open(stats_files[0], 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            st.warning(f"Error loading statistics: {e}")
        
        return {
            'total_logs': 0,
            'total_anomalies': 0,
            'servers_monitored': 10,
            'time_range': {'start': None, 'end': None}
        }
    
    def get_server_list(self) -> List[str]:
        """
        Get list of all monitored servers.
        
        Returns:
            List of server IDs
        """
        status_df = self.load_server_status()
        if not status_df.empty and 'server_id' in status_df.columns:
            return status_df['server_id'].tolist()
        return [f'server_{i:02d}' for i in range(1, 11)]





