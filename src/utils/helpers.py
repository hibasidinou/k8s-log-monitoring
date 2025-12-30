"""
Utility functions for data cleaning and preprocessing.
"""
import pandas as pd  # type: ignore[reportMissingImports]
import numpy as np  # type: ignore[reportMissingImports]
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import unicodedata
from pathlib import Path
import json

def normalize_whitespace(text: str) -> str:
    """Normalize whitespace in text (multiple spaces to single space)."""
    if pd.isna(text) or not isinstance(text, str):
        return text
    return ' '.join(text.split())

def remove_control_characters(text: str) -> str:
    """Remove control characters from text."""
    if pd.isna(text) or not isinstance(text, str):
        return text
    return ''.join(char for char in text if unicodedata.category(char)[0] != 'C' or char in '\n\t')

def normalize_unicode(text: str) -> str:
    """Normalize unicode characters (NFKD normalization)."""
    if pd.isna(text) or not isinstance(text, str):
        return text
    return unicodedata.normalize('NFKD', text)

def standardize_text(text: str, max_length: int = 10000, truncate: bool = True) -> str:
    """
    Standardize text by normalizing whitespace, removing control chars, and normalizing unicode.
    
    Args:
        text: Text to standardize
        max_length: Maximum length for text
        truncate: Whether to truncate if exceeds max_length
    
    Returns:
        Standardized text
    """
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    # Apply all text transformations
    text = normalize_unicode(text)
    text = remove_control_characters(text)
    text = normalize_whitespace(text)
    
    # Truncate if necessary
    if truncate and len(text) > max_length:
        text = text[:max_length]
    
    return text.strip()

def parse_timestamp(timestamp: Any, formats: List[str]) -> Optional[datetime]:
    """
    Parse timestamp from various formats.
    
    Args:
        timestamp: Timestamp value (string, datetime, or other)
        formats: List of format strings to try
    
    Returns:
        Parsed datetime object or None if parsing fails
    """
    if pd.isna(timestamp):
        return None
    
    # If already a datetime object
    if isinstance(timestamp, datetime):
        return timestamp
    
    # If it's a pandas Timestamp
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime()
    
    # Try to parse as string
    if not isinstance(timestamp, str):
        timestamp = str(timestamp)
    
    # Try each format
    for fmt in formats:
        try:
            return datetime.strptime(timestamp, fmt)
        except (ValueError, TypeError):
            continue
    
    # Try pandas parsing as fallback
    try:
        return pd.to_datetime(timestamp).to_pydatetime()
    except (ValueError, TypeError):
        return None

def standardize_timestamp(timestamp: Any, input_formats: List[str], 
                         output_format: str = '%Y-%m-%dT%H:%M:%S.%fZ',
                         timezone: str = 'UTC') -> Optional[str]:
    """
    Standardize timestamp to a consistent format.
    
    Args:
        timestamp: Timestamp to standardize
        input_formats: List of input formats to try
        output_format: Output format string
        timezone: Target timezone (default UTC)
    
    Returns:
        Standardized timestamp string or None
    """
    dt = parse_timestamp(timestamp, input_formats)
    if dt is None:
        return None
    
    # Convert to UTC if timezone specified
    if timezone == 'UTC' and dt.tzinfo is None:
        # Assume naive datetime is UTC
        from datetime import timezone as tz
        dt = dt.replace(tzinfo=tz.utc)
    elif dt.tzinfo is not None:
        # Convert to UTC
        dt = dt.astimezone(tz.utc)
    
    return dt.strftime(output_format)

def detect_outliers_iqr(series: pd.Series, multiplier: float = 3.0) -> pd.Series:
    """
    Detect outliers using Interquartile Range (IQR) method.
    
    Args:
        series: Pandas Series to analyze
        multiplier: IQR multiplier (default 3.0)
    
    Returns:
        Boolean Series indicating outliers (True = outlier)
    """
    if series.dtype not in [np.float64, np.int64, 'float64', 'int64']:
        return pd.Series([False] * len(series), index=series.index)
    
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    
    if IQR == 0:
        return pd.Series([False] * len(series), index=series.index)
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    return (series < lower_bound) | (series > upper_bound)

def detect_outliers_zscore(series: pd.Series, threshold: float = 3.0) -> pd.Series:
    """
    Detect outliers using Z-score method.
    
    Args:
        series: Pandas Series to analyze
        threshold: Z-score threshold (default 3.0)
    
    Returns:
        Boolean Series indicating outliers (True = outlier)
    """
    if series.dtype not in [np.float64, np.int64, 'float64', 'int64']:
        return pd.Series([False] * len(series), index=series.index)
    
    z_scores = np.abs((series - series.mean()) / series.std())
    return z_scores > threshold

def handle_outliers(series: pd.Series, method: str = 'iqr', 
                   multiplier: float = 3.0, 
                   strategy: str = 'cap') -> pd.Series:
    """
    Handle outliers in a series.
    
    Args:
        series: Pandas Series with potential outliers
        method: Detection method ('iqr' or 'zscore')
        multiplier: Multiplier for IQR or threshold for Z-score
        strategy: Handling strategy ('cap', 'remove', 'winsorize')
    
    Returns:
        Series with outliers handled
    """
    if method == 'iqr':
        outliers = detect_outliers_iqr(series, multiplier)
    elif method == 'zscore':
        outliers = detect_outliers_zscore(series, multiplier)
    else:
        return series
    
    if not outliers.any():
        return series
    
    result = series.copy()
    
    if strategy == 'cap':
        # Cap outliers at bounds
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        result = result.clip(lower=lower_bound, upper=upper_bound)
    
    elif strategy == 'winsorize':
        # Winsorize (cap at percentiles)
        lower_percentile = series.quantile(0.01)
        upper_percentile = series.quantile(0.99)
        result = result.clip(lower=lower_percentile, upper=upper_percentile)
    
    # 'remove' strategy is handled by caller (filtering rows)
    
    return result

def find_duplicates(df: pd.DataFrame, key_fields: List[str], 
                   time_window_seconds: int = 1) -> pd.Series:
    """
    Find duplicate rows based on key fields and optional time window.
    
    Args:
        df: DataFrame to check
        key_fields: List of field names to use for duplicate detection
        time_window_seconds: Time window in seconds for considering duplicates
    
    Returns:
        Boolean Series indicating duplicates (True = duplicate, keep first)
    """
    if not all(field in df.columns for field in key_fields):
        return pd.Series([False] * len(df), index=df.index)
    
    # Create a composite key
    if 'timestamp' in key_fields and 'timestamp' in df.columns:
        # Round timestamp to time window
        df_copy = df.copy()
        if df_copy['timestamp'].dtype == 'object':
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'], errors='coerce')
        df_copy['timestamp_rounded'] = df_copy['timestamp'].dt.floor(f'{time_window_seconds}S')
        key_fields_rounded = [f if f != 'timestamp' else 'timestamp_rounded' 
                             for f in key_fields]
        duplicates = df_copy.duplicated(subset=key_fields_rounded, keep='first')
    else:
        duplicates = df.duplicated(subset=key_fields, keep='first')
    
    return duplicates

def calculate_missing_values_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate comprehensive missing values statistics.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Dictionary with missing values statistics
    """
    total_rows = len(df)
    stats = {
        'total_rows': total_rows,
        'columns': {},
        'total_missing': 0,
        'columns_with_missing': []
    }
    
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_percentage = (missing_count / total_rows * 100) if total_rows > 0 else 0
        
        stats['columns'][col] = {
            'missing_count': int(missing_count),
            'missing_percentage': round(missing_percentage, 2),
            'non_missing_count': int(total_rows - missing_count),
            'dtype': str(df[col].dtype)
        }
        
        if missing_count > 0:
            stats['columns_with_missing'].append(col)
            stats['total_missing'] += missing_count
    
    stats['total_missing'] = int(stats['total_missing'])
    stats['overall_missing_percentage'] = round(
        (stats['total_missing'] / (total_rows * len(df.columns)) * 100) if total_rows > 0 else 0, 
        2
    )
    
    return stats

def calculate_data_quality_metrics(df: pd.DataFrame, 
                                   schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Calculate comprehensive data quality metrics.
    
    Args:
        df: DataFrame to analyze
        schema: Optional schema definition for validation
    
    Returns:
        Dictionary with quality metrics
    """
    metrics = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': calculate_missing_values_stats(df),
        'duplicates': {
            'count': int(df.duplicated().sum()),
            'percentage': round((df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0, 2)
        },
        'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'numeric_stats': {},
        'text_stats': {}
    }
    
    # Numeric statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        metrics['numeric_stats'][col] = {
            'mean': float(df[col].mean()) if not df[col].isna().all() else None,
            'median': float(df[col].median()) if not df[col].isna().all() else None,
            'std': float(df[col].std()) if not df[col].isna().all() else None,
            'min': float(df[col].min()) if not df[col].isna().all() else None,
            'max': float(df[col].max()) if not df[col].isna().all() else None,
            'q25': float(df[col].quantile(0.25)) if not df[col].isna().all() else None,
            'q75': float(df[col].quantile(0.75)) if not df[col].isna().all() else None,
            'outliers_iqr': int(detect_outliers_iqr(df[col]).sum()) if not df[col].isna().all() else 0
        }
    
    # Text statistics
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        non_null = df[col].dropna()
        if len(non_null) > 0:
            metrics['text_stats'][col] = {
                'unique_count': int(non_null.nunique()),
                'most_frequent': non_null.mode().iloc[0] if len(non_null.mode()) > 0 else None,
                'avg_length': float(non_null.astype(str).str.len().mean()) if len(non_null) > 0 else 0,
                'max_length': int(non_null.astype(str).str.len().max()) if len(non_null) > 0 else 0
            }
    
    return metrics

def save_json_report(data: Dict[str, Any], output_path: Path) -> None:
    """Save a dictionary as a JSON report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert numpy types to native Python types for JSON serialization
    def convert_to_json_serializable(obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_json_serializable(item) for item in obj]
        elif pd.isna(obj):
            return None
        return obj
    
    json_data = convert_to_json_serializable(data)
    
    with open(output_path, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)

def validate_k8s_pod_name(name: str) -> bool:
    """Validate Kubernetes pod name format (RFC 1123 subdomain)."""
    if pd.isna(name) or not isinstance(name, str):
        return False
    pattern = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$'
    return bool(re.match(pattern, name))

def validate_k8s_namespace(name: str) -> bool:
    """Validate Kubernetes namespace name."""
    if pd.isna(name) or not isinstance(name, str):
        return False
    # Namespace must be a valid DNS subdomain
    pattern = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
    return bool(re.match(pattern, name)) and len(name) <= 253

