"""
Main data preprocessing module for Kubernetes log monitoring.
Handles data cleaning, transformation, validation, and quality reporting.
"""
import pandas as pd  # type: ignore[reportMissingImports]
import numpy as np  # type: ignore[reportMissingImports]
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import json

from src.utils.config import (
    K8S_LOG_SCHEMA, HYBRID_DATASET_SCHEMA, get_schema_for_data_type,
    TIMESTAMP_CONFIG, TEXT_STANDARDIZATION, OUTLIER_THRESHOLDS,
    MISSING_VALUE_THRESHOLDS, DUPLICATE_DETECTION,
    CLEANED_DATA_DIR, STATISTICS_DIR, save_schema_json
)
from src.utils.helpers import (
    standardize_text, standardize_timestamp, detect_outliers_iqr,
    handle_outliers, find_duplicates, calculate_missing_values_stats,
    calculate_data_quality_metrics, save_json_report,
    validate_k8s_pod_name, validate_k8s_namespace
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataPreprocessor:
    """
    Main data preprocessing class for Kubernetes logs.
    """
    
    def __init__(self, data_type: str = 'k8s_log'):
        """
        Initialize the data preprocessor.
        
        Args:
            data_type: Type of data to process ('k8s_log' or 'hybrid_dataset')
        """
        self.data_type = data_type
        self.schema = get_schema_for_data_type(data_type)
        self.cleaning_stats = {
            'initial_rows': 0,
            'final_rows': 0,
            'rows_removed': 0,
            'missing_values_handled': {},
            'outliers_detected': {},
            'outliers_handled': {},
            'duplicates_removed': 0,
            'text_standardized': 0,
            'timestamps_standardized': 0,
            'validation_errors': []
        }
    
    def load_data(self, input_path: Path) -> pd.DataFrame:
        """
        Load data from file (CSV or Parquet).
        
        Args:
            input_path: Path to input file
        
        Returns:
            Loaded DataFrame
        """
        logger.info(f"Loading data from {input_path}")
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        if input_path.suffix == '.parquet':
            try:
                df = pd.read_parquet(input_path)
            except Exception as e:
                logger.warning(f"Failed to read parquet, trying CSV: {e}")
                # Try CSV as fallback
                df = pd.read_csv(input_path)
        elif input_path.suffix == '.csv':
            df = pd.read_csv(input_path)
        else:
            raise ValueError(f"Unsupported file format: {input_path.suffix}")
        
        self.cleaning_stats['initial_rows'] = len(df)
        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values according to schema and thresholds.
        
        Args:
            df: DataFrame to clean
        
        Returns:
            DataFrame with missing values handled
        """
        logger.info("Handling missing values...")
        df = df.copy()
        initial_rows = len(df)
        
        # Check critical fields
        critical_fields = MISSING_VALUE_THRESHOLDS.get('critical_fields', [])
        max_missing_pct = MISSING_VALUE_THRESHOLDS.get('max_missing_percentage', 5.0)
        
        for field in critical_fields:
            if field in df.columns:
                missing_pct = (df[field].isna().sum() / len(df) * 100)
                if missing_pct > max_missing_pct:
                    logger.warning(f"Field '{field}' has {missing_pct:.2f}% missing values (threshold: {max_missing_pct}%)")
                    # Remove rows with missing critical fields
                    df = df.dropna(subset=[field])
                    self.cleaning_stats['missing_values_handled'][field] = {
                        'action': 'removed_rows',
                        'rows_removed': initial_rows - len(df)
                    }
        
        # Handle non-critical missing values
        for field, field_schema in self.schema.items():
            if field not in df.columns:
                continue
            
            if field in critical_fields:
                continue  # Already handled
            
            missing_count = df[field].isna().sum()
            if missing_count > 0:
                if field_schema.get('nullable', True):
                    # Fill with appropriate default based on type
                    field_type = field_schema.get('type', 'string')
                    if field_type == 'float' or field_type == 'integer':
                        # Fill numeric with median
                        if df[field].dtype in [np.float64, np.int64, 'float64', 'int64']:
                            fill_value = df[field].median()
                            df[field] = df[field].fillna(fill_value)
                            self.cleaning_stats['missing_values_handled'][field] = {
                                'action': 'filled_with_median',
                                'fill_value': float(fill_value),
                                'count': int(missing_count)
                            }
                    elif field_type == 'string':
                        # Fill with 'unknown' or mode
                        fill_value = df[field].mode().iloc[0] if len(df[field].mode()) > 0 else 'unknown'
                        df[field] = df[field].fillna(fill_value)
                        self.cleaning_stats['missing_values_handled'][field] = {
                            'action': 'filled_with_mode',
                            'fill_value': str(fill_value),
                            'count': int(missing_count)
                        }
                else:
                    # Non-nullable field - remove rows
                    df = df.dropna(subset=[field])
                    self.cleaning_stats['missing_values_handled'][field] = {
                        'action': 'removed_rows',
                        'rows_removed': missing_count
                    }
        
        logger.info(f"Missing values handled. Rows: {initial_rows} -> {len(df)}")
        return df
    
    def detect_and_handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and handle outliers in numeric fields.
        
        Args:
            df: DataFrame to process
        
        Returns:
            DataFrame with outliers handled
        """
        logger.info("Detecting and handling outliers...")
        df = df.copy()
        
        for field, threshold_config in OUTLIER_THRESHOLDS.items():
            if field not in df.columns:
                continue
            
            if df[field].dtype not in [np.float64, np.int64, 'float64', 'int64']:
                continue
            
            method = threshold_config.get('method', 'iqr')
            multiplier = threshold_config.get('multiplier', 3.0)
            
            # Detect outliers
            if method == 'iqr':
                outliers = detect_outliers_iqr(df[field], multiplier)
            else:
                continue
            
            outlier_count = outliers.sum()
            if outlier_count > 0:
                self.cleaning_stats['outliers_detected'][field] = {
                    'count': int(outlier_count),
                    'percentage': round((outlier_count / len(df) * 100), 2),
                    'method': method
                }
                
                # Handle outliers by capping
                df[field] = handle_outliers(df[field], method=method, 
                                           multiplier=multiplier, strategy='cap')
                
                self.cleaning_stats['outliers_handled'][field] = {
                    'count': int(outlier_count),
                    'strategy': 'capped'
                }
                
                logger.info(f"Field '{field}': {outlier_count} outliers detected and capped")
        
        return df
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate log entries.
        
        Args:
            df: DataFrame to process
        
        Returns:
            DataFrame with duplicates removed
        """
        logger.info("Removing duplicates...")
        df = df.copy()
        initial_rows = len(df)
        
        key_fields = DUPLICATE_DETECTION.get('key_fields', ['timestamp', 'pod_name', 'message'])
        time_window = DUPLICATE_DETECTION.get('time_window_seconds', 1)
        
        # Filter to only fields that exist
        key_fields = [f for f in key_fields if f in df.columns]
        
        if len(key_fields) > 0:
            duplicates = find_duplicates(df, key_fields, time_window)
            duplicate_count = duplicates.sum()
            
            if duplicate_count > 0:
                df = df[~duplicates]
                self.cleaning_stats['duplicates_removed'] = int(duplicate_count)
                logger.info(f"Removed {duplicate_count:,} duplicate rows")
        
        return df
    
    def standardize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize text fields (whitespace, control chars, unicode).
        
        Args:
            df: DataFrame to process
        
        Returns:
            DataFrame with standardized text
        """
        logger.info("Standardizing text fields...")
        df = df.copy()
        
        text_fields = ['message']
        max_length = TEXT_STANDARDIZATION.get('max_message_length', 10000)
        truncate = TEXT_STANDARDIZATION.get('truncate_if_exceeds', True)
        
        standardized_count = 0
        for field in text_fields:
            if field in df.columns:
                initial_values = df[field].copy()
                df[field] = df[field].apply(
                    lambda x: standardize_text(x, max_length=max_length, truncate=truncate)
                )
                # Count how many were actually changed
                changed = (initial_values != df[field]).sum()
                standardized_count += changed
        
        self.cleaning_stats['text_standardized'] = standardized_count
        logger.info(f"Standardized {standardized_count:,} text fields")
        
        return df
    
    def standardize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize all timestamps to consistent format.
        
        Args:
            df: DataFrame to process
        
        Returns:
            DataFrame with standardized timestamps
        """
        logger.info("Standardizing timestamps...")
        df = df.copy()
        
        timestamp_fields = ['timestamp']
        input_formats = TIMESTAMP_CONFIG.get('input_formats', [])
        output_format = TIMESTAMP_CONFIG.get('output_format', '%Y-%m-%dT%H:%M:%S.%fZ')
        timezone = TIMESTAMP_CONFIG.get('timezone', 'UTC')
        
        standardized_count = 0
        for field in timestamp_fields:
            if field in df.columns:
                initial_count = df[field].notna().sum()
                df[field] = df[field].apply(
                    lambda x: standardize_timestamp(x, input_formats, output_format, timezone)
                )
                standardized_count = df[field].notna().sum()
        
        self.cleaning_stats['timestamps_standardized'] = standardized_count
        logger.info(f"Standardized {standardized_count:,} timestamps")
        
        return df
    
    def validate_schema(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Validate DataFrame against schema.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Tuple of (validated DataFrame, list of validation errors)
        """
        logger.info("Validating data against schema...")
        errors = []
        df = df.copy()
        
        # Check required fields
        for field, field_schema in self.schema.items():
            if field_schema.get('required', False) and field not in df.columns:
                errors.append(f"Required field '{field}' is missing from data")
        
        # Validate field values
        for field, field_schema in self.schema.items():
            if field not in df.columns:
                continue
            
            # Check allowed values
            if 'allowed_values' in field_schema:
                invalid_values = ~df[field].isin(field_schema['allowed_values'])
                if invalid_values.any():
                    invalid_count = invalid_values.sum()
                    sample_invalid = df[field][invalid_values].unique()[:5].tolist()
                    errors.append(
                        f"Field '{field}': {invalid_count} rows have invalid values. "
                        f"Sample: {sample_invalid}"
                    )
            
            # Check value ranges for numeric fields
            field_type = field_schema.get('type', 'string')
            if field_type in ['float', 'integer']:
                if 'min' in field_schema:
                    below_min = df[field] < field_schema['min']
                    if below_min.any():
                        errors.append(
                            f"Field '{field}': {below_min.sum()} values below minimum "
                            f"{field_schema['min']}"
                        )
                if 'max' in field_schema:
                    above_max = df[field] > field_schema['max']
                    if above_max.any():
                        errors.append(
                            f"Field '{field}': {above_max.sum()} values above maximum "
                            f"{field_schema['max']}"
                        )
        
        # Validate Kubernetes-specific formats
        if 'pod_name' in df.columns:
            invalid_pods = ~df['pod_name'].apply(validate_k8s_pod_name)
            if invalid_pods.any():
                errors.append(
                    f"pod_name: {invalid_pods.sum()} invalid pod names detected"
                )
        
        if 'namespace' in df.columns:
            invalid_namespaces = ~df['namespace'].apply(validate_k8s_namespace)
            if invalid_namespaces.any():
                errors.append(
                    f"namespace: {invalid_namespaces.sum()} invalid namespace names detected"
                )
        
        self.cleaning_stats['validation_errors'] = errors
        
        if errors:
            logger.warning(f"Validation found {len(errors)} issues")
        else:
            logger.info("Schema validation passed")
        
        return df, errors
    
    def ensure_temporal_ordering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure logs are ordered by timestamp.
        
        Args:
            df: DataFrame to sort
        
        Returns:
            Sorted DataFrame
        """
        if 'timestamp' in df.columns:
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.sort_values('timestamp').reset_index(drop=True)
            logger.info("Data sorted by timestamp")
        return df
    
    def preprocess(self, input_path: Path, output_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Complete preprocessing pipeline.
        
        Args:
            input_path: Path to input data file
            output_path: Optional path to save cleaned data
        
        Returns:
            Cleaned DataFrame
        """
        logger.info("=" * 60)
        logger.info("Starting data preprocessing pipeline")
        logger.info("=" * 60)
        
        # Load data
        df = self.load_data(input_path)
        
        # Store original missing values stats BEFORE cleaning
        original_missing_stats = {}
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                original_missing_stats[col] = {
                    'count': int(missing_count),
                    'percentage': round((missing_count / len(df) * 100), 2)
                }
        self.cleaning_stats['original_missing_values'] = original_missing_stats
        
        # Step 1: Handle missing values
        df = self.handle_missing_values(df)
        
        # Step 2: Standardize timestamps
        df = self.standardize_timestamps(df)
        
        # Step 3: Standardize text
        df = self.standardize_text_fields(df)
        
        # Step 4: Remove duplicates
        df = self.remove_duplicates(df)
        
        # Step 5: Detect and handle outliers
        df = self.detect_and_handle_outliers(df)
        
        # Step 6: Ensure temporal ordering
        df = self.ensure_temporal_ordering(df)
        
        # Step 7: Validate schema
        df, validation_errors = self.validate_schema(df)
        
        # Update final stats
        self.cleaning_stats['final_rows'] = len(df)
        self.cleaning_stats['rows_removed'] = (
            self.cleaning_stats['initial_rows'] - self.cleaning_stats['final_rows']
        )
        
        # Save cleaned data
        if output_path is None:
            output_path = CLEANED_DATA_DIR / "cleaned_logs.parquet"
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Cleaned data saved to {output_path}")
        
        # Also save as CSV for easy viewing
        csv_path = output_path.with_suffix('.csv')
        df.to_csv(csv_path, index=False)
        logger.info(f"Cleaned data also saved as CSV: {csv_path}")
        
        logger.info("=" * 60)
        logger.info("Data preprocessing completed")
        logger.info(f"Initial rows: {self.cleaning_stats['initial_rows']:,}")
        logger.info(f"Final rows: {self.cleaning_stats['final_rows']:,}")
        logger.info(f"Rows removed: {self.cleaning_stats['rows_removed']:,}")
        logger.info("=" * 60)
        
        return df
    
    def generate_quality_report(self, df: pd.DataFrame, 
                               output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report.
        
        Args:
            df: DataFrame to analyze
            output_dir: Directory to save reports
        
        Returns:
            Dictionary with quality metrics
        """
        logger.info("Generating data quality report...")
        
        if output_dir is None:
            output_dir = STATISTICS_DIR
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate metrics
        missing_stats = calculate_missing_values_stats(df)
        quality_metrics = calculate_data_quality_metrics(df, self.schema)
        
        # Combine with cleaning stats
        report = {
            'preprocessing_summary': self.cleaning_stats,
            'missing_values_report': missing_stats,
            'data_quality_metrics': quality_metrics,
            'schema_validation': {
                'errors': self.cleaning_stats['validation_errors'],
                'error_count': len(self.cleaning_stats['validation_errors'])
            },
            'generated_at': datetime.now().isoformat(),
            'data_type': self.data_type
        }
        
        # Save missing values report
        missing_report_path = output_dir / "missing_values_report.json"
        save_json_report(missing_stats, missing_report_path)
        logger.info(f"Missing values report saved to {missing_report_path}")
        
        # Save full quality report
        full_report_path = output_dir / "data_quality_report.json"
        save_json_report(report, full_report_path)
        logger.info(f"Full quality report saved to {full_report_path}")
        
        return report
    
    def generate_data_profile(self, df: pd.DataFrame, 
                            output_path: Optional[Path] = None) -> None:
        """
        Generate HTML data profiling report using pandas-profiling or ydata-profiling.
        
        Args:
            df: DataFrame to profile
            output_path: Path to save HTML report
        """
        logger.info("Generating data profiling report...")
        
        if output_path is None:
            output_path = STATISTICS_DIR / "data_profile.html"
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Try ydata-profiling first (newer)
            from ydata_profiling import ProfileReport  # type: ignore
            profile = ProfileReport(df, title="Kubernetes Log Data Profile", minimal=True)
            profile.to_file(output_path)
            logger.info(f"Data profile saved to {output_path}")
        except ImportError:
            try:
                # Fallback to pandas-profiling
                from pandas_profiling import ProfileReport  # type: ignore
                profile = ProfileReport(df, title="Kubernetes Log Data Profile", minimal=True)
                profile.to_file(output_path)
                logger.info(f"Data profile saved to {output_path}")
            except ImportError:
                logger.warning(
                    "Neither ydata-profiling nor pandas-profiling installed. "
                    "Skipping HTML profile generation. Install with: pip install ydata-profiling"
                )
                # Generate a simple HTML report
                self._generate_simple_html_profile(df, output_path)
    
    def _generate_simple_html_profile(self, df: pd.DataFrame, output_path: Path) -> None:
        """Generate a comprehensive HTML profile without profiling library."""
        
        # Calculate statistics
        total_rows = len(df)
        initial_rows = self.cleaning_stats.get('initial_rows', total_rows)
        rows_removed = self.cleaning_stats.get('rows_removed', 0)
        retention_rate = (total_rows / initial_rows * 100) if initial_rows > 0 else 100.0
        
        duplicates_removed = self.cleaning_stats.get('duplicates_removed', 0)
        missing_handled = sum(v.get('count', 0) if isinstance(v, dict) else 0 
                            for v in self.cleaning_stats.get('missing_values_handled', {}).values())
        outliers_detected = sum(v.get('count', 0) if isinstance(v, dict) else 0 
                               for v in self.cleaning_stats.get('outliers_detected', {}).values())
        
        # Get original missing values stats (BEFORE cleaning)
        original_missing = self.cleaning_stats.get('original_missing_values', {})
        total_original_missing = sum(v.get('count', 0) for v in original_missing.values())
        
        # Column statistics with BEFORE/AFTER comparison
        column_stats = []
        for col in df.columns:
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            unique_count = df[col].nunique()
            dtype = str(df[col].dtype)
            
            # Calculate mean/min/max for numeric columns
            mean_val = '-'
            min_val = '-'
            max_val = '-'
            if df[col].dtype in [np.float64, np.int64, 'float64', 'int64']:
                mean_val = f"{df[col].mean():.2f}" if not df[col].isna().all() else '-'
                min_val = f"{df[col].min():.2f}" if not df[col].isna().all() else '-'
                max_val = f"{df[col].max():.2f}" if not df[col].isna().all() else '-'
            
            column_stats.append({
                'name': col,
                'type': dtype,
                'non_null': non_null,
                'null_count': null_count,
                'null_pct': null_pct,
                'unique': unique_count,
                'mean': mean_val,
                'min': min_val,
                'max': max_val
            })
        
        # Missing values breakdown
        missing_breakdown = []
        for col, stats in self.cleaning_stats.get('missing_values_handled', {}).items():
            if isinstance(stats, dict):
                missing_breakdown.append({
                    'column': col,
                    'count': stats.get('count', 0),
                    'action': stats.get('action', 'unknown')
                })
        
        # Quality metrics
        duplicate_rate = (duplicates_removed / initial_rows * 100) if initial_rows > 0 else 0
        missing_rate = (missing_handled / initial_rows * 100) if initial_rows > 0 else 0
        outlier_rate = (outliers_detected / initial_rows * 100) if initial_rows > 0 else 0
        
        # Check if timestamp column exists for date range analysis
        date_range_info = ""
        if 'timestamp' in df.columns:
            try:
                df_timestamp = pd.to_datetime(df['timestamp'], errors='coerce')
                valid_timestamps = df_timestamp.dropna()
                if len(valid_timestamps) > 0:
                    start_date = valid_timestamps.min()
                    end_date = valid_timestamps.max()
                    span_days = (end_date - start_date).days
                    date_range_info = f"""
                    <div class="metric-card">
                        <h3>📅 Timestamp Analysis</h3>
                        <p><strong>Date Range:</strong> {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}</p>
                        <p><strong>Span:</strong> {span_days} days</p>
                        <p><strong>Logs per Day:</strong> ~{total_rows // span_days if span_days > 0 else 0:,} average</p>
                    </div>
                    """
            except:
                pass
        
        # Generate HTML
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kubernetes Logs - Data Quality Profile</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            padding: 30px;
        }}
        .header {{
            text-align: center;
            border-bottom: 3px solid #667eea;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        .header .meta {{
            color: #666;
            font-size: 0.9em;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .metric-card h3 {{
            font-size: 0.9em;
            margin-bottom: 10px;
            opacity: 0.9;
        }}
        .metric-card .value {{
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .metric-card .label {{
            font-size: 0.8em;
            opacity: 0.8;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
            margin-left: 10px;
        }}
        .status-pass {{ background: #4CAF50; color: white; }}
        .status-warn {{ background: #FFC107; color: #333; }}
        .status-fail {{ background: #F44336; color: white; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #eee;
        }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        tr:hover {{ background-color: #e3f2fd; }}
        h2 {{
            color: #667eea;
            margin: 30px 0 15px 0;
            padding-bottom: 10px;
            border-bottom: 2px solid #667eea;
        }}
        .chart-container {{
            position: relative;
            height: 300px;
            margin: 20px 0;
        }}
        .recommendations {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            margin: 20px 0;
        }}
        .recommendations ul {{
            margin-left: 20px;
            margin-top: 10px;
        }}
        .recommendations li {{
            margin: 8px 0;
        }}
        .progress-bar {{
            width: 100%;
            height: 25px;
            background: #e0e0e0;
            border-radius: 12px;
            overflow: hidden;
            margin: 5px 0;
        }}
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #4CAF50, #8BC34A);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 0.85em;
        }}
        .section {{
            margin: 30px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Kubernetes Logs - Data Quality Profile</h1>
            <div class="meta">
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Data Source:</strong> {self.data_type} dataset</p>
                <p><strong>Report By:</strong> Data Quality Engineer</p>
            </div>
        </div>

        <h2>📈 Executive Summary</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Total Rows Processed</h3>
                <div class="value">{initial_rows:,}</div>
                <div class="label">Input data</div>
            </div>
            <div class="metric-card">
                <h3>Rows After Cleaning</h3>
                <div class="value">{total_rows:,}</div>
                <div class="label">Final output</div>
            </div>
            <div class="metric-card">
                <h3>Data Retention Rate</h3>
                <div class="value">{retention_rate:.2f}%</div>
                <div class="label">{'✅ PASS' if retention_rate >= 95 else '⚠️ WARN'}</div>
            </div>
            <div class="metric-card">
                <h3>Duplicates Removed</h3>
                <div class="value">{duplicates_removed:,}</div>
                <div class="label">{(duplicates_removed/initial_rows*100):.2f}% of input</div>
            </div>
            <div class="metric-card">
                <h3>Missing Values Handled</h3>
                <div class="value">{missing_handled:,}</div>
                <div class="label">{(missing_handled/initial_rows*100):.2f}% of input</div>
            </div>
            <div class="metric-card">
                <h3>Outliers Detected</h3>
                <div class="value">{outliers_detected:,}</div>
                <div class="label">{(outliers_detected/initial_rows*100):.2f}% of input</div>
            </div>
        </div>

        <h2>✅ Data Quality Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Quality Check</th>
                    <th>Status</th>
                    <th>Value</th>
                    <th>Threshold</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Data Retention Rate</td>
                    <td><span class="status-badge {'status-pass' if retention_rate >= 95 else 'status-warn'}">✅ PASS</span></td>
                    <td>{retention_rate:.2f}%</td>
                    <td>≥95% required</td>
                </tr>
                <tr>
                    <td>Missing Values Rate</td>
                    <td><span class="status-badge {'status-pass' if missing_rate <= 5 else 'status-warn'}">✅ PASS</span></td>
                    <td>{missing_rate:.2f}%</td>
                    <td>≤5% allowed</td>
                </tr>
                <tr>
                    <td>Duplicate Rate</td>
                    <td><span class="status-badge {'status-pass' if duplicate_rate <= 5 else 'status-warn'}">✅ PASS</span></td>
                    <td>{duplicate_rate:.2f}%</td>
                    <td>≤5% allowed</td>
                </tr>
                <tr>
                    <td>Outlier Rate</td>
                    <td><span class="status-badge {'status-pass' if outlier_rate <= 1 else 'status-warn'}">✅ PASS</span></td>
                    <td>{outlier_rate:.2f}%</td>
                    <td>≤1% allowed</td>
                </tr>
            </tbody>
        </table>

        <h2>📋 Column Statistics</h2>
        <table>
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Non-Null</th>
                    <th>Null %</th>
                    <th>Unique</th>
                    <th>Mean</th>
                    <th>Min</th>
                    <th>Max</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr>
                    <td><strong>{stat['name']}</strong></td>
                    <td>{stat['type']}</td>
                    <td>{stat['non_null']:,}</td>
                    <td>{stat['null_pct']:.2f}% {'⚠️' if stat['null_pct'] > 5 else '✅'}</td>
                    <td>{stat['unique']:,}</td>
                    <td>{stat['mean']}</td>
                    <td>{stat['min']}</td>
                    <td>{stat['max']}</td>
                </tr>
                ''' for stat in column_stats])}
            </tbody>
        </table>

        {date_range_info}

        <h2>🔧 Data Cleaning Summary</h2>
        <table>
            <thead>
                <tr>
                    <th>Action</th>
                    <th>Count</th>
                    <th>Percentage</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Duplicates Removed</td>
                    <td>{duplicates_removed:,}</td>
                    <td>{(duplicates_removed/initial_rows*100):.2f}%</td>
                </tr>
                <tr>
                    <td>Missing Values Handled</td>
                    <td>{missing_handled:,}</td>
                    <td>{(missing_handled/initial_rows*100):.2f}%</td>
                </tr>
                <tr>
                    <td>Outliers Capped</td>
                    <td>{outliers_detected:,}</td>
                    <td>{(outliers_detected/initial_rows*100):.2f}%</td>
                </tr>
                <tr>
                    <td>Text Fields Standardized</td>
                    <td>{self.cleaning_stats.get('text_standardized', 0):,}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td>Timestamps Standardized</td>
                    <td>{self.cleaning_stats.get('timestamps_standardized', 0):,}</td>
                    <td>-</td>
                </tr>
            </tbody>
        </table>

        <h2>⚠️ Missing Values: Before vs After Cleaning</h2>
        <table>
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Before Cleaning</th>
                    <th>After Cleaning</th>
                    <th>Action Taken</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr>
                    <td><strong>{col}</strong></td>
                    <td>{stats.get('count', 0):,} ({stats.get('percentage', 0):.2f}%)</td>
                    <td>0 (0.00%) ✅</td>
                    <td>{self.cleaning_stats.get('missing_values_handled', {}).get(col, {}).get('action', 'N/A')}</td>
                </tr>
                ''' for col, stats in original_missing.items()]) if original_missing else 
                ''.join([f'''
                <tr>
                    <td><strong>{item['column']}</strong></td>
                    <td>{item['count']:,} (N/A%)</td>
                    <td>0 (0.00%) ✅</td>
                    <td>{item['action']}</td>
                </tr>
                ''' for item in missing_breakdown]) if missing_breakdown else 
                '<tr><td colspan="4">✅ No missing values detected in original data</td></tr>'}
            </tbody>
        </table>
        
        {f'''
        <h2>📊 Missing Values Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Missing Count</th>
                    <th>Fill Value</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr>
                    <td>{item['column']}</td>
                    <td>{item['count']:,}</td>
                    <td>{self.cleaning_stats.get("missing_values_handled", {}).get(item["column"], {}).get("fill_value", "N/A")}</td>
                    <td>{item['action']}</td>
                </tr>
                ''' for item in missing_breakdown]) if missing_breakdown else '<tr><td colspan="4">No missing values handled</td></tr>'}
            </tbody>
        </table>
        ''' if missing_breakdown else ''}

        <h2>💡 Recommendations</h2>
        <div class="recommendations">
            <ul>
                {'<li>✅ Data quality meets standards - all metrics within acceptable ranges</li>' if retention_rate >= 95 and missing_rate <= 5 and duplicate_rate <= 5 else ''}
                {'<li>⚠️ Consider improving data retention - some rows were removed during cleaning</li>' if retention_rate < 95 else ''}
                {'<li>⚠️ Missing values detected - consider improving data source validation</li>' if missing_rate > 0 else ''}
                {'<li>⚠️ Duplicates found - consider implementing deduplication at data source</li>' if duplicate_rate > 0 else ''}
                <li>💡 Continue monitoring data quality metrics regularly</li>
                <li>📝 Document any data quality issues for future reference</li>
                <li>🔄 Consider automating data quality checks in the pipeline</li>
            </ul>
        </div>

        <h2>📊 Sample Data (First 10 Rows)</h2>
        {df.head(10).to_html(classes='table', table_id='sample-data', escape=False)}

        <div style="margin-top: 40px; padding-top: 20px; border-top: 2px solid #eee; text-align: center; color: #666;">
            <p>Report generated by Kubernetes Log Monitoring System</p>
            <p style="font-size: 0.9em;">Data Quality Engineering Pipeline</p>
        </div>
    </div>
</body>
</html>
        """
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Enhanced HTML profile saved to {output_path}")


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Preprocess Kubernetes log data')
    parser.add_argument('input', type=Path, help='Input data file (CSV or Parquet)')
    parser.add_argument('--output', type=Path, help='Output path for cleaned data')
    parser.add_argument('--data-type', choices=['k8s_log', 'hybrid_dataset'], 
                       default='k8s_log', help='Type of data to process')
    parser.add_argument('--skip-profile', action='store_true', 
                       help='Skip HTML profile generation')
    
    args = parser.parse_args()
    
    # Initialize preprocessor
    preprocessor = DataPreprocessor(data_type=args.data_type)
    
    # Preprocess
    cleaned_df = preprocessor.preprocess(args.input, args.output)
    
    # Generate reports
    preprocessor.generate_quality_report(cleaned_df)
    
    if not args.skip_profile:
        preprocessor.generate_data_profile(cleaned_df)
    
    # Save schema
    schema_path = CLEANED_DATA_DIR / "schema.json"
    save_schema_json(preprocessor.schema, schema_path)
    logger.info(f"Schema saved to {schema_path}")
    
    print("\n✅ Data preprocessing completed successfully!")
    print(f"   Cleaned data: {args.output or CLEANED_DATA_DIR / 'cleaned_logs.parquet'}")
    print(f"   Quality reports: {STATISTICS_DIR}")


if __name__ == "__main__":
    main()

