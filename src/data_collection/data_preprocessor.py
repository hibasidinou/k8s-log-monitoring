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
        """Generate a simple HTML profile without profiling library."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Kubernetes Log Data Profile</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <h1>Kubernetes Log Data Profile</h1>
            <p><strong>Generated:</strong> {datetime.now().isoformat()}</p>
            <p><strong>Total Rows:</strong> {len(df):,}</p>
            <p><strong>Total Columns:</strong> {len(df.columns)}</p>
            
            <h2>Data Summary</h2>
            {df.describe().to_html()}
            
            <h2>Data Types</h2>
            <table>
                <tr><th>Column</th><th>Data Type</th><th>Non-Null Count</th><th>Null Count</th></tr>
                {''.join([f'<tr><td>{col}</td><td>{dtype}</td><td>{df[col].notna().sum():,}</td><td>{df[col].isna().sum():,}</td></tr>' 
                         for col, dtype in df.dtypes.items()])}
            </table>
            
            <h2>Sample Data</h2>
            {df.head(100).to_html()}
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Simple HTML profile saved to {output_path}")


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

