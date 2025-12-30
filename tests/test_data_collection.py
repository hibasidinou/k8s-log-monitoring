"""
Tests for data collection and preprocessing functionality.
"""
import pytest  # type: ignore[reportMissingImports]
import pandas as pd  # type: ignore[reportMissingImports]
import numpy as np  # type: ignore[reportMissingImports]
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import shutil

from src.data_collection.data_preprocessor import DataPreprocessor
from src.utils.config import K8S_LOG_SCHEMA, HYBRID_DATASET_SCHEMA, get_schema_for_data_type
from src.utils.helpers import (
    standardize_text, standardize_timestamp, detect_outliers_iqr,
    handle_outliers, find_duplicates, calculate_missing_values_stats,
    calculate_data_quality_metrics, validate_k8s_pod_name, validate_k8s_namespace
)


class TestDataPreprocessor:
    """Test suite for DataPreprocessor class."""
    
    @pytest.fixture
    def sample_k8s_data(self):
        """Create sample Kubernetes log data for testing."""
        data = {
            'timestamp': [
                datetime.now().isoformat(),
                (datetime.now() - timedelta(minutes=1)).isoformat(),
                (datetime.now() - timedelta(minutes=2)).isoformat(),
            ],
            'log_level': ['INFO', 'WARNING', 'ERROR'],
            'message': ['Pod started', 'High CPU usage', 'Pod crashed'],
            'pod_name': ['test-pod-123', 'test-pod-456', 'test-pod-789'],
            'namespace': ['default', 'production', 'staging'],
            'container_name': ['container-1', 'container-2', 'container-3'],
            'node_name': ['node-1', 'node-2', 'node-3'],
            'service': ['frontend', 'backend', 'database'],
            'cpu_usage_percent': [50.0, 85.0, 95.0],
            'memory_usage_mb': [1024.0, 2048.0, 4096.0],
            'network_bytes': [100000, 500000, 1000000],
            'is_anomaly': [0, 1, 1],
            'anomaly_type': ['normal', 'cpu_spike', 'pod_crash']
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def sample_hybrid_data(self):
        """Create sample hybrid dataset for testing."""
        data = {
            'cpu_usage': [10.0, 50.0, 90.0],
            'memory_usage': [512.0, 2048.0, 4096.0],
            'label': [0, 1, 1],
            'anomaly_type': ['normal', 'system_anomaly', 'security_attack'],
            'packets_count': [1000.0, 5000.0, 10000.0],
            'flow_bytes_per_second': [100.0, 500.0, 1000.0]
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    def test_preprocessor_initialization(self):
        """Test DataPreprocessor initialization."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        assert preprocessor.data_type == 'k8s_log'
        assert preprocessor.schema == K8S_LOG_SCHEMA
        assert preprocessor.cleaning_stats['initial_rows'] == 0
    
    def test_handle_missing_values(self, sample_k8s_data):
        """Test missing value handling."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add missing values
        df = sample_k8s_data.copy()
        df.loc[0, 'message'] = None
        df.loc[1, 'cpu_usage_percent'] = np.nan
        
        cleaned_df = preprocessor.handle_missing_values(df)
        
        # Critical field (message) should have row removed
        assert len(cleaned_df) < len(df) or cleaned_df['message'].notna().all()
        # Non-critical numeric field should be filled
        assert cleaned_df['cpu_usage_percent'].notna().all()
    
    def test_detect_outliers(self, sample_k8s_data):
        """Test outlier detection."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add outliers
        df = sample_k8s_data.copy()
        df.loc[0, 'cpu_usage_percent'] = 150.0  # Outlier
        df.loc[1, 'memory_usage_mb'] = 200000.0  # Outlier
        
        cleaned_df = preprocessor.detect_and_handle_outliers(df)
        
        # Outliers should be capped
        assert cleaned_df['cpu_usage_percent'].max() <= 100.0
        assert cleaned_df['memory_usage_mb'].max() < 200000.0
    
    def test_remove_duplicates(self, sample_k8s_data):
        """Test duplicate removal."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add duplicates
        df = pd.concat([sample_k8s_data, sample_k8s_data.iloc[[0]]], ignore_index=True)
        
        cleaned_df = preprocessor.remove_duplicates(df)
        
        assert len(cleaned_df) == len(sample_k8s_data)
        assert preprocessor.cleaning_stats['duplicates_removed'] > 0
    
    def test_standardize_text(self, sample_k8s_data):
        """Test text standardization."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add text with issues
        df = sample_k8s_data.copy()
        df.loc[0, 'message'] = '  Pod   started  \n\t'
        
        cleaned_df = preprocessor.standardize_text_fields(df)
        
        assert cleaned_df.loc[0, 'message'] == 'Pod started'
    
    def test_standardize_timestamps(self, sample_k8s_data):
        """Test timestamp standardization."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add various timestamp formats
        df = sample_k8s_data.copy()
        df.loc[0, 'timestamp'] = '2024-01-01T12:00:00'
        df.loc[1, 'timestamp'] = '2024-01-01 12:00:00'
        
        cleaned_df = preprocessor.standardize_timestamps(df)
        
        # All timestamps should be standardized
        assert cleaned_df['timestamp'].notna().all()
        assert all('T' in str(ts) for ts in cleaned_df['timestamp'] if pd.notna(ts))
    
    def test_validate_schema(self, sample_k8s_data):
        """Test schema validation."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Add invalid values
        df = sample_k8s_data.copy()
        df.loc[0, 'log_level'] = 'INVALID'
        df.loc[1, 'cpu_usage_percent'] = 150.0
        
        validated_df, errors = preprocessor.validate_schema(df)
        
        assert len(errors) > 0
        assert any('log_level' in error for error in errors)
    
    def test_full_preprocessing_pipeline(self, sample_k8s_data, temp_dir):
        """Test complete preprocessing pipeline."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Save sample data
        input_path = temp_dir / "test_input.csv"
        sample_k8s_data.to_csv(input_path, index=False)
        
        # Run preprocessing
        output_path = temp_dir / "cleaned.parquet"
        cleaned_df = preprocessor.preprocess(input_path, output_path)
        
        # Verify output
        assert len(cleaned_df) > 0
        assert output_path.exists()
        
        # Verify stats
        assert preprocessor.cleaning_stats['initial_rows'] > 0
        assert preprocessor.cleaning_stats['final_rows'] > 0


class TestHelperFunctions:
    """Test suite for helper functions."""
    
    def test_standardize_text(self):
        """Test text standardization helper."""
        text = "  Hello   World  \n\t"
        result = standardize_text(text)
        assert result == "Hello World"
        
        # Test with max length
        long_text = "A" * 20000
        result = standardize_text(long_text, max_length=100, truncate=True)
        assert len(result) == 100
    
    def test_standardize_timestamp(self):
        """Test timestamp standardization helper."""
        formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
        
        result1 = standardize_timestamp('2024-01-01T12:00:00', formats)
        assert result1 is not None
        assert 'T' in result1
        
        result2 = standardize_timestamp('2024-01-01 12:00:00', formats)
        assert result2 is not None
    
    def test_detect_outliers_iqr(self):
        """Test IQR outlier detection."""
        # Create data with outliers
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])  # 100 is outlier
        outliers = detect_outliers_iqr(data, multiplier=1.5)
        
        assert outliers.sum() > 0
        assert outliers.iloc[-1] == True  # Last value is outlier
    
    def test_handle_outliers(self):
        """Test outlier handling."""
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])
        handled = handle_outliers(data, method='iqr', strategy='cap')
        
        assert handled.max() < 100
        assert handled.min() == data.min()
    
    def test_find_duplicates(self):
        """Test duplicate detection."""
        df = pd.DataFrame({
            'timestamp': ['2024-01-01T12:00:00', '2024-01-01T12:00:00', '2024-01-01T12:01:00'],
            'pod_name': ['pod-1', 'pod-1', 'pod-2'],
            'message': ['msg1', 'msg1', 'msg2']
        })
        
        duplicates = find_duplicates(df, ['timestamp', 'pod_name', 'message'])
        assert duplicates.sum() > 0
    
    def test_calculate_missing_values_stats(self):
        """Test missing values statistics."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', 'b', 'c', np.nan],
            'col3': [1, 2, 3, 4]
        })
        
        stats = calculate_missing_values_stats(df)
        
        assert stats['total_rows'] == 4
        assert 'col1' in stats['columns']
        assert stats['columns']['col1']['missing_count'] == 1
        assert stats['columns']['col3']['missing_count'] == 0
    
    def test_calculate_data_quality_metrics(self):
        """Test data quality metrics calculation."""
        df = pd.DataFrame({
            'numeric_col': [1, 2, 3, 4, 5],
            'text_col': ['a', 'b', 'c', 'd', 'e']
        })
        
        metrics = calculate_data_quality_metrics(df)
        
        assert metrics['total_rows'] == 5
        assert 'numeric_col' in metrics['numeric_stats']
        assert 'text_col' in metrics['text_stats']
    
    def test_validate_k8s_pod_name(self):
        """Test Kubernetes pod name validation."""
        assert validate_k8s_pod_name('test-pod-123') == True
        assert validate_k8s_pod_name('test.pod.123') == True
        assert validate_k8s_pod_name('Test-Pod-123') == False  # Capital letters
        assert validate_k8s_pod_name('') == False
        assert validate_k8s_pod_name(None) == False
    
    def test_validate_k8s_namespace(self):
        """Test Kubernetes namespace validation."""
        assert validate_k8s_namespace('default') == True
        assert validate_k8s_namespace('production') == True
        assert validate_k8s_namespace('Test-Namespace') == False  # Capital letters
        assert validate_k8s_namespace('') == False
        assert validate_k8s_namespace(None) == False


class TestSchemaValidation:
    """Test suite for schema validation."""
    
    def test_get_schema_for_data_type(self):
        """Test schema retrieval by data type."""
        k8s_schema = get_schema_for_data_type('k8s_log')
        assert k8s_schema == K8S_LOG_SCHEMA
        
        hybrid_schema = get_schema_for_data_type('hybrid_dataset')
        assert hybrid_schema == HYBRID_DATASET_SCHEMA
        
        # Default to k8s_log for unknown types
        default_schema = get_schema_for_data_type('unknown')
        assert default_schema == K8S_LOG_SCHEMA


class TestDataQuality:
    """Test suite for data quality checks."""
    
    def test_missing_critical_fields(self):
        """Test handling of missing critical fields."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        # Create data missing critical field
        df = pd.DataFrame({
            'timestamp': ['2024-01-01T12:00:00'],
            'log_level': ['INFO'],
            # Missing 'message' (critical)
            'pod_name': ['test-pod']
        })
        
        cleaned_df = preprocessor.handle_missing_values(df)
        
        # Should remove rows with missing critical fields
        assert len(cleaned_df) == 0 or 'message' in cleaned_df.columns
    
    def test_outlier_capping(self):
        """Test that outliers are properly capped."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        df = pd.DataFrame({
            'cpu_usage_percent': [10, 20, 30, 40, 50, 200]  # 200 is outlier
        })
        
        cleaned_df = preprocessor.detect_and_handle_outliers(df)
        
        # Outlier should be capped
        assert cleaned_df['cpu_usage_percent'].max() < 200
    
    def test_temporal_ordering(self):
        """Test that data is sorted by timestamp."""
        preprocessor = DataPreprocessor(data_type='k8s_log')
        
        df = pd.DataFrame({
            'timestamp': [
                '2024-01-01T12:00:00',
                '2024-01-01T10:00:00',
                '2024-01-01T11:00:00'
            ],
            'message': ['msg1', 'msg2', 'msg3']
        })
        
        ordered_df = preprocessor.ensure_temporal_ordering(df)
        
        # Should be sorted
        timestamps = pd.to_datetime(ordered_df['timestamp'])
        assert timestamps.is_monotonic_increasing


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

