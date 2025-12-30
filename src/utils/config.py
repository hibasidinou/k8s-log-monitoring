"""
Configuration and schema definitions for Kubernetes log monitoring system.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
from pathlib import Path
import pandas as pd  # type: ignore[reportMissingImports]
import numpy as np  # type: ignore[reportMissingImports]

# Base directory paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CLEANED_DATA_DIR = PROCESSED_DATA_DIR / "cleaned"
OUTPUT_DATA_DIR = PROCESSED_DATA_DIR / "output"
STATISTICS_DIR = OUTPUT_DATA_DIR / "statistics"

# Create directories if they don't exist
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
STATISTICS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# SCHEMA DEFINITIONS
# ============================================================================

# Kubernetes log entry schema
K8S_LOG_SCHEMA = {
    'timestamp': {
        'type': 'datetime',
        'required': True,
        'nullable': False,
        'description': 'ISO format timestamp of the log entry',
        'timezone_aware': True
    },
    'log_level': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'allowed_values': ['INFO', 'WARNING', 'ERROR', 'DEBUG', 'CRITICAL'],
        'description': 'Log severity level'
    },
    'message': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'min_length': 1,
        'max_length': 10000,
        'description': 'Log message content'
    },
    'pod_name': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'pattern': r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$',
        'description': 'Kubernetes pod name (RFC 1123 subdomain)'
    },
    'namespace': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'allowed_values': ['default', 'production', 'staging', 'development', 'monitoring', 'kube-system'],
        'description': 'Kubernetes namespace'
    },
    'container_name': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'min_length': 1,
        'max_length': 253,
        'description': 'Container name within the pod'
    },
    'node_name': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'pattern': r'^node-\d+$',
        'description': 'Kubernetes node name'
    },
    'service': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'description': 'Service name associated with the pod'
    },
    'cpu_usage_percent': {
        'type': 'float',
        'required': True,
        'nullable': False,
        'min': 0.0,
        'max': 100.0,
        'description': 'CPU usage percentage (0-100)'
    },
    'memory_usage_mb': {
        'type': 'float',
        'required': True,
        'nullable': False,
        'min': 0.0,
        'max': 100000.0,  # 100GB max
        'description': 'Memory usage in megabytes'
    },
    'network_bytes': {
        'type': 'integer',
        'required': True,
        'nullable': False,
        'min': 0,
        'description': 'Network bytes transferred'
    },
    'is_anomaly': {
        'type': 'integer',
        'required': True,
        'nullable': False,
        'allowed_values': [0, 1],
        'description': 'Binary flag indicating if this is an anomaly (1) or normal (0)'
    },
    'anomaly_type': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'allowed_values': ['normal', 'cpu_spike', 'memory_leak', 'pod_crash', 
                          'network_flood', 'disk_io_bottleneck', 'system_anomaly', 'security_attack'],
        'description': 'Type of anomaly or normal operation'
    }
}

# Hybrid dataset schema (for processed/merged data)
HYBRID_DATASET_SCHEMA = {
    'cpu_usage': {
        'type': 'float',
        'required': True,
        'nullable': True,  # Can be NaN if not available
        'min': 0.0,
        'max': 100.0,
        'description': 'CPU usage percentage'
    },
    'memory_usage': {
        'type': 'float',
        'required': True,
        'nullable': True,
        'min': 0.0,
        'max': 100000.0,
        'description': 'Memory usage in MB'
    },
    'label': {
        'type': 'integer',
        'required': True,
        'nullable': False,
        'allowed_values': [0, 1],
        'description': 'Binary label: 0=normal, 1=anomaly'
    },
    'anomaly_type': {
        'type': 'string',
        'required': True,
        'nullable': False,
        'description': 'Type of anomaly'
    },
    'packets_count': {
        'type': 'float',
        'required': False,
        'nullable': True,
        'min': 0.0,
        'description': 'Network packets count'
    },
    'flow_bytes_per_second': {
        'type': 'float',
        'required': False,
        'nullable': True,
        'min': 0.0,
        'description': 'Network flow bytes per second'
    },
    'flow_packets_per_second': {
        'type': 'float',
        'required': False,
        'nullable': True,
        'min': 0.0,
        'description': 'Network flow packets per second'
    },
    'container_cpu_usage_seconds_rate': {
        'type': 'float',
        'required': False,
        'nullable': True,
        'min': 0.0,
        'description': 'Container CPU usage rate'
    },
    'container_memory_usage_bytes': {
        'type': 'float',
        'required': False,
        'nullable': True,
        'min': 0.0,
        'description': 'Container memory usage in bytes'
    },
    'anomaly_type_encoded': {
        'type': 'integer',
        'required': False,
        'nullable': True,
        'description': 'Encoded anomaly type'
    }
}

# ============================================================================
# DATA QUALITY THRESHOLDS
# ============================================================================

# Outlier detection thresholds (using IQR method)
OUTLIER_THRESHOLDS = {
    'cpu_usage_percent': {
        'method': 'iqr',
        'multiplier': 3.0,  # 3 * IQR
        'min': 0.0,
        'max': 100.0
    },
    'memory_usage_mb': {
        'method': 'iqr',
        'multiplier': 3.0,
        'min': 0.0,
        'max': 100000.0
    },
    'network_bytes': {
        'method': 'iqr',
        'multiplier': 3.0,
        'min': 0
    }
}

# Missing value thresholds
MISSING_VALUE_THRESHOLDS = {
    'critical_fields': ['timestamp', 'log_level', 'message', 'pod_name', 'namespace'],
    'max_missing_percentage': 5.0,  # Max 5% missing for critical fields
    'drop_if_all_missing': True
}

# Duplicate detection
DUPLICATE_DETECTION = {
    'key_fields': ['timestamp', 'pod_name', 'message'],  # Fields to identify duplicates
    'time_window_seconds': 1,  # Consider duplicates within 1 second
    'remove_duplicates': True
}

# ============================================================================
# TIMESTAMP CONFIGURATION
# ============================================================================

TIMESTAMP_CONFIG = {
    'input_formats': [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f%z',  # With timezone
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S.%fZ',  # UTC with Z
        '%Y-%m-%dT%H:%M:%SZ'
    ],
    'output_format': '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO 8601 UTC
    'timezone': 'UTC',
    'default_timezone': 'UTC'
}

# ============================================================================
# TEXT STANDARDIZATION
# ============================================================================

TEXT_STANDARDIZATION = {
    'normalize_whitespace': True,
    'remove_control_chars': True,
    'normalize_unicode': True,
    'max_message_length': 10000,
    'truncate_if_exceeds': True
}

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def get_schema_for_data_type(data_type: str) -> Dict[str, Any]:
    """
    Get the appropriate schema based on data type.
    
    Args:
        data_type: Type of data ('k8s_log' or 'hybrid_dataset')
    
    Returns:
        Schema dictionary
    """
    schemas = {
        'k8s_log': K8S_LOG_SCHEMA,
        'hybrid_dataset': HYBRID_DATASET_SCHEMA
    }
    return schemas.get(data_type, K8S_LOG_SCHEMA)

def validate_field(field_name: str, value: Any, schema: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate a single field against its schema definition.
    
    Args:
        field_name: Name of the field
        value: Value to validate
        schema: Schema definition for the field
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if field_name not in schema:
        return True, None  # Unknown field, skip validation
    
    field_schema = schema[field_name]
    
    # Check required fields
    if field_schema.get('required', False) and value is None:
        return False, f"Required field '{field_name}' is missing"
    
    # Check nullable
    if not field_schema.get('nullable', True) and (value is None or (isinstance(value, float) and pd.isna(value))):
        return False, f"Field '{field_name}' cannot be null"
    
    # Type checking
    expected_type = field_schema.get('type')
    if value is not None and not pd.isna(value):
        if expected_type == 'string' and not isinstance(value, str):
            return False, f"Field '{field_name}' must be a string"
        elif expected_type == 'integer' and not isinstance(value, (int, np.integer)):
            return False, f"Field '{field_name}' must be an integer"
        elif expected_type == 'float' and not isinstance(value, (float, np.floating, int, np.integer)):
            return False, f"Field '{field_name}' must be a float"
        elif expected_type == 'datetime' and not isinstance(value, (datetime, str)):
            return False, f"Field '{field_name}' must be a datetime or string"
    
    # Value range checking
    if expected_type in ['float', 'integer'] and value is not None:
        if 'min' in field_schema and value < field_schema['min']:
            return False, f"Field '{field_name}' value {value} is below minimum {field_schema['min']}"
        if 'max' in field_schema and value > field_schema['max']:
            return False, f"Field '{field_name}' value {value} is above maximum {field_schema['max']}"
    
    # Allowed values checking
    if 'allowed_values' in field_schema and value not in field_schema['allowed_values']:
        return False, f"Field '{field_name}' value '{value}' is not in allowed values {field_schema['allowed_values']}"
    
    return True, None

def save_schema_json(schema: Dict[str, Any], output_path: Path) -> None:
    """Save schema definition to JSON file."""
    # Convert schema to JSON-serializable format
    json_schema = {}
    for field, definition in schema.items():
        json_schema[field] = {
            k: v for k, v in definition.items() 
            if k not in ['pattern']  # Skip regex patterns for JSON
        }
    
    with open(output_path, 'w') as f:
        json.dump(json_schema, f, indent=2, default=str)

