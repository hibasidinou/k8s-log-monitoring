# Data Specification for Kubernetes Log Monitoring

## Overview

This document describes the data formats, schemas, and quality standards for the Kubernetes log monitoring system.

## Data Types

The system processes two main types of data:

1. **Kubernetes Log Entries** (`k8s_log`) - Raw log entries from Kubernetes clusters
2. **Hybrid Dataset** (`hybrid_dataset`) - Processed and merged datasets combining system and security logs

---

## Kubernetes Log Entry Schema

### Required Fields

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `timestamp` | datetime | ISO format timestamp of the log entry | Required, non-nullable, timezone-aware |
| `log_level` | string | Log severity level | Required, one of: INFO, WARNING, ERROR, DEBUG, CRITICAL |
| `message` | string | Log message content | Required, 1-10000 characters |
| `pod_name` | string | Kubernetes pod name | Required, RFC 1123 subdomain format |
| `namespace` | string | Kubernetes namespace | Required, valid DNS subdomain |
| `container_name` | string | Container name within the pod | Required, 1-253 characters |
| `node_name` | string | Kubernetes node name | Required, format: `node-{number}` |
| `service` | string | Service name associated with the pod | Required |
| `cpu_usage_percent` | float | CPU usage percentage | Required, range: 0.0-100.0 |
| `memory_usage_mb` | float | Memory usage in megabytes | Required, range: 0.0-100000.0 |
| `network_bytes` | integer | Network bytes transferred | Required, minimum: 0 |
| `is_anomaly` | integer | Binary flag for anomaly detection | Required, values: 0 or 1 |
| `anomaly_type` | string | Type of anomaly or normal operation | Required, one of: normal, cpu_spike, memory_leak, pod_crash, network_flood, disk_io_bottleneck, system_anomaly, security_attack |

### Field Details

#### Timestamp Format

- **Input Formats Supported:**
  - `%Y-%m-%dT%H:%M:%S.%f` (e.g., `2024-01-01T12:00:00.123456`)
  - `%Y-%m-%dT%H:%M:%S` (e.g., `2024-01-01T12:00:00`)
  - `%Y-%m-%d %H:%M:%S.%f` (e.g., `2024-01-01 12:00:00.123456`)
  - `%Y-%m-%d %H:%M:%S` (e.g., `2024-01-01 12:00:00`)
  - With timezone: `%Y-%m-%dT%H:%M:%S.%f%z` or `%Y-%m-%dT%H:%M:%S%z`
  - UTC with Z: `%Y-%m-%dT%H:%M:%S.%fZ` or `%Y-%m-%dT%H:%M:%SZ`

- **Output Format:** ISO 8601 UTC format: `%Y-%m-%dT%H:%M:%S.%fZ`
- **Timezone:** All timestamps are normalized to UTC

#### Log Levels

- `INFO`: Informational messages
- `WARNING`: Warning conditions
- `ERROR`: Error conditions
- `DEBUG`: Debug messages
- `CRITICAL`: Critical conditions

#### Pod Name Format

Kubernetes pod names must follow RFC 1123 subdomain format:
- Lowercase alphanumeric characters or hyphens
- Must start and end with alphanumeric character
- Can contain dots for subdomains
- Example: `frontend-pod-123`, `backend.pod.456`

#### Namespace Format

Kubernetes namespace names must be valid DNS subdomains:
- Lowercase alphanumeric characters or hyphens
- Must start and end with alphanumeric character
- Maximum length: 253 characters
- Common namespaces: `default`, `production`, `staging`, `development`, `monitoring`, `kube-system`

#### Anomaly Types

- `normal`: Normal operation
- `cpu_spike`: CPU usage spike detected
- `memory_leak`: Memory leak detected
- `pod_crash`: Pod crash event
- `network_flood`: Network flooding detected
- `disk_io_bottleneck`: Disk I/O bottleneck
- `system_anomaly`: General system anomaly
- `security_attack`: Security attack detected

---

## Hybrid Dataset Schema

The hybrid dataset combines system and security logs with the following schema:

| Field | Type | Required | Nullable | Description |
|-------|------|----------|----------|-------------|
| `cpu_usage` | float | Yes | Yes | CPU usage percentage (0-100) |
| `memory_usage` | float | Yes | Yes | Memory usage in MB (0-100000) |
| `label` | integer | Yes | No | Binary label: 0=normal, 1=anomaly |
| `anomaly_type` | string | Yes | No | Type of anomaly |
| `packets_count` | float | No | Yes | Network packets count |
| `flow_bytes_per_second` | float | No | Yes | Network flow bytes per second |
| `flow_packets_per_second` | float | No | Yes | Network flow packets per second |
| `container_cpu_usage_seconds_rate` | float | No | Yes | Container CPU usage rate |
| `container_memory_usage_bytes` | float | No | Yes | Container memory usage in bytes |
| `anomaly_type_encoded` | integer | No | Yes | Encoded anomaly type |

---

## Data Quality Standards

### Missing Values

- **Critical Fields:** Maximum 5% missing values allowed
  - Critical fields: `timestamp`, `log_level`, `message`, `pod_name`, `namespace`
  - Rows with missing critical fields are removed

- **Non-Critical Fields:**
  - Numeric fields: Filled with median value
  - String fields: Filled with mode value or 'unknown'
  - Non-nullable fields: Rows are removed

### Outlier Detection

Outliers are detected using the Interquartile Range (IQR) method:
- **Method:** IQR with 3.0x multiplier
- **Strategy:** Outliers are capped at the bounds (not removed)
- **Fields Monitored:**
  - `cpu_usage_percent`: Range 0.0-100.0
  - `memory_usage_mb`: Range 0.0-100000.0
  - `network_bytes`: Minimum 0

### Duplicate Detection

- **Key Fields:** `timestamp`, `pod_name`, `message`
- **Time Window:** 1 second (logs within 1 second with same pod and message are considered duplicates)
- **Action:** First occurrence is kept, subsequent duplicates are removed

### Text Standardization

- **Whitespace:** Multiple spaces normalized to single space
- **Control Characters:** Removed (except newline and tab)
- **Unicode:** Normalized using NFKD normalization
- **Length:** Maximum 10,000 characters (truncated if exceeds)

### Temporal Ordering

- All logs are sorted by timestamp in ascending order
- Ensures proper temporal sequence for time-series analysis

---

## Data Validation Rules

### Schema Validation

1. **Required Fields:** All required fields must be present
2. **Data Types:** Fields must match expected data types
3. **Value Ranges:** Numeric fields must be within specified ranges
4. **Allowed Values:** Enum fields must match allowed values
5. **Format Validation:** String fields must match format patterns (e.g., pod names, namespaces)

### Kubernetes-Specific Validation

- Pod names validated against RFC 1123 subdomain format
- Namespace names validated against DNS subdomain format
- Node names validated against pattern `node-{number}`

---

## Output Files

### Cleaned Data

- **Location:** `data/processed/cleaned/cleaned_logs.parquet`
- **Format:** Parquet (optimized for Spark processing)
- **Schema:** Validated against schema definition

### Schema Definition

- **Location:** `data/processed/cleaned/schema.json`
- **Format:** JSON
- **Content:** Complete schema definition with field types and constraints

### Quality Reports

- **Missing Values Report:** `data/processed/output/statistics/missing_values_report.json`
  - Detailed statistics on missing values per column
  - Missing percentages and counts

- **Data Quality Report:** `data/processed/output/statistics/data_quality_report.json`
  - Comprehensive quality metrics
  - Preprocessing summary
  - Validation errors
  - Numeric and text statistics

- **Data Profile:** `data/processed/output/statistics/data_profile.html`
  - Interactive HTML report with data profiling
  - Statistics, distributions, and sample data

---

## Data Processing Pipeline

### Steps

1. **Load Data:** Read from CSV or Parquet file
2. **Handle Missing Values:** Remove or fill missing values according to rules
3. **Standardize Timestamps:** Convert all timestamps to consistent UTC format
4. **Standardize Text:** Normalize whitespace, remove control chars, normalize unicode
5. **Remove Duplicates:** Identify and remove duplicate log entries
6. **Detect and Handle Outliers:** Identify outliers and cap them at bounds
7. **Ensure Temporal Ordering:** Sort by timestamp
8. **Validate Schema:** Validate against schema definition
9. **Generate Reports:** Create quality reports and data profile

### Usage

```python
from src.data_collection.data_preprocessor import DataPreprocessor
from pathlib import Path

# Initialize preprocessor
preprocessor = DataPreprocessor(data_type='k8s_log')

# Preprocess data
input_path = Path("data/raw/logs.csv")
output_path = Path("data/processed/cleaned/cleaned_logs.parquet")
cleaned_df = preprocessor.preprocess(input_path, output_path)

# Generate quality reports
preprocessor.generate_quality_report(cleaned_df)
preprocessor.generate_data_profile(cleaned_df)
```

### Command Line

```bash
python -m src.data_collection.data_preprocessor \
    data/raw/logs.csv \
    --output data/processed/cleaned/cleaned_logs.parquet \
    --data-type k8s_log
```

---

## Quality Metrics

### Metrics Tracked

- **Total Rows:** Initial and final row counts
- **Rows Removed:** Count of rows removed during cleaning
- **Missing Values:** Per-column missing value statistics
- **Outliers Detected:** Count and percentage of outliers per field
- **Duplicates Removed:** Count of duplicate rows removed
- **Text Standardized:** Count of text fields standardized
- **Timestamps Standardized:** Count of timestamps standardized
- **Validation Errors:** List of schema validation errors

### Reporting

All quality metrics are saved to JSON files for programmatic access and HTML reports for human review.

---

## Best Practices

1. **Always validate data** against schema before processing
2. **Monitor quality metrics** to detect data quality issues early
3. **Review validation errors** to understand data issues
4. **Use Parquet format** for processed data (better compression and Spark compatibility)
5. **Keep raw data** separate from processed data
6. **Document data transformations** in quality reports

---

## Version History

- **v1.0** (2024-01-01): Initial specification
  - Kubernetes log entry schema
  - Hybrid dataset schema
  - Data quality standards
  - Processing pipeline documentation

