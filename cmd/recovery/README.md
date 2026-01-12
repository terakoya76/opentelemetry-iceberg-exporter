# Iceberg Recovery Tool

A CLI tool to recover orphaned parquet files by registering them with the Iceberg catalog.

## Overview

When the OpenTelemetry Iceberg Exporter writes telemetry data, it follows a best-effort pattern:
1. Data is written to storage first (ensuring durability)
2. Catalog registration is attempted afterward

If the exporter fails or restarts between these steps, parquet files may exist in storage but not be registered with the Iceberg catalog. These "orphaned" files are valid data but invisible to Iceberg-aware query tools.

This recovery tool scans storage for such orphaned files and registers them with the catalog, making the data queryable again.

## Building

```bash
# From the project root
go build -o iceberg-recovery ./cmd/recovery
```

## Quick Start

```bash
# 1. Always start with a dry-run to see what would be recovered
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel --dry-run

# 2. If the dry-run looks good, run without --dry-run to perform recovery
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel
```

## Command-Line Reference

### Storage Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--storage-type` | string | `s3` | Storage backend type: `s3`, `r2`, `filesystem` |
| `--bucket` | string | - | Bucket name (required for S3 and R2) |
| `--aws-region` | string | - | AWS region (for S3) |
| `--endpoint` | string | - | S3-compatible endpoint URL (for MinIO, LocalStack, etc.) |
| `--access-key-id` | string | - | Access key ID (for S3 or R2). Uses default credential chain if empty |
| `--secret-access-key` | string | - | Secret access key (for S3 or R2). Uses default credential chain if empty |
| `--account-id` | string | - | Account ID (required for R2) |
| `--local-base-path` | string | - | Local filesystem base path (required for filesystem) |

### Catalog Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--catalog-type` | string | `rest` | Catalog type: `rest`, `none` |
| `--catalog-uri` | string | - | REST catalog URI (required for REST catalog) |
| `--catalog-token` | string | - | Bearer token for catalog authentication |
| `--catalog-warehouse` | string | - | Catalog warehouse location |
| `--namespace` | string | `otel` | Iceberg namespace (database) |

### Recovery Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--tables` | string[] | (all) | Specific tables to recover (comma-separated). If not set, all tables are scanned |
| `--dry-run` | bool | `false` | Only show what would be recovered without making changes |
| `--after` | string | - | Filter files with partition time >= this (RFC3339 or YYYY-MM-DD) |
| `--before` | string | - | Filter files with partition time < this (RFC3339 or YYYY-MM-DD) |
| `--verbose` | bool | `false` | Enable verbose/debug logging |

## Usage Examples

### AWS S3

```bash
# Using default credential chain (recommended)
./iceberg-recovery --storage-type=s3 --bucket=my-otel-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel

# With explicit credentials
./iceberg-recovery --storage-type=s3 --bucket=my-otel-bucket --aws-region=us-east-1 \
  --access-key-id=AKIAIOSFODNN7EXAMPLE \
  --secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel
```

### S3-Compatible Storage (MinIO, LocalStack)

```bash
./iceberg-recovery --storage-type=s3 --bucket=my-bucket \
  --endpoint=http://minio:9000 \
  --access-key-id=minioadmin \
  --secret-access-key=minioadmin \
  --catalog-type=rest --catalog-uri=http://nessie:19120/iceberg \
  --namespace=otel
```

### Cloudflare R2

```bash
./iceberg-recovery --storage-type=r2 --bucket=my-bucket \
  --account-id=your-account-id \
  --access-key-id=your-r2-access-key \
  --secret-access-key=your-r2-secret-key \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --catalog-warehouse=my-warehouse \
  --catalog-token=my-token \
  --namespace=otel
```

### Local Filesystem

```bash
./iceberg-recovery --storage-type=filesystem --local-base-path=/data/iceberg \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel
```

### Recover Specific Tables Only

```bash
# Recover only traces and logs, skip metrics tables
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel \
  --tables=otel_traces,otel_logs
```

### With Authenticated Catalog

```bash
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=https://catalog.example.com/iceberg \
  --catalog-token=your-bearer-token \
  --catalog-warehouse=s3://my-bucket/warehouse \
  --namespace=otel
```

### Filter by Time Range

```bash
# Recover files from a specific date range (RFC3339 format)
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel \
  --after=2024-01-15T00:00:00Z --before=2024-01-16T00:00:00Z

# Recover files from the last week (date-only format)
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel \
  --after=2024-01-08 --before=2024-01-15
```

### Verbose Mode for Debugging

```bash
./iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
  --catalog-type=rest --catalog-uri=http://catalog:8181 \
  --namespace=otel \
  --verbose
```

## Output

The tool outputs progress and results to stdout:

```
Starting recovery process...

=== Recovery Results ===
Total files found:    42
Successfully registered: 38
Already registered:      3
Failed:                  1

=== Errors ===
  data/otel_traces/year=2024/month=01/day=15/hour=10/abc123.parquet: schema mismatch

=== Registered Files ===
  data/otel_traces/year=2024/month=01/day=15/hour=10/def456.parquet (otel_traces, 1048576 bytes)
  ...

=== By Table ===
  otel_traces: 20 files
  otel_logs: 15 files
  otel_metrics_gauge: 3 files

Recovery process complete.
```

### Dry-Run Output

When using `--dry-run`, no changes are made and the output shows what would be recovered:

```
Starting recovery process...
DRY-RUN MODE: No changes will be made

=== Recovery Results ===
Total files found:    42
Files to recover:     42

Run without --dry-run to perform actual recovery.

=== By Table ===
(Tables with files to recover)
  otel_traces: 20 files
  otel_logs: 15 files
  ...

Recovery process complete.
```

## Signal Handling

The tool handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully. When interrupted, it will stop processing and exit cleanly.

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success (or dry-run completed) |
| 1 | Error (configuration error, connection failure, etc.) |

## Best Practices

1. **Always dry-run first**: Use `--dry-run` to preview what will be recovered before making changes.

2. **Start with verbose mode**: When troubleshooting, use `--verbose` to see detailed logging.

3. **Use time filters for large backlogs**: If you have many orphaned files, use `--after` and `--before` to process specific time ranges.

4. **Filter by table when possible**: If you know which tables need recovery, use `--tables` to speed up the scan.

5. **Schedule periodic recovery**: Consider running this tool periodically (e.g., via cron) to automatically recover any orphaned files.

## Required Flags by Storage Type

### S3

- `--bucket` (required)
- `--aws-region` or `--endpoint` (at least one recommended)

### R2

- `--bucket` (required)
- `--account-id` (required)
- `--access-key-id` (required)
- `--secret-access-key` (required)

### Filesystem

- `--local-base-path` (required)

## Required Flags by Catalog Type

### REST

- `--catalog-uri` (required)

### None

No additional flags required. Note: Using `--catalog-type=none` means the tool cannot register files with a catalog.

## See Also

- [Main README](../../README.md) - Full project documentation
- [OpenTelemetry Iceberg Exporter](../../README.md#configuration) - Configuration reference for the exporter
