package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

func TestBuildStorageConfig_S3(t *testing.T) {
	tests := []struct {
		name        string
		params      StorageParams
		wantErr     bool
		errContains string
	}{
		{
			name: "valid S3 with region",
			params: StorageParams{
				Type:   "s3",
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
			wantErr: false,
		},
		{
			name: "valid S3 with endpoint",
			params: StorageParams{
				Type:     "s3",
				Bucket:   "my-bucket",
				Endpoint: "http://localhost:9000",
			},
			wantErr: false,
		},
		{
			name: "valid S3 with credentials",
			params: StorageParams{
				Type:            "s3",
				Bucket:          "my-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantErr: false,
		},
		{
			name: "empty type defaults to S3",
			params: StorageParams{
				Type:   "",
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
			wantErr: false,
		},
		{
			name: "missing bucket",
			params: StorageParams{
				Type:   "s3",
				Region: "us-east-1",
			},
			wantErr:     true,
			errContains: "--bucket is required",
		},
		{
			name: "empty bucket",
			params: StorageParams{
				Type:   "s3",
				Bucket: "",
				Region: "us-east-1",
			},
			wantErr:     true,
			errContains: "--bucket is required",
		},
		{
			name: "S3 with only bucket (no region or endpoint)",
			params: StorageParams{
				Type:   "s3",
				Bucket: "my-bucket",
			},
			wantErr: false,
		},
		{
			name: "S3 bucket with whitespace only",
			params: StorageParams{
				Type:   "s3",
				Bucket: "   ",
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildStorageConfig(tc.params)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "s3", cfg.Type)
				assert.Equal(t, tc.params.Bucket, cfg.S3.Bucket)
			}
		})
	}
}

func TestBuildStorageConfig_R2(t *testing.T) {
	tests := []struct {
		name        string
		params      StorageParams
		wantErr     bool
		errContains string
	}{
		{
			name: "valid R2 with all params",
			params: StorageParams{
				Type:            "r2",
				AccountID:       "account123",
				Bucket:          "my-bucket",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			},
			wantErr: false,
		},
		{
			name: "missing Cloudflare account ID",
			params: StorageParams{
				Type:            "r2",
				Bucket:          "my-bucket",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			},
			wantErr:     true,
			errContains: "--account-id and --bucket are required",
		},
		{
			name: "missing bucket",
			params: StorageParams{
				Type:            "r2",
				AccountID:       "account123",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			},
			wantErr:     true,
			errContains: "--account-id and --bucket are required",
		},
		{
			name: "missing AWS access key ID",
			params: StorageParams{
				Type:            "r2",
				AccountID:       "account123",
				Bucket:          "my-bucket",
				SecretAccessKey: "secret-key",
			},
			wantErr:     true,
			errContains: "--access-key-id and --secret-access-key are required",
		},
		{
			name: "missing AWS secret access key",
			params: StorageParams{
				Type:        "r2",
				AccountID:   "account123",
				Bucket:      "my-bucket",
				AccessKeyID: "access-key",
			},
			wantErr:     true,
			errContains: "--access-key-id and --secret-access-key are required",
		},
		{
			name: "missing all credentials",
			params: StorageParams{
				Type:      "r2",
				AccountID: "account123",
				Bucket:    "my-bucket",
			},
			wantErr:     true,
			errContains: "--access-key-id and --secret-access-key are required",
		},
		{
			name: "missing Cloudflare account ID and bucket",
			params: StorageParams{
				Type:            "r2",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			},
			wantErr:     true,
			errContains: "--account-id and --bucket are required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildStorageConfig(tc.params)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "r2", cfg.Type)
				assert.Equal(t, tc.params.AccountID, cfg.R2.AccountID)
				assert.Equal(t, tc.params.Bucket, cfg.R2.Bucket)
			}
		})
	}
}

func TestBuildStorageConfig_Filesystem(t *testing.T) {
	tests := []struct {
		name        string
		params      StorageParams
		wantErr     bool
		errContains string
	}{
		{
			name: "valid filesystem with base path",
			params: StorageParams{
				Type:          "filesystem",
				LocalBasePath: "/data/iceberg",
			},
			wantErr: false,
		},
		{
			name: "valid filesystem with relative path",
			params: StorageParams{
				Type:          "filesystem",
				LocalBasePath: "./data",
			},
			wantErr: false,
		},
		{
			name: "missing local base path",
			params: StorageParams{
				Type: "filesystem",
			},
			wantErr:     true,
			errContains: "--local-base-path is required",
		},
		{
			name: "empty local base path",
			params: StorageParams{
				Type:          "filesystem",
				LocalBasePath: "",
			},
			wantErr:     true,
			errContains: "--local-base-path is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildStorageConfig(tc.params)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "filesystem", cfg.Type)
				assert.Equal(t, tc.params.LocalBasePath, cfg.Filesystem.BasePath)
			}
		})
	}
}

func TestBuildStorageConfig_UnknownType(t *testing.T) {
	tests := []struct {
		name        string
		storageType string
	}{
		{"unknown type", "unknown"},
		{"invalid type", "invalid"},
		{"gcs type (not supported)", "gcs"},
		{"azure type (not supported)", "azure"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := StorageParams{
				Type: tc.storageType,
			}

			_, err := BuildStorageConfig(params)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown storage type")
			assert.Contains(t, err.Error(), tc.storageType)
		})
	}
}

func TestBuildCatalogConfig_REST(t *testing.T) {
	tests := []struct {
		name        string
		params      CatalogParams
		wantErr     bool
		errContains string
	}{
		{
			name: "valid REST catalog with URI only",
			params: CatalogParams{
				Type:      iceberg.CatalogTypeRest,
				URI:       "http://catalog:8181",
				Namespace: "otel",
			},
			wantErr: false,
		},
		{
			name: "valid REST catalog with all params",
			params: CatalogParams{
				Type:      iceberg.CatalogTypeRest,
				URI:       "http://catalog:8181",
				Token:     "bearer-token-123",
				Warehouse: "s3://my-bucket/warehouse",
				Namespace: "otel",
			},
			wantErr: false,
		},
		{
			name: "missing catalog URI",
			params: CatalogParams{
				Type:      iceberg.CatalogTypeRest,
				Namespace: "otel",
			},
			wantErr:     true,
			errContains: "--catalog-uri is required",
		},
		{
			name: "empty catalog URI",
			params: CatalogParams{
				Type:      iceberg.CatalogTypeRest,
				URI:       "",
				Namespace: "otel",
			},
			wantErr:     true,
			errContains: "--catalog-uri is required",
		},
		{
			name: "REST with only URI",
			params: CatalogParams{
				Type: iceberg.CatalogTypeRest,
				URI:  "http://localhost:8181",
			},
			wantErr: false,
		},
		{
			name: "REST URI with whitespace only",
			params: CatalogParams{
				Type: iceberg.CatalogTypeRest,
				URI:  "   ",
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildCatalogConfig(tc.params)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, iceberg.CatalogTypeRest, cfg.Type)
				assert.Equal(t, tc.params.URI, cfg.REST.URI)
				assert.Equal(t, tc.params.Namespace, cfg.Namespace)
			}
		})
	}
}

func TestBuildCatalogConfig_None(t *testing.T) {
	tests := []struct {
		name   string
		params CatalogParams
	}{
		{
			name: "none catalog with namespace",
			params: CatalogParams{
				Type:      "none",
				Namespace: "otel",
			},
		},
		{
			name: "none catalog without namespace",
			params: CatalogParams{
				Type: "none",
			},
		},
		{
			name: "none catalog ignores URI",
			params: CatalogParams{
				Type:      "none",
				URI:       "http://ignored:8181",
				Namespace: "otel",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildCatalogConfig(tc.params)

			require.NoError(t, err)
			assert.Equal(t, "none", cfg.Type)
			assert.Equal(t, tc.params.Namespace, cfg.Namespace)
		})
	}
}

func TestBuildCatalogConfig_UnknownType(t *testing.T) {
	tests := []struct {
		name        string
		catalogType string
	}{
		{"unknown type", "unknown"},
		{"invalid type", "invalid"},
		{"hive type (not supported)", "hive"},
		{"glue type (not supported)", "glue"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := CatalogParams{
				Type:      tc.catalogType,
				URI:       "http://catalog:8181",
				Namespace: "otel",
			}

			_, err := BuildCatalogConfig(params)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown catalog type")
			assert.Contains(t, err.Error(), tc.catalogType)
		})
	}
}

func TestParseTimeFlag(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantNil     bool
		wantTime    time.Time
		wantErr     bool
		errContains string
	}{
		{
			name:    "empty string returns nil",
			input:   "",
			wantNil: true,
			wantErr: false,
		},
		{
			name:     "valid RFC3339 format",
			input:    "2024-01-15T10:30:00Z",
			wantNil:  false,
			wantTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "valid RFC3339 with timezone offset",
			input:    "2024-01-15T10:30:00+09:00",
			wantNil:  false,
			wantTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("+09:00", 9*60*60)),
			wantErr:  false,
		},
		{
			name:     "valid date-only format",
			input:    "2024-01-15",
			wantNil:  false,
			wantTime: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "date-only format start of year",
			input:    "2024-01-01",
			wantNil:  false,
			wantTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "date-only format end of year",
			input:    "2024-12-31",
			wantNil:  false,
			wantTime: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:        "invalid format - just year",
			input:       "2024",
			wantErr:     true,
			errContains: "invalid time format",
		},
		{
			name:        "invalid format - year-month",
			input:       "2024-01",
			wantErr:     true,
			errContains: "invalid time format",
		},
		{
			name:        "invalid format - random string",
			input:       "not-a-date",
			wantErr:     true,
			errContains: "invalid time format",
		},
		{
			name:        "invalid format - slash separator",
			input:       "2024/01/15",
			wantErr:     true,
			errContains: "invalid time format",
		},
		{
			name:        "invalid format - missing T in datetime",
			input:       "2024-01-15 10:30:00Z",
			wantErr:     true,
			errContains: "invalid time format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseTimeFlag(tc.input)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
				return
			}

			require.NoError(t, err)

			if tc.wantNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.True(t, result.Equal(tc.wantTime),
					"expected %v, got %v", tc.wantTime, *result)
			}
		})
	}
}
