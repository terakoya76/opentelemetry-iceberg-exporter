package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldUsePathStyleForEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     bool
	}{
		// AWS S3 - should use virtual-hosted-style
		{
			name:     "AWS S3 regional endpoint",
			endpoint: "https://s3.us-east-1.amazonaws.com",
			want:     false,
		},
		{
			name:     "AWS S3 global endpoint",
			endpoint: "https://s3.amazonaws.com",
			want:     false,
		},

		// Google Cloud Storage - should use virtual-hosted-style
		{
			name:     "GCS endpoint",
			endpoint: "https://storage.googleapis.com",
			want:     false,
		},

		// Localhost - should use path-style
		{
			name:     "localhost",
			endpoint: "http://localhost:9000",
			want:     true,
		},
		{
			name:     "localhost without port",
			endpoint: "http://localhost",
			want:     true,
		},
		{
			name:     "127.0.0.1",
			endpoint: "http://127.0.0.1:9000",
			want:     true,
		},
		{
			name:     "IPv6 loopback",
			endpoint: "http://[::1]:9000",
			want:     true,
		},

		// IP addresses - should use path-style
		{
			name:     "private IP address",
			endpoint: "http://192.168.1.100:9000",
			want:     true,
		},
		{
			name:     "public IP address",
			endpoint: "http://203.0.113.50:9000",
			want:     true,
		},

		// MinIO and S3-compatible services - should use path-style
		{
			name:     "MinIO hostname",
			endpoint: "http://minio:9000",
			want:     true,
		},
		{
			name:     "MinIO subdomain",
			endpoint: "http://minio.example.com:9000",
			want:     true,
		},
		{
			name:     "LocalStack",
			endpoint: "http://localstack:4566",
			want:     true,
		},
		{
			name:     "S3Mock",
			endpoint: "http://s3mock:9090",
			want:     true,
		},

		// Kubernetes-style hostnames - should use path-style
		{
			name:     "Kubernetes service",
			endpoint: "http://minio.minio.svc.cluster.local:9000",
			want:     true,
		},
		{
			name:     "Internal hostname",
			endpoint: "http://storage.internal:9000",
			want:     true,
		},

		// Custom domains - should use path-style (safer default)
		{
			name:     "custom domain",
			endpoint: "https://storage.mycompany.com",
			want:     true,
		},

		// Edge cases
		{
			name:     "invalid URL",
			endpoint: "not-a-valid-url",
			want:     true, // Default to path-style when we can't parse
		},
		{
			name:     "empty endpoint",
			endpoint: "",
			want:     true, // Empty parses but has no host
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldUsePathStyleForEndpoint(tt.endpoint)
			assert.Equal(t, tt.want, got, "endpoint: %s", tt.endpoint)
		})
	}
}

func TestS3FileIOConfig_ShouldUsePathStyle(t *testing.T) {
	tests := []struct {
		name string
		cfg  S3FileIOConfig
		want bool
	}{
		{
			name: "AWS S3 with region only (no endpoint)",
			cfg: S3FileIOConfig{
				Region:   "us-east-1",
				Endpoint: "",
			},
			want: false,
		},
		{
			name: "MinIO endpoint auto-detected",
			cfg: S3FileIOConfig{
				Endpoint: "http://minio:9000",
			},
			want: true,
		},
		{
			name: "localhost auto-detected",
			cfg: S3FileIOConfig{
				Endpoint: "http://localhost:9000",
			},
			want: true,
		},
		{
			name: "AWS endpoint auto-detected",
			cfg: S3FileIOConfig{
				Endpoint: "https://s3.us-west-2.amazonaws.com",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.ShouldUsePathStyle()
			assert.Equal(t, tt.want, got)
		})
	}
}
