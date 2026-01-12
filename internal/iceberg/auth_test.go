package iceberg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAWSCredentials_HasCredentials(t *testing.T) {
	tests := []struct {
		name  string
		creds AWSCredentials
		want  bool
	}{
		{
			name: "both access key and secret key set",
			creds: AWSCredentials{
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			want: true,
		},
		{
			name: "only access key set",
			creds: AWSCredentials{
				AccessKeyID: "AKIAIOSFODNN7EXAMPLE",
			},
			want: false,
		},
		{
			name: "only secret key set",
			creds: AWSCredentials{
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			want: false,
		},
		{
			name:  "neither set",
			creds: AWSCredentials{},
			want:  false,
		},
		{
			name: "empty strings",
			creds: AWSCredentials{
				AccessKeyID:     "",
				SecretAccessKey: "",
			},
			want: false,
		},
		{
			name: "with region but no credentials",
			creds: AWSCredentials{
				Region: "us-east-1",
			},
			want: false,
		},
		{
			name: "full credentials with region",
			creds: AWSCredentials{
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Region:          "us-west-2",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.creds.HasCredentials()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildAWSConfig(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		creds          AWSCredentials
		wantErr        bool
		wantRegion     string
		wantHasCreds   bool
		skipCredsCheck bool // Skip credential check for default chain tests
	}{
		{
			name: "with static credentials and region",
			creds: AWSCredentials{
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Region:          "us-west-2",
			},
			wantErr:      false,
			wantRegion:   "us-west-2",
			wantHasCreds: true,
		},
		{
			name: "with region only (uses default credential chain)",
			creds: AWSCredentials{
				Region: "eu-west-1",
			},
			wantErr:        false,
			wantRegion:     "eu-west-1",
			skipCredsCheck: true, // Default chain behavior varies by environment
		},
		{
			name:           "empty credentials (uses default credential chain)",
			creds:          AWSCredentials{},
			wantErr:        false,
			skipCredsCheck: true,
		},
		{
			name: "R2 auto region",
			creds: AWSCredentials{
				AccessKeyID:     "r2-access-key",
				SecretAccessKey: "r2-secret-key",
				Region:          "auto",
			},
			wantErr:      false,
			wantRegion:   "auto",
			wantHasCreds: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := BuildAWSConfig(ctx, tt.creds)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)

			// Verify region if expected
			if tt.wantRegion != "" {
				assert.Equal(t, tt.wantRegion, cfg.Region)
			}

			// Verify credentials are set (only when we explicitly provided them)
			if !tt.skipCredsCheck && tt.wantHasCreds {
				creds, err := cfg.Credentials.Retrieve(ctx)
				require.NoError(t, err)
				assert.Equal(t, tt.creds.AccessKeyID, creds.AccessKeyID)
				assert.Equal(t, tt.creds.SecretAccessKey, creds.SecretAccessKey)
			}
		})
	}
}

func TestBuildAWSConfigFromStorageConfig(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		storageCfg      FileIOConfig
		wantErr         bool
		wantErrContains string
		wantRegion      string
	}{
		{
			name: "S3 storage type with credentials",
			storageCfg: FileIOConfig{
				Type: "s3",
				S3: S3FileIOConfig{
					Region:          "us-east-1",
					Bucket:          "test-bucket",
					AccessKeyID:     "s3-access-key",
					SecretAccessKey: "s3-secret-key",
				},
			},
			wantErr:    false,
			wantRegion: "us-east-1",
		},
		{
			name: "empty type defaults to S3",
			storageCfg: FileIOConfig{
				Type: "",
				S3: S3FileIOConfig{
					Region: "ap-northeast-1",
					Bucket: "test-bucket",
				},
			},
			wantErr:    false,
			wantRegion: "ap-northeast-1",
		},
		{
			name: "R2 storage type with valid credentials",
			storageCfg: FileIOConfig{
				Type: "r2",
				R2: R2FileIOConfig{
					AccountID:       "account123",
					Bucket:          "r2-bucket",
					AccessKeyID:     "r2-access-key",
					SecretAccessKey: "r2-secret-key",
				},
			},
			wantErr:    false,
			wantRegion: "auto",
		},
		{
			name: "R2 storage type missing access key",
			storageCfg: FileIOConfig{
				Type: "r2",
				R2: R2FileIOConfig{
					AccountID:       "account123",
					Bucket:          "r2-bucket",
					AccessKeyID:     "",
					SecretAccessKey: "r2-secret-key",
				},
			},
			wantErr:         true,
			wantErrContains: "access_key_id",
		},
		{
			name: "R2 storage type missing secret key",
			storageCfg: FileIOConfig{
				Type: "r2",
				R2: R2FileIOConfig{
					AccountID:       "account123",
					Bucket:          "r2-bucket",
					AccessKeyID:     "r2-access-key",
					SecretAccessKey: "",
				},
			},
			wantErr:         true,
			wantErrContains: "secret_access_key",
		},
		{
			name: "R2 storage type missing both credentials",
			storageCfg: FileIOConfig{
				Type: "r2",
				R2: R2FileIOConfig{
					AccountID:       "account123",
					Bucket:          "r2-bucket",
					AccessKeyID:     "",
					SecretAccessKey: "",
				},
			},
			wantErr:         true,
			wantErrContains: "access_key_id",
		},
		{
			name: "filesystem storage type",
			storageCfg: FileIOConfig{
				Type: "filesystem",
				Filesystem: LocalFileIOConfig{
					BasePath: "/tmp/test",
				},
			},
			wantErr: false,
			// Filesystem returns a default config
		},
		{
			name: "unsupported storage type",
			storageCfg: FileIOConfig{
				Type: "unsupported",
			},
			wantErr:         true,
			wantErrContains: "unsupported storage type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := BuildAWSConfigFromStorageConfig(ctx, tt.storageCfg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)

			if tt.wantRegion != "" {
				assert.Equal(t, tt.wantRegion, cfg.Region)
			}
		})
	}
}
