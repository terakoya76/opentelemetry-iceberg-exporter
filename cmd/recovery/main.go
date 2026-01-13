package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/recovery"
)

// StorageParams holds storage configuration parameters.
type StorageParams struct {
	Type            string
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	AccountID       string
	LocalBasePath   string
}

// CatalogParams holds catalog configuration parameters.
type CatalogParams struct {
	Type      string
	URI       string
	Token     string
	Warehouse string
	Namespace string
}

// RecoveryParams holds recovery operation parameters.
type RecoveryParams struct {
	Tables  []string
	DryRun  bool
	Verbose bool
}

var (
	// Storage flags
	storageType     string
	bucketName      string
	awsRegion       string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	accountID       string
	localBasePath   string

	// Catalog flags
	catalogType      string
	catalogURI       string
	catalogToken     string
	catalogWarehouse string
	namespace        string

	// Recovery flags
	tables     []string
	dryRun     bool
	verbose    bool
	afterTime  string
	beforeTime string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "iceberg-recovery",
		Short: "Recover orphaned parquet files by registering them with the Iceberg catalog",
		Long: `Iceberg Recovery Tool

This tool scans storage for parquet files that were written by the OpenTelemetry
Iceberg Exporter but not registered with the Iceberg catalog (due to failures).

It will attempt to register these orphaned files with the catalog so they become
queryable via Iceberg-aware tools.

Examples:
  # Dry run - see what would be recovered (S3)
  iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel --dry-run

  # Recover all tables (S3)
  iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel

  # Recover with R2 storage
  iceberg-recovery --storage-type=r2 --account-id=account123 --bucket=my-bucket \
    --access-key-id=KEY --secret-access-key=SECRET \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel

  # Recover specific table (filesystem)
  iceberg-recovery --storage-type=filesystem --local-base-path=/data \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel --tables=otel_traces

  # Recover files from a specific date range (RFC3339)
  iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel --after=2024-01-15T00:00:00Z --before=2024-01-16T00:00:00Z

  # Recover files from the last week (date-only format)
  iceberg-recovery --storage-type=s3 --bucket=my-bucket --aws-region=us-east-1 \
    --catalog-type=rest --catalog-uri=http://catalog:8181 \
    --namespace=otel --after=2024-01-08 --before=2024-01-15`,
		RunE: runRecovery,
	}

	// Storage flags
	rootCmd.Flags().StringVar(&storageType, "storage-type", "s3", "Storage type: s3, r2, filesystem")
	rootCmd.Flags().StringVar(&bucketName, "bucket", "", "Bucket name (for S3 or R2)")
	rootCmd.Flags().StringVar(&awsRegion, "aws-region", "", "AWS region (for S3)")
	rootCmd.Flags().StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint URL (for MinIO, etc.)")
	rootCmd.Flags().StringVar(&accessKeyID, "access-key-id", "", "Access key ID (for S3 or R2)")
	rootCmd.Flags().StringVar(&secretAccessKey, "secret-access-key", "", "Secret access key (for S3 or R2)")
	rootCmd.Flags().StringVar(&accountID, "account-id", "", "Account ID (for R2)")
	rootCmd.Flags().StringVar(&localBasePath, "local-base-path", "", "Local filesystem base path")

	// Catalog flags
	rootCmd.Flags().StringVar(&catalogType, "catalog-type", "rest", "Catalog type: rest, none")
	rootCmd.Flags().StringVar(&catalogURI, "catalog-uri", "", "REST catalog URI")
	rootCmd.Flags().StringVar(&catalogToken, "catalog-token", "", "Catalog bearer token")
	rootCmd.Flags().StringVar(&catalogWarehouse, "catalog-warehouse", "", "Catalog warehouse location")
	rootCmd.Flags().StringVar(&namespace, "namespace", "otel", "Iceberg namespace")

	// Recovery flags
	rootCmd.Flags().StringSliceVar(&tables, "tables", nil, "Specific tables to recover (default: all)")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Only show what would be recovered")
	rootCmd.Flags().BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	rootCmd.Flags().StringVar(&afterTime, "after", "", "Filter files with partition time >= this (RFC3339 or YYYY-MM-DD)")
	rootCmd.Flags().StringVar(&beforeTime, "before", "", "Filter files with partition time < this (RFC3339 or YYYY-MM-DD)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runRecovery(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt signal, canceling...")
		cancel()
	}()

	// Create logger
	logLevel := zapcore.InfoLevel
	if verbose {
		logLevel = zapcore.DebugLevel
	}
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(logLevel)
	zapConfig.Encoding = "console"
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapLogger, err := zapConfig.Build()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer func() { _ = zapLogger.Sync() }()

	verbosity := configtelemetry.LevelNormal
	if verbose {
		verbosity = configtelemetry.LevelDetailed
	}
	vlogger := logger.New(zapLogger, verbosity)

	// Build storage config
	storageParams := StorageParams{
		Type:            storageType,
		Bucket:          bucketName,
		Region:          awsRegion,
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		AccountID:       accountID,
		LocalBasePath:   localBasePath,
	}
	storageCfg, err := BuildStorageConfig(storageParams)
	if err != nil {
		return fmt.Errorf("storage configuration error: %w", err)
	}

	// Create FileIO
	fileIO, err := iceberg.NewFileIO(ctx, storageCfg)
	if err != nil {
		return fmt.Errorf("failed to create FileIO: %w", err)
	}
	defer func() { _ = fileIO.Close() }()

	// Build catalog config
	catalogParams := CatalogParams{
		Type:      catalogType,
		URI:       catalogURI,
		Token:     catalogToken,
		Warehouse: catalogWarehouse,
		Namespace: namespace,
	}
	catalogCfg, err := BuildCatalogConfig(catalogParams)
	if err != nil {
		return fmt.Errorf("catalog configuration error: %w", err)
	}

	// Create catalog
	catalog, err := iceberg.NewCatalog(ctx, catalogCfg, storageCfg, vlogger)
	if err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}
	defer func() { _ = catalog.Close() }()

	// Create reconciler
	reconciler, err := recovery.NewReconciler(fileIO, catalog, namespace, vlogger)
	if err != nil {
		return fmt.Errorf("failed to create reconciler: %w", err)
	}

	// Parse time flags
	after, err := parseTimeFlag(afterTime)
	if err != nil {
		return fmt.Errorf("invalid --after flag: %w", err)
	}
	before, err := parseTimeFlag(beforeTime)
	if err != nil {
		return fmt.Errorf("invalid --before flag: %w", err)
	}

	// Build recovery options
	opts := recovery.RecoveryOptions{
		Namespace: namespace,
		DryRun:    dryRun,
		ScanOptions: recovery.ScanOptions{
			Tables: tables,
			After:  after,
			Before: before,
		},
	}

	// Run recovery
	fmt.Println("Starting recovery process...")
	if dryRun {
		fmt.Println("DRY-RUN MODE: No changes will be made")
	}
	fmt.Println()

	result, err := reconciler.Recover(ctx, opts)
	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	// Print results
	printResults(result, dryRun)

	return nil
}

// BuildStorageConfig builds a FileIOConfig from the given parameters.
// Exported for testing.
func BuildStorageConfig(params StorageParams) (iceberg.FileIOConfig, error) {
	cfg := iceberg.FileIOConfig{
		Type: params.Type,
	}

	switch params.Type {
	case "s3", "":
		if params.Bucket == "" {
			return cfg, fmt.Errorf("--bucket is required for S3 storage")
		}
		cfg.Type = "s3"
		cfg.S3 = iceberg.S3FileIOConfig{
			Bucket:          params.Bucket,
			Region:          params.Region,
			Endpoint:        params.Endpoint,
			AccessKeyID:     params.AccessKeyID,
			SecretAccessKey: params.SecretAccessKey,
		}

	case "r2":
		if params.AccountID == "" || params.Bucket == "" {
			return cfg, fmt.Errorf("--account-id and --bucket are required for R2 storage")
		}
		if params.AccessKeyID == "" || params.SecretAccessKey == "" {
			return cfg, fmt.Errorf("--access-key-id and --secret-access-key are required for R2 storage")
		}
		cfg.R2 = iceberg.R2FileIOConfig{
			AccountID:       params.AccountID,
			Bucket:          params.Bucket,
			AccessKeyID:     params.AccessKeyID,
			SecretAccessKey: params.SecretAccessKey,
		}

	case "filesystem":
		if params.LocalBasePath == "" {
			return cfg, fmt.Errorf("--local-base-path is required for filesystem storage")
		}
		cfg.Filesystem = iceberg.LocalFileIOConfig{
			BasePath: params.LocalBasePath,
		}

	default:
		return cfg, fmt.Errorf("unknown storage type: %s", params.Type)
	}

	return cfg, nil
}

// BuildCatalogConfig builds a CatalogConfig from the given parameters.
// Exported for testing.
func BuildCatalogConfig(params CatalogParams) (iceberg.CatalogConfig, error) {
	cfg := iceberg.CatalogConfig{
		Type:      params.Type,
		Namespace: params.Namespace,
	}

	switch params.Type {
	case "rest":
		if params.URI == "" {
			return cfg, fmt.Errorf("--catalog-uri is required for REST catalog")
		}
		cfg.REST = iceberg.RESTCatalogConfig{
			URI:       params.URI,
			Warehouse: params.Warehouse,
			Token:     params.Token,
		}

	case "none":
		// No additional config needed

	default:
		return cfg, fmt.Errorf("unknown catalog type: %s", params.Type)
	}

	return cfg, nil
}

// parseTimeFlag parses a time string flag into a *time.Time.
// Accepts RFC3339 format (e.g., "2024-01-15T00:00:00Z") or date-only format (e.g., "2024-01-15").
// Returns nil if the input is empty.
// Exported for testing.
func parseTimeFlag(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}

	// Try RFC3339 first
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return &t, nil
	}

	// Try date-only format (interpreted as 00:00:00 UTC)
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return &t, nil
	}

	return nil, fmt.Errorf("invalid time format %q: expected RFC3339 (e.g., 2024-01-15T00:00:00Z) or date (e.g., 2024-01-15)", value)
}

func printResults(result *recovery.RecoveryResult, isDryRun bool) {
	fmt.Println("=== Recovery Results ===")
	fmt.Printf("Total files found:    %d\n", result.TotalFiles)

	if isDryRun {
		fmt.Printf("Files to recover:     %d\n", result.TotalFiles)
		fmt.Println()
		fmt.Println("Run without --dry-run to perform actual recovery.")
	} else {
		fmt.Printf("Successfully registered: %d\n", result.SuccessCount)
		fmt.Printf("Already registered:      %d\n", result.SkippedCount)
		fmt.Printf("Failed:                  %d\n", result.FailureCount)

		if len(result.Errors) > 0 {
			fmt.Println()
			fmt.Println("=== Errors ===")
			for _, e := range result.Errors {
				fmt.Printf("  %s: %s\n", e.File.Path, e.Error)
			}
		}

		if len(result.RegisteredFiles) > 0 {
			fmt.Println()
			fmt.Println("=== Registered Files ===")
			for _, f := range result.RegisteredFiles {
				fmt.Printf("  %s (%s, %d bytes)\n", f.Path, f.TableName, f.Size)
			}
		}
	}

	// Group by table for summary
	if result.TotalFiles > 0 {
		fmt.Println()
		fmt.Println("=== By Table ===")
		tableCount := make(map[string]int)
		for _, f := range result.RegisteredFiles {
			tableCount[f.TableName]++
		}
		// Include files that would be processed in dry-run mode
		if isDryRun {
			fmt.Println("(Tables with files to recover)")
		}
		for table, count := range tableCount {
			if table == "" {
				table = "(unknown)"
			}
			fmt.Printf("  %s: %d files\n", table, count)
		}
	}

	fmt.Println()
	fmt.Println("Recovery process complete.")
}
