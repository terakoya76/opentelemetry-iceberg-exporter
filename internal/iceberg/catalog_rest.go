package iceberg

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// RESTCatalog implements Catalog using the Iceberg REST catalog API via iceberg-go.
type RESTCatalog struct {
	catalog   catalog.Catalog
	warehouse string
	logger    *logger.VerboseLogger

	// awsConfig is the AWS configuration for S3 file access.
	// This is injected into context to bypass iceberg-go's S3 property parsing,
	// which doesn't support properties like s3.signer.uri returned by some REST catalogs.
	awsConfig *aws.Config

	// Cache for tracking initialized tables
	tableCache sync.Map // map[string]bool
}

// NewRESTCatalog creates a new REST catalog using iceberg-go.
func NewRESTCatalog(ctx context.Context, cfg RESTCatalogConfig, storageCfg FileIOConfig, vlogger *logger.VerboseLogger) (*RESTCatalog, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required for REST catalog initialization")
	}

	if cfg.URI == "" {
		return nil, fmt.Errorf("rest catalog URI is required (catalog.rest.uri)")
	}

	vlogger.Info("connecting to REST catalog",
		zap.String("uri", cfg.URI),
		zap.String("warehouse", cfg.Warehouse),
		zap.Bool("token_configured", cfg.Token != ""))

	awsCfg, err := buildAWSConfigFromStorage(ctx, storageCfg, vlogger)
	if err != nil {
		vlogger.Warn("failed to build AWS config from storage config, catalog operations may fail",
			zap.Error(err))
	}

	var opts []rest.Option

	// Add logging transport to debug HTTP requests (only when verbosity is Detailed)
	if vlogger.IsDetailed() {
		opts = append(opts, rest.WithCustomTransport(&loggingTransport{
			wrapped: http.DefaultTransport,
			logger:  vlogger,
		}))
	}

	// Bearer token authentication (e.g., Cloudflare API token for R2 Data Catalog)
	if cfg.Token != "" {
		opts = append(opts, rest.WithOAuthToken(cfg.Token))
	} else {
		// No authentication configured - this may cause "Anonymous" access errors
		vlogger.Warn("no authentication configured for REST catalog - table operations will fail with 'Anonymous' error",
			zap.String("uri", cfg.URI))
	}

	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}

	if awsCfg != nil {
		opts = append(opts, rest.WithAwsConfig(*awsCfg))
	}

	cat, err := rest.NewCatalog(ctx, "rest", cfg.URI, opts...)
	if err != nil {
		errMsg := normalizeIcebergError(err)

		if cfg.Token == "" {
			errMsg += " (no authentication configured - try providing --catalog-token)"
		}
		return nil, fmt.Errorf("failed to initialize rest catalog: %s", errMsg)
	}

	restCatalog := &RESTCatalog{
		catalog:   cat,
		warehouse: cfg.Warehouse,
		logger:    vlogger,
		awsConfig: awsCfg,
	}

	// Perform connection test (best-effort, does not fail catalog creation)
	restCatalog.testConnection(ctx, cfg.URI, cfg.Warehouse)

	return restCatalog, nil
}

// testConnection performs a connection test by listing namespaces.
// This is a best-effort operation that logs success or failure but does not
// affect catalog initialization.
func (c *RESTCatalog) testConnection(ctx context.Context, uri, warehouse string) {
	// Use ListNamespaces as a lightweight connection test
	namespaces, err := c.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		c.logger.Warn("REST catalog connection test failed (catalog operations may fail later)",
			zap.String("uri", uri),
			zap.Error(err))
		return
	}

	c.logger.Info("successfully connected to REST catalog",
		zap.String("uri", uri),
		zap.String("warehouse", warehouse),
		zap.Int("namespaces_found", len(namespaces)))
}

// EnsureNamespace implements Catalog.EnsureNamespace.
func (c *RESTCatalog) EnsureNamespace(ctx context.Context, namespace string) error {
	ident := catalog.ToIdentifier(namespace)

	_, err := c.catalog.LoadNamespaceProperties(ctx, ident)
	if err == nil {
		return nil // Namespace exists
	}

	props := map[string]string{}
	// Only set location if warehouse is a full path (has a URL scheme like s3://)
	// If warehouse is just a name (e.g., for Nessie), let the server resolve the location
	if c.warehouse != "" && strings.Contains(c.warehouse, "://") {
		props["location"] = fmt.Sprintf("%s/%s", c.warehouse, namespace)
	}

	err = c.catalog.CreateNamespace(ctx, ident, props)
	if err != nil {
		// Ignore "already exists" error (race condition)
		// Check if it exists now
		_, checkErr := c.catalog.LoadNamespaceProperties(ctx, ident)
		if checkErr == nil {
			return nil
		}

		errMsg := normalizeIcebergError(err)
		c.logger.Error("failed to create namespace",
			zap.String("namespace", namespace),
			zap.String("error_type", fmt.Sprintf("%T", err)),
			zap.String("error_message", errMsg),
			zap.Error(checkErr))

		return fmt.Errorf("failed to create namespace %s: %s", namespace, errMsg)
	}

	c.logger.Info("created namespace", zap.String("namespace", namespace))
	return nil
}

// EnsureTable implements Catalog.EnsureTable.
func (c *RESTCatalog) EnsureTable(ctx context.Context, namespace, tableName string, schema *arrow.Schema, partitionSpec PartitionSpec) error {
	cacheKey := fmt.Sprintf("%s.%s", namespace, tableName)

	if _, ok := c.tableCache.Load(cacheKey); ok {
		return nil // Table exists
	}

	tableIdent := catalog.ToIdentifier(namespace, tableName)

	// First, try to load the table to see if it already exists
	c.logger.Debug("checking if table exists",
		zap.String("namespace", namespace),
		zap.String("table", tableName))

	_, loadErr := c.catalog.LoadTable(ctx, tableIdent)
	if loadErr == nil {
		c.logger.Debug("table already exists",
			zap.String("namespace", namespace),
			zap.String("table", tableName))
		c.tableCache.Store(cacheKey, true)
		return nil
	}

	c.logger.Debug("table does not exist or cannot be loaded, will attempt to create",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.String("load_error_type", fmt.Sprintf("%T", loadErr)),
		zap.String("load_error", loadErr.Error()))

	// Convert Arrow schema to Iceberg schema
	icebergSchema, err := arrowSchemaToIcebergSchema(schema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	icebergPartitionSpec := buildIcebergPartitionSpec(partitionSpec, icebergSchema)

	c.logger.Debug("attempting to create table",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int("schema_fields", len(icebergSchema.Fields())),
		zap.Int("partition_fields", len(partitionSpec.Fields)))

	_, err = c.catalog.CreateTable(ctx, tableIdent, icebergSchema,
		catalog.WithPartitionSpec(&icebergPartitionSpec))
	if err != nil {
		// Ignore "already exists" error (race condition)
		_, checkErr := c.catalog.LoadTable(ctx, tableIdent)
		if checkErr == nil {
			c.tableCache.Store(cacheKey, true)
			return nil
		}

		errMsg := normalizeIcebergError(err)
		c.logger.Error("failed to create table",
			zap.String("namespace", namespace),
			zap.String("table", tableName),
			zap.Int("schema_fields", len(icebergSchema.Fields())),
			zap.Int("partition_fields", len(partitionSpec.Fields)),
			zap.String("error_type", fmt.Sprintf("%T", err)),
			zap.String("error_message", errMsg),
			zap.Error(checkErr))

		return fmt.Errorf("failed to create table %s.%s: %s (verify REST catalog is running and accessible)", namespace, tableName, errMsg)
	}

	c.logger.Info("created table",
		zap.String("namespace", namespace),
		zap.String("table", tableName))

	c.tableCache.Store(cacheKey, true)
	return nil
}

// AppendRecords implements Catalog.AppendRecords.
func (c *RESTCatalog) AppendRecords(ctx context.Context, namespace, tableName string, record arrow.RecordBatch, props iceberg.Properties) error {
	if record.NumRows() == 0 {
		return nil
	}

	c.logger.Debug("appending records to table",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int64("record_count", record.NumRows()))

	if c.awsConfig != nil {
		ctx = utils.WithAwsConfig(ctx, c.awsConfig)
	}

	tableIdent := catalog.ToIdentifier(namespace, tableName)

	// Load table fresh to get the latest state
	tbl, err := c.catalog.LoadTable(ctx, tableIdent)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", namespace, tableName, err)
	}

	// Use the record's own schema for the RecordReader.
	// - We cannot use the table's Iceberg schema converted back to Arrow because
	//   ArrowSchemaToIcebergWithFreshIDs assigns fresh field IDs during table creation,
	//   which differ from the field IDs in the original Arrow schema used to create records.
	// - The resulting schema mismatch causes "arrow/array: mismatch schema" errors.
	// - iceberg-go's tbl.Append() handles the actual field ID mapping internally when
	//   writing Parquet files with proper Iceberg metadata.
	rdr, err := array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer rdr.Release()

	// Add OTEL metadata to snapshot properties
	snapshotProps := iceberg.Properties{
		"otel.exporter":     "iceberg",
		"otel.record_count": fmt.Sprintf("%d", record.NumRows()),
	}
	for k, v := range props {
		snapshotProps[k] = v
	}

	// table.Append() handles:
	// - Automatic partitioning based on table's partition spec
	// - Parquet file writing to storage
	// - Catalog metadata registration
	// All atomically within a transaction
	_, err = tbl.Append(ctx, rdr, snapshotProps)
	if err != nil {
		return fmt.Errorf("failed to append records to table %s.%s: %w", namespace, tableName, err)
	}

	c.logger.Info("successfully appended records to table",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int64("record_count", record.NumRows()))

	return nil
}

// AppendDataFiles implements Catalog.AppendDataFiles.
func (c *RESTCatalog) AppendDataFiles(ctx context.Context, opts []AppendOptions) error {
	if len(opts) == 0 {
		return nil
	}

	// All files must belong to the same namespace and table
	namespace := opts[0].Namespace
	tableName := opts[0].Table

	// Collect file paths and calculate totals
	filePaths := make([]string, 0, len(opts))
	var totalRecords, totalSize int64
	for _, opt := range opts {
		if opt.Namespace != namespace || opt.Table != tableName {
			return fmt.Errorf("all files must belong to the same namespace and table, got %s.%s and %s.%s",
				namespace, tableName, opt.Namespace, opt.Table)
		}
		filePaths = append(filePaths, opt.FilePath)
		totalRecords += opt.RecordCount
		totalSize += opt.FileSizeBytes
	}

	c.logger.Debug("appending data files to table",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int("file_count", len(filePaths)),
		zap.Int64("total_records", totalRecords),
		zap.Int64("total_size", totalSize))

	if c.awsConfig != nil {
		ctx = utils.WithAwsConfig(ctx, c.awsConfig)
	}

	tableIdent := catalog.ToIdentifier(namespace, tableName)
	tbl, err := c.catalog.LoadTable(ctx, tableIdent)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", namespace, tableName, err)
	}

	tx := tbl.NewTransaction()
	snapshotProps := iceberg.Properties{
		"otel.exporter":     "iceberg",
		"otel.record_count": fmt.Sprintf("%d", totalRecords),
		"otel.file_size":    fmt.Sprintf("%d", totalSize),
		"otel.file_count":   fmt.Sprintf("%d", len(filePaths)),
	}

	if err := tx.AddFiles(ctx, filePaths, snapshotProps, false); err != nil {
		return fmt.Errorf("failed to add files to table %s.%s: %w", namespace, tableName, err)
	}

	if _, err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction for table %s.%s: %w", namespace, tableName, err)
	}

	c.logger.Info("successfully registered data files",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int("file_count", len(filePaths)))

	return nil
}

// ListDataFiles implements Catalog.ListDataFiles.
func (c *RESTCatalog) ListDataFiles(ctx context.Context, namespace, tableName string) ([]string, error) {
	c.logger.Debug("listing data files from all snapshots",
		zap.String("namespace", namespace),
		zap.String("table", tableName))

	if c.awsConfig != nil {
		ctx = utils.WithAwsConfig(ctx, c.awsConfig)
	}

	// Load the table from the catalog
	tableIdent := catalog.ToIdentifier(namespace, tableName)
	table, err := c.catalog.LoadTable(ctx, tableIdent)
	if err != nil {
		// If the table doesn't exist, return empty list
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist") {
			c.logger.Debug("table does not exist, returning empty file list",
				zap.String("namespace", namespace),
				zap.String("table", tableName))
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to load table %s.%s: %w", namespace, tableName, err)
	}

	// Get all snapshots from the table metadata (includes historical snapshots for time-travel)
	// This ensures that files removed from the current snapshot by operations like rewrite_data_files
	// but still referenced by older snapshots are correctly identified as registered files.
	snapshots := table.Metadata().Snapshots()
	if len(snapshots) == 0 {
		c.logger.Debug("table has no snapshots, returning empty file list",
			zap.String("namespace", namespace),
			zap.String("table", tableName))
		return []string{}, nil
	}

	c.logger.Debug("found snapshots to scan",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int("snapshot_count", len(snapshots)))

	// Get the file IO from the table for reading manifests
	fileIO, err := table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get file IO for table %s.%s: %w", namespace, tableName, err)
	}

	// Use a map to deduplicate file paths across snapshots
	filePathSet := make(map[string]struct{})

	// Iterate through ALL snapshots to collect all data files
	for _, snapshot := range snapshots {
		manifests, err := snapshot.Manifests(fileIO)
		if err != nil {
			c.logger.Warn("failed to read manifests for snapshot, skipping",
				zap.Int64("snapshot_id", snapshot.SnapshotID),
				zap.Error(err))
			continue
		}

		for _, manifest := range manifests {
			// Skip delete manifests, we only want data files
			if manifest.ManifestContent() != iceberg.ManifestContentData {
				continue
			}

			// Fetch manifest entries (skip deleted entries within the manifest)
			entries, err := manifest.FetchEntries(fileIO, true)
			if err != nil {
				c.logger.Warn("failed to read manifest entries, skipping manifest",
					zap.String("manifest", manifest.FilePath()),
					zap.Int64("snapshot_id", snapshot.SnapshotID),
					zap.Error(err))
				continue
			}

			for _, entry := range entries {
				// Include all data files regardless of status within the manifest entry,
				// because a file marked as DELETED in one snapshot may still be
				// EXISTING/ADDED in an older snapshot that we need to track
				filePathSet[entry.DataFile().FilePath()] = struct{}{}
			}
		}
	}

	// Convert map to slice
	filePaths := make([]string, 0, len(filePathSet))
	for path := range filePathSet {
		filePaths = append(filePaths, path)
	}

	c.logger.Debug("listed data files from all snapshots",
		zap.String("namespace", namespace),
		zap.String("table", tableName),
		zap.Int("snapshot_count", len(snapshots)),
		zap.Int("file_count", len(filePaths)))

	return filePaths, nil
}

// Close implements Catalog.Close.
func (c *RESTCatalog) Close() error {
	c.tableCache.Range(func(key, _ interface{}) bool {
		c.tableCache.Delete(key)
		return true
	})
	return nil
}

// GetCatalogType implements Catalog.GetCatalogType.
func (c *RESTCatalog) GetCatalogType() string {
	return CatalogTypeRest
}

// arrowSchemaToIcebergSchema converts an Arrow schema to Iceberg schema.
// This uses iceberg-go's ArrowSchemaToIcebergWithFreshIDs which:
// 1. Converts Arrow types to Iceberg types (including unsigned → signed conversion)
// 2. Automatically assigns fresh sequential field IDs (1, 2, 3, ...)
// 3. Properly handles nested types (lists, maps) with unique element IDs
func arrowSchemaToIcebergSchema(schema *arrow.Schema) (*iceberg.Schema, error) {
	// The second parameter (downcastNsTimestamp) is false since we use microsecond timestamps.
	return table.ArrowSchemaToIcebergWithFreshIDs(schema, false)
}

// buildIcebergPartitionSpec builds an Iceberg partition spec from our config.
func buildIcebergPartitionSpec(spec PartitionSpec, schema *iceberg.Schema) iceberg.PartitionSpec {
	if len(spec.Fields) == 0 {
		return iceberg.NewPartitionSpec()
	}

	var partFields []iceberg.PartitionField
	for i, f := range spec.Fields {
		// Find source field ID from schema
		sourceID := -1
		for _, field := range schema.Fields() {
			if field.Name == f.SourceColumn {
				sourceID = field.ID
				break
			}
		}

		if sourceID == -1 {
			continue
		}

		transform := parseTransform(f.Transform)
		partFields = append(partFields, iceberg.PartitionField{
			SourceID:  sourceID,
			FieldID:   1000 + i, // To avoid conflicts with Iceberg's schema field IDs
			Name:      f.Name,
			Transform: transform,
		})
	}

	return iceberg.NewPartitionSpec(partFields...)
}

// parseTransform parses a transform string into an Iceberg transform.
func parseTransform(transform string) iceberg.Transform {
	switch transform {
	case "identity":
		return iceberg.IdentityTransform{}
	case "year":
		return iceberg.YearTransform{}
	case "month":
		return iceberg.MonthTransform{}
	case "day":
		return iceberg.DayTransform{}
	case "hour":
		return iceberg.HourTransform{}
	default:
		return iceberg.IdentityTransform{}
	}
}

// buildAWSConfigFromStorage creates an AWS config from the storage configuration.
// This is used to bypass iceberg-go's S3 property parsing which doesn't support
// properties like s3.signer.uri returned by some REST catalogs.
// By injecting our own AWS config via context, we bypass ParseAWSConfig() entirely.
func buildAWSConfigFromStorage(ctx context.Context, storageCfg FileIOConfig, vlogger *logger.VerboseLogger) (*aws.Config, error) {
	// Log the storage type being used
	switch storageCfg.Type {
	case "s3", "":
		vlogger.Debug("building AWS config from S3 storage config",
			zap.String("region", storageCfg.S3.Region),
			zap.Bool("has_credentials", storageCfg.S3.AccessKeyID != ""))
	case "r2":
		vlogger.Debug("building AWS config from R2 storage config",
			zap.Bool("has_credentials", storageCfg.R2.AccessKeyID != ""))
	case "filesystem":
		vlogger.Debug("building default AWS config for filesystem storage")
	}

	// Delegate to the centralized auth utility
	return BuildAWSConfigFromStorageConfig(ctx, storageCfg)
}

// normalizeIcebergError builds a descriptive error message by extracting a meaningful
// error message from iceberg-go errors.
//
// iceberg-go's REST client may return empty error messages when:
// 1. The REST API returns errors in an unexpected format
// 2. The response body cannot be parsed as JSON
// 3. The error JSON structure differs from expected {"error": {"type": "...", "message": "..."}}
func normalizeIcebergError(err error) string {
	if err == nil {
		return "unknown error (nil)"
	}

	errMsg := err.Error()

	// Check for empty or essentially empty error messages
	// iceberg-go's errorResponse.Error() returns "Type: Message", so if both are empty, we get ": "
	if errMsg == "" || errMsg == ": " || strings.TrimSpace(errMsg) == ":" {
		// Try to get more info from the error type
		errType := fmt.Sprintf("%T", err)
		errTypeLower := strings.ToLower(errType)

		// Check for common iceberg-go error types and provide better messages
		// Use case-insensitive comparison to handle both rest.errorResponse and any mock types
		switch {
		case strings.Contains(errTypeLower, "errorresponse"):
			return fmt.Sprintf("REST API returned error with no details (type: %s). Check catalog server logs for more information", errType)
		case strings.Contains(errTypeLower, "url.error"):
			return fmt.Sprintf("network error connecting to catalog: %v", err)
		default:
			return fmt.Sprintf("error with no message (type: %s)", errType)
		}
	}

	// Check for known error patterns and enhance them
	switch {
	case strings.Contains(errMsg, "connection refused"):
		return errMsg + " - ensure the REST catalog service is running"
	case strings.Contains(errMsg, "no such host"):
		return errMsg + " - check the catalog URI hostname"
	case strings.Contains(errMsg, "timeout"):
		return errMsg + " - catalog service may be overloaded or unreachable"
	default:
		return errMsg
	}
}
