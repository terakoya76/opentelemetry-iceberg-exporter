package iceberg

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// NewCatalog creates a new Catalog based on the configuration.
// Returns NoCatalog if catalog type is CatalogTypeNone ("none").
// Returns error if catalog type is empty (must be explicitly configured).
func NewCatalog(ctx context.Context, cfg CatalogConfig, storageCfg FileIOConfig, vlogger *logger.VerboseLogger) (Catalog, error) {
	switch cfg.Type {
	case "":
		return nil, fmt.Errorf("catalog.type is required: must be one of %q or %q", CatalogTypeRest, CatalogTypeNone)

	case CatalogTypeNone:
		vlogger.Info("catalog registration explicitly disabled (type=" + CatalogTypeNone + ")")
		return NewNoCatalog(), nil

	case CatalogTypeRest:
		vlogger.Info("using REST catalog",
			zap.String("uri", cfg.REST.URI),
			zap.String("warehouse", cfg.REST.Warehouse))
		return NewRESTCatalog(ctx, cfg.REST, storageCfg, vlogger)

	default:
		return nil, fmt.Errorf("unsupported catalog type: %s (must be one of %q or %q)", cfg.Type, CatalogTypeRest, CatalogTypeNone)
	}
}
