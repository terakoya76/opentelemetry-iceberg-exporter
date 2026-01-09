package iceberg

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// NewCatalog creates a new Catalog based on the configuration.
// Returns NoCatalog if catalog type is "none".
// Returns error if catalog type is empty (must be explicitly configured).
func NewCatalog(ctx context.Context, cfg CatalogConfig, storageCfg FileIOConfig, vlogger *logger.VerboseLogger) (Catalog, error) {
	switch cfg.Type {
	case "":
		return nil, fmt.Errorf("catalog.type is required: must be one of \"rest\" or \"none\"")

	case "none":
		vlogger.Info("catalog registration explicitly disabled (type=none)")
		return NewNoCatalog(), nil

	case "rest":
		vlogger.Info("using REST catalog",
			zap.String("uri", cfg.REST.URI),
			zap.String("warehouse", cfg.REST.Warehouse))
		return NewRESTCatalog(ctx, cfg.REST, storageCfg, vlogger)

	default:
		return nil, fmt.Errorf("unsupported catalog type: %s (must be one of \"rest\" or \"none\")", cfg.Type)
	}
}
