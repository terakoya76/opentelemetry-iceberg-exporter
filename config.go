package icebergexporter

import (
	"fmt"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/multierr"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

// Config holds the configuration for the Iceberg exporter.
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	configretry.BackOffConfig    `mapstructure:"retry_on_failure"`

	// Storage configuration (required)
	Storage iceberg.FileIOConfig `mapstructure:"storage"`

	// Catalog configuration (optional - catalog type "none" disables registration)
	Catalog iceberg.CatalogConfig `mapstructure:"catalog"`

	// Partition configuration
	Partition PartitionConfig `mapstructure:"partition"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	var errs error

	// Validate storage
	if err := cfg.Storage.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}

	// Validate catalog
	if err := cfg.Catalog.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}

	// Validate partition
	if err := cfg.validatePartition(); err != nil {
		errs = multierr.Append(errs, err)
	}

	return errs
}

func (cfg *Config) validatePartition() error {
	switch cfg.Partition.Granularity {
	case "", "hourly", "daily", "monthly":
		return nil
	default:
		return fmt.Errorf("partition.granularity must be one of: hourly, daily, monthly")
	}
}
