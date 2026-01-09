package icebergexporter

import (
	"fmt"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/multierr"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

// supportedLevels defines the verbosity levels supported by the iceberg exporter.
// LevelNone is not supported because it would mean no export.
var supportedLevels = map[configtelemetry.Level]struct{}{
	configtelemetry.LevelBasic:    {},
	configtelemetry.LevelNormal:   {},
	configtelemetry.LevelDetailed: {},
}

// Config holds the configuration for the Iceberg exporter.
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	configretry.BackOffConfig    `mapstructure:"retry_on_failure"`

	// Verbosity controls the verbosity of the exporter's logging output.
	// Valid values are: basic, normal (default), detailed.
	Verbosity configtelemetry.Level `mapstructure:"verbosity"`

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

	if err := cfg.validateVerbosity(); err != nil {
		errs = multierr.Append(errs, err)
	}

	if err := cfg.Storage.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}

	if err := cfg.Catalog.Validate(); err != nil {
		errs = multierr.Append(errs, err)
	}

	if err := cfg.validatePartition(); err != nil {
		errs = multierr.Append(errs, err)
	}

	return errs
}

func (cfg *Config) validateVerbosity() error {
	if _, ok := supportedLevels[cfg.Verbosity]; !ok {
		return fmt.Errorf("verbosity level %q is not supported, supported levels are: basic, normal, detailed", cfg.Verbosity)
	}
	return nil
}

func (cfg *Config) validatePartition() error {
	switch cfg.Partition.Granularity {
	case "", "hourly", "daily", "monthly":
		return nil
	default:
		return fmt.Errorf("partition.granularity must be one of: hourly, daily, monthly")
	}
}
