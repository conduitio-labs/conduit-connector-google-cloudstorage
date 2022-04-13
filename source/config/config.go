package config

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio/conduit-connector-google-cloudstorage/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	ConfigKeyPollingPeriod = "pollingPeriod"
	DefaultPollingPeriod   = "1s"
)

type SourceConfig struct {
	config.Config
	ConfigKeyPollingPeriod time.Duration
}

func ParseSourceConfig(ctx context.Context, cfg map[string]string) (SourceConfig, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("ParseSourceConfig: Start Parsing the Config")

	globalConfig, err := config.ParseGlobalConfig(ctx, cfg)
	if err != nil {
		logger.Error().Msgf("ParseSourceConfig: Error While Parsing the Global Config: %v", err)
		return SourceConfig{}, err
	}

	pollingPeriodString, exists := cfg[ConfigKeyPollingPeriod]
	if !exists || pollingPeriodString == "" {
		pollingPeriodString = DefaultPollingPeriod
	}
	pollingPeriod, err := time.ParseDuration(pollingPeriodString)
	if err != nil {
		return SourceConfig{}, fmt.Errorf(
			"%q config value should be a valid duration",
			ConfigKeyPollingPeriod,
		)
	}
	if pollingPeriod <= 0 {
		return SourceConfig{}, fmt.Errorf(
			"%q config value should be positive, got %s",
			ConfigKeyPollingPeriod,
			pollingPeriod,
		)
	}

	logger.Info().Msg("ParseSourceConfig: Start Parsing the Config")
	return SourceConfig{
		Config:                 globalConfig,
		ConfigKeyPollingPeriod: pollingPeriod,
	}, nil
}
