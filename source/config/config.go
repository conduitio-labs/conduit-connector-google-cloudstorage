// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// ConfigKeyPollingPeriod is the config name for the GCS CDC polling period.
	ConfigKeyPollingPeriod = "pollingPeriod"

	// DefaultPollingPeriod is the value assumed for the pooling period when the
	// config omits the polling period parameter.
	DefaultPollingPeriod = "1s"
)

// SourceConfig represents source configuration with GCS configurations.
type SourceConfig struct {
	config.Config
	PollingPeriod time.Duration
}

// ParseSourceConfig attempts to parse the configurations into a SourceConfig struct that Source could utilize.
func ParseSourceConfig(ctx context.Context, cfg map[string]string) (SourceConfig, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "ParseSourceConfig").Logger()
	logger.Trace().Msg("Start Parsing the Config")

	globalConfig, err := config.ParseGlobalConfig(ctx, cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Parsing the Global Config")
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

	logger.Trace().Msg("Start Parsing the Config")
	return SourceConfig{
		Config:        globalConfig,
		PollingPeriod: pollingPeriod,
	}, nil
}
