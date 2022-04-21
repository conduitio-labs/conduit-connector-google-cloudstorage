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
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// ConfigKeyGCPServiceAccountKey is the config name for Google cloud platform service account key
	ConfigKeyGCPServiceAccountKey = "serviceAccountKey"

	// ConfigKeyGCSBucket is the config name for Google cloud storage bucket
	ConfigKeyGCSBucket = "bucket"
)

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

// Config represents configuration needed for Google cloud storage
type Config struct {
	GoogleCloudStorageBucket     string
	GoogleCloudServiceAccountKey string
}

// ParseGlobalConfig attempts to parse plugins.Config into a Config struct
func ParseGlobalConfig(ctx context.Context, cfg map[string]string) (Config, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "ParseGlobalConfig").Logger()
	logger.Trace().Msg("Started Parsing the config")

	err := checkEmpty(cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Check Empty")
		return Config{}, err
	}

	serviceAccountKey, ok := cfg[ConfigKeyGCPServiceAccountKey]
	if !ok {
		err := RequiredConfigErr(ConfigKeyGCPServiceAccountKey)
		logger.Error().Stack().Err(err).Msgf("Error while Parsing the config for the key %q", ConfigKeyGCPServiceAccountKey)
		return Config{}, err
	}

	bucket, ok := cfg[ConfigKeyGCSBucket]
	if !ok {
		err := RequiredConfigErr(ConfigKeyGCSBucket)
		logger.Error().Stack().Err(err).Msgf("Error while Parsing the config for the key %q", ConfigKeyGCSBucket)
		return Config{}, err
	}

	logger.Trace().Msg("Successfully Parsed the config")
	return Config{
		GoogleCloudStorageBucket:     bucket,
		GoogleCloudServiceAccountKey: serviceAccountKey,
	}, nil
}

func RequiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}
