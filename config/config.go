package config

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	ConfigKeyGoogleCloudServiceAccountKey = "serviceAccountKey"
	ConfigKeyGoogleCloudStorageBucket     = "bucket"
)

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

type Config struct {
	GoogleCloudStorageBucket     string
	GoogleCloudServiceAccountKey string
}

func ParseGlobalConfig(ctx context.Context, cfg map[string]string) (Config, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("ParseGlobalConfig: Started Parsing the config")

	err := checkEmpty(cfg)
	if err != nil {
		logger.Error().Msg("ParseGlobalConfig: Error While Check Empty")
		return Config{}, err
	}

	serviceAccountKey, ok := cfg[ConfigKeyGoogleCloudServiceAccountKey]
	if !ok {
		logger.Error().Msgf("ParseGlobalConfig: Error while Parsing the config for the key %q", ConfigKeyGoogleCloudServiceAccountKey)
		return Config{}, requiredConfigErr(ConfigKeyGoogleCloudServiceAccountKey)
	}

	bucket, ok := cfg[ConfigKeyGoogleCloudStorageBucket]
	if !ok {
		logger.Error().Msgf("ParseGlobalConfig: Error while Parsing the config for the key %q", ConfigKeyGoogleCloudStorageBucket)
		return Config{}, requiredConfigErr(ConfigKeyGoogleCloudStorageBucket)
	}

	logger.Info().Msg("ParseGlobalConfig: Successfully Parsed the config")
	return Config{
		GoogleCloudStorageBucket:     bucket,
		GoogleCloudServiceAccountKey: serviceAccountKey,
	}, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}
