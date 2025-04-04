// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigBucket            = "bucket"
	ConfigPollingPeriod     = "pollingPeriod"
	ConfigServiceAccountKey = "serviceAccountKey"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigBucket: {
			Default:     "",
			Description: "Bucket is the name for Google cloud storage bucket.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPollingPeriod: {
			Default:     "1s",
			Description: "PollingPeriod is the GCS CDC polling period to fetch new data at regular interval.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		ConfigServiceAccountKey: {
			Default:     "",
			Description: "ServiceAccountKey is the Google cloud platform service account key.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
