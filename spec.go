package connector

import (
	"github.com/conduitio/conduit-connector-google-cloudstorage/config"
	sourceConfig "github.com/conduitio/conduit-connector-google-cloudstorage/source/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "Google Cloud Storage",
		Summary:     "An Google Cloud Storage Source and Destination Connector for Conduit, Written in Go.",
		Description: "Real time data transmission with google cloud storage",
		Version:     "v0.1.0",
		Author:      "Santosh Kumar Gajawada",
		SourceParams: map[string]sdk.Parameter{
			config.ConfigKeyGoogleCloudServiceAccountKey: {
				Default:     "",
				Required:    true,
				Description: "Google Cloud Storage ServiceAccountKey",
			},
			config.ConfigKeyGoogleCloudStorageBucket: {
				Default:     "",
				Required:    true,
				Description: "Google Cloud Storage Bucket",
			},
			sourceConfig.ConfigKeyPollingPeriod: {
				Default:     sourceConfig.DefaultPollingPeriod,
				Required:    false,
				Description: "polling period for the CDC mode, formatted as a time.Duration string.",
			},
		},
		DestinationParams: map[string]sdk.Parameter{},
	}
}
