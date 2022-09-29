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

package connector

import (
	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/config"
	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/source"
	sourceConfig "github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var Connector = sdk.Connector{
	NewSpecification: specification,
	NewSource:        source.NewSource,
	NewDestination:   nil,
}

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

// specification returns the connector's specification.
func specification() sdk.Specification {
	return sdk.Specification{
		Name:        "Google Cloud Storage",
		Summary:     "An Google Cloud Storage Source and Destination Connector for Conduit, Written in Go.",
		Description: "Real time data transmission with google cloud storage",
		Version:     version,
		Author:      "Santosh Kumar Gajawada",
		SourceParams: map[string]sdk.Parameter{
			config.ConfigKeyGCPServiceAccountKey: {
				Default:     "",
				Required:    true,
				Description: "Google Cloud Storage ServiceAccountKey",
			},
			config.ConfigKeyGCSBucket: {
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
