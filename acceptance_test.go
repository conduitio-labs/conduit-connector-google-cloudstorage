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

package cloudstorage

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	sourceConfig "github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/config"
	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/utils"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"go.uber.org/goleak"
)

type GCSAcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
	GCSClient *storage.Client
}

func (d GCSAcceptanceTestDriver) WriteToSource(t *testing.T, records []opencdc.Record) []opencdc.Record {
	ctx := context.Background()
	testBucket := d.Config.SourceConfig[sourceConfig.ConfigBucket]
	for _, record := range records {
		wc := d.GCSClient.Bucket(testBucket).Object(string(record.Key.Bytes())).NewWriter(ctx)
		defer func() {
			if err := wc.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := wc.Write(record.Payload.After.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	return records
}

func TestAcceptance(t *testing.T) {
	srcConfig, projectID, err := utils.ParseIntegrationConfig()
	if err != nil {
		t.Skip(err)
	}

	gcsClient, err := utils.NewGCSClient(srcConfig)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := gcsClient.Close(); err != nil {
			t.Fatal(err)
		}
	})

	sdk.AcceptanceTest(t, GCSAcceptanceTestDriver{
		sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      srcConfig,
				DestinationConfig: nil,
				BeforeTest: func(t *testing.T) {
					srcConfig[sourceConfig.ConfigBucket] = "acceptance-test-bucket-" + uuid.NewString()
					if err := utils.CreateTestGCSBucket(gcsClient, projectID, srcConfig[sourceConfig.ConfigBucket]); err != nil {
						t.Fatalf("could not create test gcs bucket: %v", err)
					}
				},
				AfterTest: func(t *testing.T) {
					utils.ClearAndDeleteTestGCSBucket(t, gcsClient, srcConfig[sourceConfig.ConfigBucket])
				},
				// Apart from the IgnoreCurrent, runtime_pollWait is also ignorned because here the GCS/storage client(Created Above) make a gRPC connection which is consistent and opens until it is closed.
				GoleakOptions: []goleak.Option{goleak.IgnoreCurrent(), goleak.IgnoreTopFunction("internal/poll.runtime_pollWait")},
				// Tests Starting from TestAcceptance/TestDestination were skipped because the destination connector for the GCS is not implemented.
				Skip: []string{"^TestAcceptance/TestDestination_"},
			},
		},
		gcsClient,
	})
}

// GenerateRecord needed to override because maximum object length for GCS is 1024 characters.
func (d GCSAcceptanceTestDriver) GenerateRecord(_ *testing.T, operation opencdc.Operation) opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position(uuid.NewString()),
		Operation: operation,
		Key:       opencdc.RawData(uuid.NewString()),
		Payload: opencdc.Change{
			After: opencdc.RawData(uuid.NewString()),
		},
	}
}
