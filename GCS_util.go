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
	"context"
	"errors"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/config"
	sourceConfig "github.com/conduitio/conduit-connector-google-cloudstorage/source/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	projectID = "projectID"
)

func newGCSClient(cfg map[string]string) (*storage.Client, error) {
	ctx := context.Background()
	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(cfg[config.ConfigKeyGCPServiceAccountKey])))
}

func createTestGCSBucket(client *storage.Client, projectID, bucketName string) error {
	bucket := client.Bucket(bucketName)
	ctx := context.Background()
	return bucket.Create(ctx, projectID, &storage.BucketAttrs{
		VersioningEnabled: true,
	})
}

func clearAndDeleteTestGCSBucket(t *testing.T, client *storage.Client, bucket string) {
	ctx := context.Background()
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Versions: true,
	})

	for {
		objAttr, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatalf("could not iterate objects: %v", err)
		}

		err = client.Bucket(bucket).Object(objAttr.Name).Generation(objAttr.Generation).Delete(context.Background())
		if err != nil {
			t.Fatalf("could not delete object %s: %v", objAttr.Name, err)
		}
	}

	bucketHandle := client.Bucket(bucket)
	if err := bucketHandle.Delete(context.Background()); err != nil {
		t.Fatalf("Bucket(%q).Delete: %v", bucket, err)
	}
}

func parseIntegrationConfig() (map[string]string, error) {
	serviceAccountKey := os.Getenv("GCP_ServiceAccount_Key")
	if serviceAccountKey == "" {
		return map[string]string{}, errors.New("GCP_ServiceAccount_Key env var must be set")
	}

	projectid := os.Getenv("GCP_ProjectID")
	if projectid == "" {
		return map[string]string{}, errors.New("GCP_ProjectID env var must be set")
	}

	bucket := os.Getenv("GCP_Bucket")
	if bucket == "" {
		return map[string]string{}, errors.New("GCP_Bucket env var must be set")
	}

	return map[string]string{
		config.ConfigKeyGCPServiceAccountKey: serviceAccountKey,
		config.ConfigKeyGCSBucket:            bucket,
		projectID:                            projectid,
		sourceConfig.ConfigKeyPollingPeriod:  "100ms",
	}, nil
}
