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

package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/config"
	sourceConfig "github.com/conduitio/conduit-connector-google-cloudstorage/source/config"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Object struct {
	key     string
	content string
}

const (
	projectID = "projectID"
)

func TestSource_SuccessfulSnapshot(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 5)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	_ = source.Teardown(ctx)
}

func TestSource_SnapshotRestart(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := json.Marshal(position.Position{
		Key:       "file0003",
		Timestamp: time.Now(),
		Type:      0,
	})
	if err != nil {
		t.Fatal(err)
	}
	// set a non nil position
	err = source.Open(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 10)

	// read and assert
	for _, file := range testFiles {
		// first position is not nil, then snapshot will start from beginning
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	_ = source.Teardown(ctx)
}

func TestSource_EmptyBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = source.Read(ctx)

	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
	_ = source.Teardown(ctx)
}

func TestSource_NonExistentBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)

	source := &Source{}

	// set the bucket name to a unique uuid
	cfg[config.ConfigKeyGCSBucket] = uuid.NewString()

	err := source.Configure(context.Background(), cfg)
	if err == nil {
		t.Fatal("should return an error for non existent buckets")
	}
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// read bucket while empty
	_, err = source.Read(ctx)

	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	// write files to bucket
	addObjectsToTheBucket(ctx, t, testBucket, client, 3)

	// read one record and assert position type is CDC
	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	pos, _ := position.ParseRecordPosition(obj.Position)
	if pos.Type != position.TypeCDC {
		t.Fatalf("expected first position after reading an empty bucket to be CDC, got: %s", obj.Position)
	}
	_ = source.Teardown(ctx)
}

func TestSource_CDC_ReadRecordsUpdate(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 3)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// make sure the update action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Second)

	content := uuid.NewString()
	testFileName := "file0000" // already exists in the bucket

	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	fmt.Fprint(wc, content)
	wc.Close()

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	// the update should be detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	_ = source.Teardown(ctx)
}

func prepareIntegrationTest(t *testing.T) (*storage.Client, map[string]string) {
	cfg, err := parseIntegrationConfig()
	if err != nil {
		t.Skip(err)
	}

	client, err := newGCSClient(cfg)
	if err != nil {
		t.Fatalf("could not create GCS client: %v", err)
	}

	bucket := "conduit-gcs-source-test-" + uuid.NewString()
	if err := createTestGCSBucket(client, cfg[projectID], bucket); err != nil {
		t.Fatalf("could not create test gcs client: %v", err)
	}
	t.Cleanup(func() {
		clearTestGCSBucket(t, client, bucket)
		deleteTestGCSBucket(t, client, bucket)
	})

	cfg[config.ConfigKeyGCSBucket] = bucket

	return client, cfg
}

func newGCSClient(cfg map[string]string) (*storage.Client, error) {
	return storage.NewClient(context.Background(), option.WithCredentialsJSON([]byte(cfg[config.ConfigKeyGCPServiceAccountKey])))
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

func createTestGCSBucket(client *storage.Client, projectID, bucketName string) error {
	bucket := client.Bucket(bucketName)
	return bucket.Create(context.Background(), projectID, nil)
}

func addObjectsToTheBucket(ctx context.Context, t *testing.T, testBucket string, client *storage.Client, num int) []Object {
	testFiles := make([]Object, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("file%04d", i)
		content := uuid.NewString()
		testFiles[i] = Object{
			key:     key,
			content: content,
		}

		wc := client.Bucket(testBucket).Object(key).NewWriter(ctx)
		defer wc.Close()
		fmt.Fprint(wc, content)
	}
	return testFiles
}

func clearTestGCSBucket(t *testing.T, client *storage.Client, bucket string) {
	it := client.Bucket(bucket).Objects(context.Background(), nil)

	for {
		objAttr, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatalf("could not iterate objects: %v", err)
		}

		err = client.Bucket(bucket).Object(objAttr.Name).Delete(context.Background())
		if err != nil {
			t.Fatalf("could not delete object %s: %v", objAttr.Name, err)
		}
	}
}

func deleteTestGCSBucket(t *testing.T, client *storage.Client, bucket string) {
	bucketHandle := client.Bucket(bucket)
	if err := bucketHandle.Delete(context.Background()); err != nil {
		t.Fatalf("Bucket(%q).Delete: %v", bucket, err)
	}
}

// readWithTimeout will try to read the next record until the timeout is reached.
func readWithTimeout(ctx context.Context, source *Source, timeout time.Duration) (sdk.Record, error) {
	timeoutTimer := time.After(timeout)

	for {
		rec, err := source.Read(ctx)
		if !errors.Is(err, sdk.ErrBackoffRetry) {
			return rec, err
		}

		select {
		case <-time.After(time.Millisecond * 100):
			// try again
		case <-timeoutTimer:
			return sdk.Record{}, context.DeadlineExceeded
		}
	}
}

// readAndAssert will read the next record and assert that the returned record is
// the same as the wanted object.
func readAndAssert(ctx context.Context, t *testing.T, source *Source, want Object) (sdk.Record, error) {
	got, err := source.Read(ctx)
	if err != nil {
		return got, err
	}

	gotKey := string(got.Key.Bytes())
	gotPayload := string(got.Payload.Bytes())
	if gotKey != want.key {
		t.Fatalf("expected key: %s\n got: %s", want.key, gotKey)
	}
	if gotPayload != want.content {
		t.Fatalf("expected content: %s\n got: %s", want.content, gotPayload)
	}

	return got, err
}

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))

	if !errors.Is(err, config.ErrEmptyConfig) {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), map[string]string{"foobar": "foobar"})

	if errors.Is(err, config.RequiredConfigErr(config.ConfigKeyGCPServiceAccountKey)) {
		t.Errorf("expected error serviceAccountKey config value must be set, got %v", err)
	}
}

func TestTeardownSource_NoOpen(t *testing.T) {
	con := NewSource()
	err := con.Teardown(context.Background())

	if err != nil {
		t.Errorf("expected no error but, got %v", err)
	}
}
