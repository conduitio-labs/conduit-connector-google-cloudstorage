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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/config"
	sourceConnector "github.com/conduitio/conduit-connector-google-cloudstorage/source"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

type Object struct {
	key     string
	content string
}

func TestSource_SuccessfulSnapshot(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 5)

	// read and assert
	var record sdk.Record
	for _, file := range testFiles {
		record, err = readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if p, err := position.ParseRecordPosition(record.Position); err != nil {
		t.Fatal(err)
	} else if p.Type != position.TypeCDC {
		t.Fatalf("expected a position Type CDC, got: %v", p.Type)
	}

	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_SnapshotRestart(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	numberOfFiles := 5
	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, numberOfFiles)

	// read and assert until last file is left
	var record sdk.Record
	for i := 0; i < numberOfFiles-1; i++ {
		record, err = readAndAssert(ctx, t, source, testFiles[i])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	lastReadPosition := record.Position
	if p, err := position.ParseRecordPosition(lastReadPosition); err != nil {
		t.Fatal(err)
	} else if p.Type != position.TypeSnapshot {
		t.Fatalf("expected a position Type TypeSnapshot, got: %v", p.Type)
	}

	// Stop the source
	_ = source.Teardown(ctx)

	// Snapshot Restart will read all the files again

	source = &sourceConnector.Source{}
	err = source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Open with lastReadPosition
	err = source.Open(ctx, lastReadPosition)
	if err != nil {
		t.Fatal(err)
	}

	// read and assert all the files
	for i := 0; i < numberOfFiles; i++ {
		record, err = readAndAssert(ctx, t, source, testFiles[i])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// As the last item reached now the position will be cdc
	if p, err := position.ParseRecordPosition(record.Position); err != nil {
		t.Fatal(err)
	} else if p.Type != position.TypeCDC {
		t.Fatalf("expected a position Type CDC, got: %v", p.Type)
	}

	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_SnapshotRestartAfterLastRecord(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	numberOfFiles := 5
	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, numberOfFiles)

	// read and assert until last file is left
	var record sdk.Record
	for i := 0; i < numberOfFiles; i++ {
		record, err = readAndAssert(ctx, t, source, testFiles[i])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	lastReadPosition := record.Position
	if p, err := position.ParseRecordPosition(lastReadPosition); err != nil {
		t.Fatal(err)
	} else if p.Type != position.TypeCDC {
		t.Fatalf("expected a position Type TypeCDC, got: %v", p.Type)
	}

	// Stop the source
	_ = source.Teardown(ctx)

	content := uuid.NewString()
	testFileName := "test-file1"
	// insert a file to the bucket
	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	// Snapshot Restart will not read all the files again instead reads only the newly added testfile

	source = &sourceConnector.Source{}
	err = source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Open with lastReadPosition
	err = source.Open(ctx, lastReadPosition)
	if err != nil {
		t.Fatal(err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}
	if strings.Compare(string(obj.Payload.After.Bytes()), content) != 0 {
		t.Fatalf("expected payload: %s, got: %s", content, string(obj.Payload.After.Bytes()))
	}

	// As there is nothing left in the bucket ErrBackoffRetry will be returned
	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_SnapshotStartFromNonNilPosition(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
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
	err = source.Open(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 10)

	// read and assert
	for _, file := range testFiles {
		// snapshot will start again from beginning if the Position.Type is Snapshot But the Position.Key is not Empty.
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestSource_EmptyBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = source.Read(ctx)

	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_NonExistentBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)
	ctx := context.Background()

	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	// set the bucket name to a unique uuid
	cfg[config.ConfigKeyGCSBucket] = uuid.NewString()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// bucket existence check at "Open"
	err = source.Open(ctx, nil)
	if err == nil {
		t.Fatal("should return an error for non existent buckets")
	}
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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
}

func TestSource_CDC_ReadRecordsUpdate(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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
	time.Sleep(time.Millisecond)

	content := uuid.NewString()
	testFileName := "file0000" // already exists in the bucket

	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	// the update should be detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	if strings.Compare(string(obj.Payload.After.Bytes()), content) != 0 {
		t.Fatalf("expected Payload: %s, got: %s", content, string(obj.Payload.After.Bytes()))
	}
}

func TestSource_CDC_ReadRecordsInsert(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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
	time.Sleep(time.Millisecond)

	content := uuid.NewString()
	testFileName := "test-file"
	// insert a file to the bucket
	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	obj, err := readWithTimeout(ctx, source, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	if strings.Compare(string(obj.Payload.After.Bytes()), content) != 0 {
		t.Fatalf("expected Payload: %s, got: %s", content, string(obj.Payload.After.Bytes()))
	}
}

func TestSource_CDC_ReadRecordsInsertContextCancellation(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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
	time.Sleep(time.Millisecond)

	content := uuid.NewString()
	testFileName := "test-file"
	// insert a file to the bucket
	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	obj, err := readWithTimeout(ctx, source, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	cancel()
	if _, err := source.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected error: %v, got: %v", context.Canceled, err)
	}
}

func TestSource_CDCRestart(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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

	// make sure the insert action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Millisecond)

	content := uuid.NewString()
	testFileName := "test-file"
	// insert a file to the bucket
	wc := client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	obj, err := readWithTimeout(ctx, source, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	lastReadPosition := obj.Position
	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	// call teardown to stop iterator and close the client
	_ = source.Teardown(ctx)

	// start the source process again
	source1 := &sourceConnector.Source{}
	defer func() {
		_ = source1.Teardown(ctx)
	}()

	err = source1.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source1.Open(ctx, lastReadPosition)
	if err != nil {
		t.Fatal(err)
	}

	// make sure the insert action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Millisecond)

	content = uuid.NewString()
	testFileName = "test-file1"
	// insert a file to the bucket
	wc = client.Bucket(testBucket).Object(testFileName).NewWriter(ctx)
	writeAndClose(t, wc, content)

	obj, err = readWithTimeout(ctx, source1, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}
}

func TestSource_CDCPositionToCaptureInsertandDeleteActions(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)
	startTime := time.Now()

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	addObjectsToTheBucket(ctx, t, testBucket, client, 2)

	testFileName := "file0001" // already exists in the bucket
	expectedAction := "delete"
	// Delete a file that exists in the bucket
	err = client.Bucket(testBucket).Object(testFileName).Delete(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	p, err := json.Marshal(position.Position{
		Key:       "file0001",
		Timestamp: startTime,
		Type:      1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// initialize the connector to start detecting changes from the past, so all the bucket is new data
	err = source.Open(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Read(ctx)
	// error is expected after resetting the connector with a new CDC position
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("GCS connector should return a BackoffRetry error for the first Read() call after starting CDC, Instead the returned Error:%v", err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	// the Read should return the first file from the bucket, since in has the oldest modified date
	if strings.Compare(string(obj.Key.Bytes()), "file0000") != 0 {
		t.Fatalf("expected key: 'file0000', got: %s", string(obj.Key.Bytes()))
	}

	// next read should return the deleted file
	obj2, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Compare(string(obj2.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj2.Key.Bytes()))
	}
	if obj2.Operation != sdk.OperationDelete {
		t.Fatalf("expected action: %s, got: %s", expectedAction, obj2.Operation.String())
	}
}

func TestSource_CDC_DeleteWithVersioning(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
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

	// make sure the update action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Millisecond)

	testFileName := "file0001" // already exists in the bucket
	expectedAction := "delete"
	// Delete a file that exists in the bucket
	err = client.Bucket(testBucket).Object(testFileName).Delete(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}
	if obj.Operation != sdk.OperationDelete {
		t.Fatalf("expected action: %s, got: %s", expectedAction, obj.Operation.String())
	}
}

func TestSource_CDC_EmptyBucketWithDeletedObjects(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyGCSBucket]
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add one file
	testFiles := addObjectsToTheBucket(ctx, t, testBucket, client, 1)

	// delete the added file
	testFileName := "file0000"
	// Delete a file that exists in the bucket
	err = client.Bucket(testBucket).Object(testFileName).Delete(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// we need the deleted file's modified date to be in the past
	time.Sleep(time.Millisecond)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if !errors.Is(err, sdk.ErrBackoffRetry) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// should have changed to CDC
	// CDC should NOT read the deleted object
	_, err = readWithTimeout(ctx, source, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should be DeadlineExceeded but got %v", err)
	}
}

func TestOpenSource_FailsWhenClientCreation(t *testing.T) {
	err := os.Setenv("GCP_ServiceAccount_Key", "Incorrect service account key")
	os.Setenv("GCP_Bucket", "bucket")
	os.Setenv("GCP_ProjectID", "projectid")
	ctx := context.Background()

	if err != nil {
		t.Fatal(err)
	}
	cfg, err := parseIntegrationConfig()
	if err != nil {
		t.Fatal(err)
	}

	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err = source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = source.Open(ctx, nil)
	expectedErr := "dialing: invalid character 'I' looking for beginning of value"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestOpenSource_FailsParsePosition(t *testing.T) {
	cfg, err := parseIntegrationConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err = source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = source.Open(ctx, []byte("Invalid Position"))
	expectedErr := "invalid character 'I' looking for beginning of value"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestOpenSource_InvalidPositionType(t *testing.T) {
	cfg, err := parseIntegrationConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	source := &sourceConnector.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()

	err = source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	p, err := json.Marshal(position.Position{
		Key:       "xyz",
		Timestamp: time.Now(),
		Type:      2,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = source.Open(ctx, p)
	expectedErr := "invalid position type, no TypeSnapshot:0 or TypeCDC:1"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
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
		clearAndDeleteTestGCSBucket(t, client, bucket)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	})

	cfg[config.ConfigKeyGCSBucket] = bucket

	return client, cfg
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
		writeAndClose(t, wc, content)
	}
	return testFiles
}

// readWithTimeout will try to read the next record until the timeout is reached.
func readWithTimeout(ctx context.Context, source *sourceConnector.Source, timeout time.Duration) (sdk.Record, error) {
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
func readAndAssert(ctx context.Context, t *testing.T, source *sourceConnector.Source, want Object) (sdk.Record, error) {
	got, err := source.Read(ctx)
	if err != nil {
		return got, err
	}

	gotKey := string(got.Key.Bytes())
	gotPayload := string(got.Payload.After.Bytes())
	if gotKey != want.key {
		t.Fatalf("expected key: %s\n got: %s", want.key, gotKey)
	}
	if gotPayload != want.content {
		t.Fatalf("expected content: %s\n got: %s", want.content, gotPayload)
	}

	if err := source.Ack(ctx, got.Position); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return got, err
}

func writeAndClose(t *testing.T, w *storage.Writer, data string) {
	defer func() {
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := fmt.Fprint(w, data); err != nil {
		t.Fatal(err)
	}
}
