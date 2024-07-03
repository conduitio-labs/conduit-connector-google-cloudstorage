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

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SnapshotIterator to iterate through GoogleCloudStorage objects in a specific bucket.
type SnapshotIterator struct {
	bucket string
	client *storage.Client

	iterator           *storage.ObjectIterator
	maxLastModified    time.Time
	maxKeyLastModified string
}

// NewSnapshotIterator takes the GoogleCloudStorage bucket, the client, and the position.
// it returns a snapshotIterator starting from the position provided.
func NewSnapshotIterator(ctx context.Context, bucket string, client *storage.Client, p position.Position) (*SnapshotIterator, error) {
	sdk.Logger(ctx).Trace().Msg("NewSnapshotIterator: Starting the NewSnapshotIterator")

	iterator := client.Bucket(bucket).Objects(ctx, nil)
	return &SnapshotIterator{
		bucket:          bucket,
		client:          client,
		iterator:        iterator,
		maxLastModified: p.Timestamp,
	}, nil
}

// Next returns the next record in the iterator.
// returns an empty record and an error if anything wrong happened.
func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "SnapshotIterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Starting The Next Method of SnapshotIterator Iterator")

	objectAttrs, err := s.iterator.Next()
	if err != nil {
		return sdk.Record{}, err
	}
	// read object
	objectReader, err := s.client.Bucket(s.bucket).Object(objectAttrs.Name).NewReader(ctx)
	defer func(r *storage.Reader) {
		err := r.Close()
		if err != nil {
			logger.Error().Stack().Err(err).Msg("Error while closing the object reader")
		}
	}(objectReader)

	if err != nil {
		return sdk.Record{}, fmt.Errorf("could not fetch the next object: %w", err)
	}

	ObjectData, err := io.ReadAll(objectReader)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("could not read the object's body: %w", err)
	}

	// check if maxLastModified should be updated
	if s.maxLastModified.Before(objectAttrs.Updated) {
		s.maxLastModified = objectAttrs.Updated
		s.maxKeyLastModified = objectAttrs.Name
	}

	p, err := json.Marshal(position.Position{
		Key:       objectAttrs.Name,
		Type:      position.TypeSnapshot,
		Timestamp: s.maxLastModified,
	})
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while marshalling the position")
		return sdk.Record{}, err
	}

	// create the record
	output := sdk.Util.Source.NewRecordSnapshot(
		p,
		map[string]string{
			MetadataContentType: objectAttrs.ContentType,
		},
		sdk.RawData(objectAttrs.Name),
		sdk.RawData(ObjectData),
	)

	return output, nil
}

func (s *SnapshotIterator) HasNext(_ context.Context) bool {
	if s.iterator.PageInfo().Token == "" && s.iterator.PageInfo().Remaining() == 0 {
		return false
	}
	return true
}

func (s *SnapshotIterator) Stop() {
	// nothing to stop
}
