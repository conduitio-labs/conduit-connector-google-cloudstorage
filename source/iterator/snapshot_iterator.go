package iterator

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SnapshotIterator to iterate through GoogleCloudStorage objects in a specific bucket.
type SnapshotIterator struct {
	bucket string
	client *storage.Client

	iterator        *storage.ObjectIterator
	maxLastModified time.Time
}

// NewSnapshotIterator takes the GoogleCloudStorage bucket, the client, and the position.
// it returns a snapshotIterator starting from the position provided.
func NewSnapshotIterator(ctx context.Context, bucket string, client *storage.Client, p position.Position) (*SnapshotIterator, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("NewSnapshotIterator: Starting the NewSnapshotIterator")

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
	logger := sdk.Logger(ctx)
	logger.Info().Msg("SnapshotIterator Next: Starting The Next Method of SnapshotIterator Iterator")

	objectAttrs, err := s.iterator.Next()
	if err != nil {
		return sdk.Record{}, err
	}
	// read object
	objectReader, err := s.client.Bucket(s.bucket).Object(objectAttrs.Name).NewReader(ctx)
	defer func(r *storage.Reader) {
		err := r.Close()
		if err != nil {
			logger.Error().Msgf("SnapshotIterator Next: Error while closing the object reader %v", err)

		}
	}(objectReader)

	if err != nil {
		return sdk.Record{}, fmt.Errorf("could not fetch the next object: %w", err)
	}

	ObjectData, err := ioutil.ReadAll(objectReader)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("could not read the object's body: %w", err)
	}

	// check if maxLastModified should be updated
	if s.maxLastModified.Before(objectAttrs.Updated) {
		s.maxLastModified = objectAttrs.Updated
	}

	p := position.Position{
		Key:       objectAttrs.Name,
		Type:      position.TypeSnapshot,
		Timestamp: s.maxLastModified,
	}

	// create the record
	output := sdk.Record{
		Metadata: map[string]string{
			"content-type": objectAttrs.ContentType,
		},
		Position:  p.ToRecordPosition(),
		Payload:   sdk.RawData(ObjectData),
		Key:       sdk.RawData(objectAttrs.Name),
		CreatedAt: objectAttrs.Updated,
	}

	return output, nil
}
func (s *SnapshotIterator) Stop() {
	// nothing to stop
}
