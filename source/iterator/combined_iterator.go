package iterator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googleIterator "google.golang.org/api/iterator"
)

type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	bucket        string
	pollingPeriod time.Duration
	client        *storage.Client
}

var ErrDone = errors.New("no more items in combined iterator")

func NewCombinedIterator(ctx context.Context, bucket string, pollingPeriod time.Duration, client *storage.Client, p position.Position) (*CombinedIterator, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("NewCombinedIterator: Starting the NewCombinedIterator")

	var err error
	c := &CombinedIterator{
		bucket:        bucket,
		pollingPeriod: pollingPeriod,
		client:        client,
	}

	switch p.Type {
	case position.TypeSnapshot:
		logger.Info().Msg("NewCombinedIterator: Starting creating a New Snaphot iterator")

		if len(p.Key) != 0 {
			logger.Warn().Msgf("NewCombinedIterator: got position: %s, snapshot will be restarted from the beginning of the bucket\n", p.ToRecordPosition())
		}

		p = position.Position{} // always start snapshot from the beginning, so position is nil
		c.snapshotIterator, err = NewSnapshotIterator(ctx, bucket, client, p)
		if err != nil {
			logger.Error().Msgf("NewCombinedIterator: Error while creating New Snaphot iterator: %v", err)
			return nil, fmt.Errorf("could not create the snapshot iterator: %w", err)
		}

		logger.Info().Msg("NewCombinedIterator: Sucessfully created the New Snaphot iterator")

	case position.TypeCDC:
		logger.Info().Msg("NewCombinedIterator: Starting creating a CDC iterator")

		c.cdcIterator, err = NewCDCIterator(ctx, bucket, pollingPeriod, client, p.Timestamp)
		if err != nil {
			logger.Error().Msgf("NewCombinedIterator: Error while creating CDC iterator: %v", err)

			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}

		logger.Info().Msg("NewCombinedIterator: Sucessfully created the CDC iterator")

	default:
		return nil, fmt.Errorf("invalid position type (%d)", p.Type)
	}
	return c, nil
}

func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("CombinedIterator Next: Starting The Next Method of Combined Iterator")

	switch {
	case c.snapshotIterator != nil:
		logger.Info().Msg("CombinedIterator Next: Using the Snapshot Iterator for pulling the data")

		record, err := c.snapshotIterator.Next(ctx)
		if err == googleIterator.Done {
			logger.Info().Msg("CombinedIterator Next: Switching from snapshot to the CDC iterator")

			err := c.switchToCDCIterator(ctx)
			if err != nil {
				logger.Error().Msgf("CombinedIterator Next: Error Switching from snapshot to the CDC iterator: %v", err)
				return sdk.Record{}, err
			}
			return c.Next(ctx)
		} else if err != nil {
			logger.Error().Msgf("CombinedIterator Next: Error During the snapshot iterator: %v", err)

			return sdk.Record{}, err
		}
		logger.Info().Msg("CombinedIterator Next: Successfully return the record from the snapshot iterator")
		return record, nil

	case c.cdcIterator != nil:
		logger.Info().Msg("CombinedIterator Next: Using the CDC Iterator for pulling the data")
		return c.cdcIterator.Next(ctx)
	default:
		logger.Error().Msg("CombinedIterator Next: Both the itertors are not initailsed")
		return sdk.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) switchToCDCIterator(ctx context.Context) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("CombinedIterator switchToCDCIterator: Starting switching to the CDC iterator")

	var err error
	timestamp := c.snapshotIterator.maxLastModified
	// zero timestamp means nil position (empty bucket), so start detecting actions from now
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	logger.Info().Msgf("CombinedIterator switchToCDCIterator: Create the CDC iterator with %v polling period, %v max last modified time", c.pollingPeriod, timestamp)
	c.cdcIterator, err = NewCDCIterator(ctx, c.bucket, c.pollingPeriod, c.client, timestamp)
	if err != nil {
		logger.Error().Msgf("CombinedIterator switchToCDCIterator: Error while creating the CDC interator %v", err)
		return fmt.Errorf("could not create cdc iterator: %w", err)
	}
	c.snapshotIterator = nil

	logger.Info().Msg("CombinedIterator switchToCDCIterator: Successfully switched to the CDC iterator")
	return nil
}

func (c *CombinedIterator) Stop() {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop()
	}
}
