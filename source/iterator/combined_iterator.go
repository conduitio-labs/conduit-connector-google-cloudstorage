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
	logger := sdk.Logger(ctx).With().Str("Method", "NewCombinedIterator").Logger()
	logger.Trace().Msg("Starting the NewCombinedIterator")

	var err error
	c := &CombinedIterator{
		bucket:        bucket,
		pollingPeriod: pollingPeriod,
		client:        client,
	}

	switch p.Type {
	case position.TypeSnapshot:
		logger.Trace().Msg("Starting creating a New Snaphot iterator")

		if len(p.Key) != 0 {
			logger.Warn().Msgf("got position: %v, snapshot will be restarted from the beginning of the bucket\n", p)
		}

		p = position.Position{} // always start snapshot from the beginning, so position is nil
		c.snapshotIterator, err = NewSnapshotIterator(ctx, bucket, client, p)
		if err != nil {
			logger.Error().Stack().Err(err).Msg("Error while creating New Snaphot iterator")
			return nil, fmt.Errorf("could not create the snapshot iterator: %w", err)
		}

		logger.Trace().Msg("Sucessfully created the New Snaphot iterator")

	case position.TypeCDC:
		logger.Trace().Msg("Starting creating a CDC iterator")

		c.cdcIterator, err = NewCDCIterator(ctx, bucket, pollingPeriod, client, p.Timestamp, p.Key)
		if err != nil {
			logger.Error().Stack().Err(err).Msg("Error while creating CDC iterator")

			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}

		logger.Trace().Msg("Sucessfully created the CDC iterator")

	default:
		return nil, fmt.Errorf("invalid position type (%d)", p.Type)
	}
	return c, nil
}

func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "CombinedIterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Starting The Next Method of Combined Iterator")

	switch {
	case c.snapshotIterator != nil:
		logger.Trace().Msg("Using the Snapshot Iterator for pulling the data")

		record, err := c.snapshotIterator.Next(ctx)
		if err == googleIterator.Done {
			logger.Trace().Msg("Switching from snapshot to the CDC iterator")

			err := c.switchToCDCIterator(ctx)
			if err != nil {
				logger.Error().Stack().Err(err).Msg("Error Switching from snapshot to the CDC iterator")
				return sdk.Record{}, err
			}
			return c.Next(ctx)
		} else if err != nil {
			logger.Error().Stack().Err(err).Msg("Error During the snapshot iterator")

			return sdk.Record{}, err
		}
		logger.Trace().Msg("Successfully return the record from the snapshot iterator")
		return record, nil

	case c.cdcIterator != nil:
		logger.Trace().Msg("Using the CDC Iterator for pulling the data")
		return c.cdcIterator.Next(ctx)
	default:
		logger.Error().Msg("Both the itertors are not initailsed")
		return sdk.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) switchToCDCIterator(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Class", "CombinedIterator").Str("Method", "switchToCDCIterator").Logger()
	logger.Trace().Msg("Starting switching to the CDC iterator")

	var err error
	timestamp := c.snapshotIterator.maxLastModified
	// zero timestamp means nil position (empty bucket), so start detecting actions from now
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	logger.Trace().Msgf("Create the CDC iterator with %v polling period, %v max last modified time", c.pollingPeriod, timestamp)
	c.cdcIterator, err = NewCDCIterator(ctx, c.bucket, c.pollingPeriod, c.client, timestamp, c.snapshotIterator.maxKeyLastModified)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while creating the CDC interator")
		return fmt.Errorf("could not create cdc iterator: %w", err)
	}
	c.snapshotIterator = nil

	logger.Trace().Msg("Successfully switched to the CDC iterator")
	return nil
}

func (c *CombinedIterator) Stop(ctx context.Context) {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop(ctx)
	}
}
