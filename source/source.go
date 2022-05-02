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
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/config"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/iterator"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/option"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource
	config           config.SourceConfig
	client           *storage.Client
	combinedIterator *iterator.CombinedIterator
}

func NewSource() sdk.Source {
	return &Source{}
}

// Configure parses and stores the configurations
// returns an error in case of invalid config
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Configure").Logger()
	logger.Trace().Msg("Starting Configuring the Source Connector...")

	sourceConfig, err := config.ParseSourceConfig(ctx, cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While parsing the Source Config")
		return err
	}

	s.config = sourceConfig

	s.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(s.config.GoogleCloudServiceAccountKey)))
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Creating the Storage Client")
		return err
	}

	err = s.bucketExists(ctx, s.config.GoogleCloudStorageBucket)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Checking the Bucket Existence")
		return err
	}
	logger.Trace().Msg("Successfully completed configuring the source connector...")
	return nil
}

// Open prepare the plugin to start sending records from the given position
func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Open").Logger()
	logger.Trace().Msg("Starting Open the Source Connector...")

	p, err := position.ParseRecordPosition(pos)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while parsing the record position")
		return err
	}

	s.combinedIterator, err = iterator.NewCombinedIterator(ctx, s.config.GoogleCloudStorageBucket, s.config.PollingPeriod, s.client, p)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while create a combined iterator")
		return fmt.Errorf("couldn't create a combined iterator: %w", err)
	}
	logger.Trace().Msg("Successfully completed Open of the Source Connector...")
	return nil
}

// Read gets the next object from the GCS bucket
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Read").Logger()
	logger.Trace().Msg("Starting Read of the Source Connector...")

	r, err := s.combinedIterator.Next(ctx)

	if err == iterator.ErrDone {
		logger.Debug().Msg("combined iterator fetched complete records")
		return sdk.Record{}, sdk.ErrBackoffRetry
	} else if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while fetching the records")
		return sdk.Record{}, err
	}
	logger.Trace().Msg("Successfully completed Read of the Source Connector...")
	return r, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("Class", "Source").Str("Method", "Ack").Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Teardown").Logger()
	logger.Trace().Msg("Starting Teardown the Source Connector...")

	if s.client != nil {
		err := s.client.Close()
		if err != nil {
			logger.Error().Stack().Err(err).Msg("Error While Closing the Storage Client")
			return err
		}
	}

	if s.combinedIterator != nil {
		s.combinedIterator.Stop()
		s.combinedIterator = nil
	}

	logger.Trace().Msg("Successfully Teardown the Source Connector...")
	return nil
}

func (s *Source) bucketExists(ctx context.Context, bucketName string) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "bucketExists").Logger()
	logger.Trace().Msg("Starting Checking the Bucket exist ...")

	// check if the bucket exists
	_, err := s.client.Bucket(bucketName).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		logger.Error().Stack().Err(err).Msg("Error Bucker Not Exist")
		return err
	}
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while checking the bucket exists")
		return err
	}
	return nil
}
