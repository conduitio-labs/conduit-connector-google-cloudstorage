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

type Source struct {
	sdk.UnimplementedSource
	config           config.SourceConfig
	client           *storage.Client
	combinedIterator *iterator.CombinedIterator
	//lastPositionRead sdk.Position
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source Configure: Starting Configuring the Source Connector...")

	config, err := config.ParseSourceConfig(ctx, cfg)
	if err != nil {
		logger.Error().Msgf("Source Configure: Error While parsing the Source Config: %v", err)
		return err
	}

	s.config = config

	s.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(s.config.GoogleCloudServiceAccountKey)))
	if err != nil {
		logger.Error().Msgf("Source Configure: Error While Creating the Storage Client: %v", err)
		return err
	}

	err = s.bucketExists(ctx, s.config.GoogleCloudStorageBucket)
	if err != nil {
		logger.Error().Msgf("Source Configure: Error While Checking the Bucket Existence: %v", err)
		return err
	}

	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source Open: Starting Open the Source Connector...")

	p, err := position.ParseRecordPosition(pos)
	if err != nil {
		logger.Error().Msgf("Source Open: Error while parsing the record position: %v", err)
		return err
	}

	s.combinedIterator, err = iterator.NewCombinedIterator(ctx, s.config.GoogleCloudStorageBucket, s.config.PollingPeriod, s.client, p)
	if err != nil {
		logger.Error().Msgf("Source Open: Error while create a combined iterator: %v", err)
		return fmt.Errorf("couldn't create a combined iterator: %w", err)
	}
	logger.Info().Msg("Source Open: Successfully completed Open of the Source Connector...")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source Read: Starting Read of the Source Connector...")

	r, err := s.combinedIterator.Next(ctx)

	if err == iterator.ErrDone {
		logger.Error().Msg("Source Read: combined iterator fetched complete records")
		return sdk.Record{}, sdk.ErrBackoffRetry
	} else if err != nil {
		logger.Error().Msgf("Source Read: Error while fetching the records: %v", err)
		return sdk.Record{}, err
	}
	logger.Info().Msg("Source Read: Successfully completed Read of the Source Connector...")
	return r, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source Teardown: Starting Teardown the Source Connector...")

	if s.client != nil {
		err := s.client.Close()
		if err != nil {
			logger.Error().Msgf("Source Teardown: Error While Closing the Storage Client: %v", err)
			return err
		}
	}

	if s.combinedIterator != nil {
		s.combinedIterator.Stop()
		s.combinedIterator = nil
	}

	logger.Info().Msg("Source Teardown: Successfully Teardown the Source Connector...")
	return nil
}

func (s *Source) bucketExists(ctx context.Context, bucketName string) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source bucketExists: Starting Checking the Bucket exist ...")

	_, err := s.client.Bucket(bucketName).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		logger.Error().Msg("Source bucketExists: Error Bucker Not Exist")
		return err
	}
	if err != nil {
		logger.Error().Msgf("Source bucketExists: Error while checking the bucket exists: %v", err)
		return err
	}
	return nil
}
