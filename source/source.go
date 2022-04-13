package source

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Source struct {
	sdk.UnimplementedSource
	config   config.SourceConfig
	client   *storage.Client
	iterator *storage.ObjectIterator
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
	s.iterator = s.client.Bucket(s.config.GoogleCloudStorageBucket).Objects(ctx, nil)
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	objectAttrs, err := s.iterator.Next()
	if err == iterator.Done {
		return sdk.Record{}, sdk.ErrBackoffRetry
	} else if err != nil {
		return sdk.Record{}, err
	}

	rc, err := s.client.Bucket(s.config.GoogleCloudStorageBucket).Object(objectAttrs.Name).NewReader(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("Object(%q).NewReader: %v", objectAttrs.Name, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("Object(%q).Cant read data from reader: %v", objectAttrs.Name, err)
	}

	position := sdk.Position(fmt.Sprintf("%s_%d", objectAttrs.Name, objectAttrs.Updated.Unix()))

	return sdk.Record{
		Position: position,
		Metadata: map[string]string{
			"content-type": objectAttrs.ContentType,
		},
		Key:       sdk.RawData(objectAttrs.Name),
		Payload:   sdk.RawData(data),
		CreatedAt: objectAttrs.Created,
	}, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Source Teardown: Starting Teardown the Source Connector...")

	if s.client == nil {
		logger.Error().Msg("Source Teardown: Unable to close the storage client which is not even intialized")
		return errors.New("storage client not intialized yet")
	}

	err := s.client.Close()
	if err != nil {
		logger.Error().Msgf("Source Teardown: Error While Closing the Storage Client: %v", err)
		return err
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
