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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"gopkg.in/tomb.v2"
)

const (
	MetadataContentType = "gcs.contentType"
)

// CDCIterator scans the bucket periodically and detects changes made to it.
type CDCIterator struct {
	bucket        string
	client        *storage.Client
	buffer        chan opencdc.Record
	ticker        *time.Ticker
	lastModified  time.Time
	caches        chan []CacheEntry
	isTruncated   bool
	nextKeyMarker *string
	tomb          *tomb.Tomb
	lastEntryKey  string
}

type CacheEntry struct {
	key          string
	lastModified time.Time
	deleteMarker bool
}

// NewCDCIterator returns a CDCIterator and starts the process of listening to changes every pollingPeriod.
func NewCDCIterator(ctx context.Context, bucket string, pollingPeriod time.Duration, client *storage.Client, from time.Time, fromKey string) (*CDCIterator, error) {
	sdk.Logger(ctx).Trace().Str("Method", "NewCDCIterator").Msg("NewCDCIterator: Starting the NewCDCIterator")

	cdc := CDCIterator{
		bucket:        bucket,
		client:        client,
		buffer:        make(chan opencdc.Record, 1),
		caches:        make(chan []CacheEntry),
		ticker:        time.NewTicker(pollingPeriod),
		isTruncated:   true,
		nextKeyMarker: nil,
		tomb:          &tomb.Tomb{},
		lastModified:  from,
		lastEntryKey:  fromKey,
	}

	// start listening to changes
	cdc.tomb.Go(cdc.startCDC)
	cdc.tomb.Go(cdc.flush)

	return &cdc, nil
}

// Next returns the next record from the buffer.
func (w *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case r := <-w.buffer:
		return r, nil
	case <-w.tomb.Dead():
		return opencdc.Record{}, w.tomb.Err()
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	default:
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
}

func (w *CDCIterator) Stop(ctx context.Context) {
	logger := sdk.Logger(ctx).With().Str("Class", "CDCIterator").Str("Method", "Stop").Logger()
	logger.Trace().Msg("Starting Stop Method of CDCIterator ...")

	// stop the two goRoutines
	w.ticker.Stop()
	w.tomb.Kill(errors.New("cdc iterator is stopped"))
	<-w.tomb.Dead()

	logger.Trace().Msg("Successfully Stopped the CDCIterator ...")
}

// startCDC scans the GoogleCloudStorage bucket every polling period for changes
// only detects the changes made after the w.lastModified.
func (w *CDCIterator) startCDC() error {
	defer close(w.caches)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case <-w.ticker.C: // detect changes every polling period
			query := &storage.Query{Versions: true}
			err := query.SetAttrSelection([]string{"Updated", "Deleted", "Name"})
			if err != nil {
				return fmt.Errorf("startCDC:Error while query SetAttrSelection Bucket(%q).Objects: %w", w.bucket, err)
			}
			it := w.client.Bucket(w.bucket).Objects(w.tomb.Context(nil), query) //nolint:staticcheck // SA1012 tomb expects nil

			cache, err := w.fetchCacheEntries(it)
			if err != nil {
				return fmt.Errorf("startCDC:Error while fetchCacheEntries Bucket(%q).Objects: %w", w.bucket, err)
			}

			if len(cache) == 0 {
				continue
			}
			sort.Slice(cache, func(i, j int) bool {
				return cache[i].lastModified.Before(cache[j].lastModified)
			})

			select {
			case w.caches <- cache:
				w.lastModified = cache[len(cache)-1].lastModified
				w.lastEntryKey = cache[len(cache)-1].key
				// worked fine
			case <-w.tomb.Dying():
				return w.tomb.Err()
			}
		}
	}
}

// flush: go routine that will get the objects from the bucket and flush the detected changes into the buffer.
func (w *CDCIterator) flush() error {
	defer close(w.buffer)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case cache := <-w.caches:
			for i := 0; i < len(cache); i++ {
				entry := cache[i]
				var output opencdc.Record

				if entry.deleteMarker {
					var err error
					if output, err = w.createDeletedRecord(entry); err != nil {
						return err
					}
				} else {
					reader, err := w.client.Bucket(w.bucket).Object(entry.key).NewReader(w.tomb.Context(nil)) //nolint:staticcheck // SA1012 tomb expects nil
					if err != nil {
						return err
					}
					output, err = w.createRecord(entry, reader)
					if err != nil {
						return err
					}
				}

				select {
				case w.buffer <- output:
					// worked fine
				case <-w.tomb.Dying():
					return w.tomb.Err()
				}
			}
		}
	}
}

// fetchCacheEntries create the slice of entries/objects based upon the lastmodified time so they should be part of CDC.
func (w *CDCIterator) fetchCacheEntries(it *storage.ObjectIterator) ([]CacheEntry, error) {
	cache := make([]CacheEntry, 0, 1000)
	for {
		objectAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			// Done iterating all the objects in the bucket
			break
		} else if err != nil {
			return cache, fmt.Errorf("startCDC: Bucket(%q).Objects: %w", w.bucket, err)
		}
		if w.checkLastModified(objectAttrs) {
			// Object got modified before lastmodified time
			continue
		}
		entry := CacheEntry{
			key:          objectAttrs.Name,
			lastModified: objectAttrs.Updated,
		}
		if !objectAttrs.Deleted.IsZero() {
			entry.deleteMarker = true
			entry.lastModified = objectAttrs.Deleted
		}
		if len(cache) > 0 && cache[len(cache)-1].key == entry.key {
			cache[len(cache)-1] = entry
		} else {
			cache = append(cache, entry)
		}
		// Object is added to the cache entry
	}
	return cache, nil
}

// createRecord creates the record for the object fetched from GoogleCloudStorage (for updates and inserts).
func (w *CDCIterator) createRecord(entry CacheEntry, reader *storage.Reader) (opencdc.Record, error) {
	// build record
	defer reader.Close()
	rawBody, err := io.ReadAll(reader)
	if err != nil {
		return opencdc.Record{}, err
	}
	p, err := json.Marshal(position.Position{
		Key:       entry.key,
		Timestamp: entry.lastModified,
		Type:      position.TypeCDC,
	})
	if err != nil {
		return opencdc.Record{}, err
	}

	return sdk.Util.Source.NewRecordCreate(
		p,
		map[string]string{
			MetadataContentType: reader.Attrs.ContentType,
		},
		opencdc.RawData(entry.key),
		opencdc.RawData(rawBody),
	), nil
}

// createDeletedRecord creates the record for the object fetched from GoogleCloudStorage bucket (for deletes).
func (w *CDCIterator) createDeletedRecord(entry CacheEntry) (opencdc.Record, error) {
	p, err := json.Marshal(position.Position{
		Key:       entry.key,
		Timestamp: entry.lastModified,
		Type:      position.TypeCDC,
	})
	if err != nil {
		return opencdc.Record{}, err
	}

	return sdk.Util.Source.NewRecordDelete(
		p,
		map[string]string{},
		opencdc.RawData(entry.key),
		nil,
	), nil
}

func (w *CDCIterator) checkLastModified(objectAttrs *storage.ObjectAttrs) bool {
	return !((objectAttrs.Updated.After(w.lastModified) || objectAttrs.Deleted.After(w.lastModified)) || // check if the object is updated or deleted after the last modified time
		((objectAttrs.Updated.Equal(w.lastModified) || objectAttrs.Deleted.Equal(w.lastModified)) && strings.Compare(objectAttrs.Name, w.lastEntryKey) > 0)) // If the object updated or deleted at last modified time then lexicographically check is done
}
