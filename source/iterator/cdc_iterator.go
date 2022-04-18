package iterator

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"cloud.google.com/go/storage"
	"github.com/conduitio/conduit-connector-google-cloudstorage/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"gopkg.in/tomb.v2"
)

// CDCIterator scans the bucket periodically and detects changes made to it.
type CDCIterator struct {
	bucket        string
	client        *storage.Client
	buffer        chan sdk.Record
	ticker        *time.Ticker
	lastModified  time.Time
	caches        chan []CacheEntry
	isTruncated   bool
	nextKeyMarker *string
	tomb          *tomb.Tomb
}

type CacheEntry struct {
	key          string
	lastModified time.Time
	deleteMarker bool
}

// NewCDCIterator returns a CDCIterator and starts the process of listening to changes every pollingPeriod.
func NewCDCIterator(ctx context.Context, bucket string, pollingPeriod time.Duration, client *storage.Client, from time.Time) (*CDCIterator, error) {
	logger := sdk.Logger(ctx)
	logger.Info().Msg("NewCDCIterator: Starting the NewCDCIterator")

	cdc := CDCIterator{
		bucket:        bucket,
		client:        client,
		buffer:        make(chan sdk.Record, 1),
		caches:        make(chan []CacheEntry),
		ticker:        time.NewTicker(pollingPeriod),
		isTruncated:   true,
		nextKeyMarker: nil,
		tomb:          &tomb.Tomb{},
		lastModified:  from,
	}

	// start listening to changes
	cdc.tomb.Go(cdc.startCDC)
	cdc.tomb.Go(cdc.flush)

	return &cdc, nil
}

// Next returns the next record from the buffer.
func (w *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-w.buffer:
		return r, nil
	case <-w.tomb.Dead():
		return sdk.Record{}, w.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, ErrDone
	}
}

func (w *CDCIterator) Stop() {
	// stop the two goRoutines
	w.ticker.Stop()
	w.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// startCDC scans the GoogleCloudStorage bucket every polling period for changes
// only detects the changes made after the w.lastModified
func (w *CDCIterator) startCDC() error {
	defer close(w.caches)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case <-w.ticker.C: // detect changes every polling period
			cache := make([]CacheEntry, 0, 1000)
			it := w.client.Bucket(w.bucket).Objects(w.tomb.Context(nil), nil)
			for {
				objectAttrs, err := it.Next()
				if err == iterator.Done {
					fmt.Fprint(os.Stdout, "startCDC: iterator.Done\n")
					break
				} else if err != nil {
					return fmt.Errorf("startCDC: Bucket(%q).Objects: %v\n", w.bucket, err)
				}
				if !(objectAttrs.Updated.After(w.lastModified) || objectAttrs.Deleted.After(w.lastModified)) {
					fmt.Fprintf(os.Stdout, "startCDC: Object %s modified before lastmodified time: %v, Object Updated at %v,Object Deleted at %v\n", objectAttrs.Name, w.lastModified, objectAttrs.Updated, objectAttrs.Deleted)
					continue
				}
				entry := CacheEntry{
					key:          objectAttrs.Name,
					lastModified: objectAttrs.Updated,
					deleteMarker: !objectAttrs.Deleted.IsZero(),
				}
				fmt.Fprintf(os.Stdout, "startCDC: Object %s added to the cache entry with updated at %v,deleted at %v, CDC .lastModified: %v\n", objectAttrs.Name, objectAttrs.Updated, objectAttrs.Deleted, w.lastModified)
				cache = append(cache, entry)
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
				var output sdk.Record

				if entry.deleteMarker {
					output = w.createDeletedRecord(entry)
				} else {

					reader, err := w.client.Bucket(w.bucket).Object(entry.key).NewReader(w.tomb.Context(nil))

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

// createRecord creates the record for the object fetched from GoogleCloudStorage (for updates and inserts)
func (w *CDCIterator) createRecord(entry CacheEntry, reader *storage.Reader) (sdk.Record, error) {
	// build record
	defer reader.Close()
	rawBody, err := ioutil.ReadAll(reader)
	if err != nil {
		return sdk.Record{}, err
	}
	p := position.Position{
		Key:       entry.key,
		Timestamp: entry.lastModified,
		Type:      position.TypeCDC,
	}

	return sdk.Record{
		Metadata: map[string]string{
			"content-type": reader.Attrs.ContentType,
		},
		Position:  p.ToRecordPosition(),
		Payload:   sdk.RawData(rawBody),
		Key:       sdk.RawData(entry.key),
		CreatedAt: reader.Attrs.LastModified,
	}, nil
}

// createDeletedRecord creates the record for the object fetched from GoogleCloudStorage bucket (for deletes)
func (w *CDCIterator) createDeletedRecord(entry CacheEntry) sdk.Record {
	p := position.Position{
		Key:       entry.key,
		Timestamp: entry.lastModified,
		Type:      position.TypeCDC,
	}
	return sdk.Record{
		Metadata: map[string]string{
			"action": "delete",
		},
		Position:  p.ToRecordPosition(),
		Key:       sdk.RawData(entry.key),
		CreatedAt: entry.lastModified,
	}
}
