# Conduit Connector Google Cloud Storage

### General
The Google Cloud Storage connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides source GCS connectors.

### How to build it
Run `make`.

### Testing

Run `make test` with optional `GOTEST_FLAGS` set to run all the tests. You must set the environment variables (`GCP_ServiceAccount_Key`
, `GCP_ProjectID`, `GCP_Bucket`)
before you run all the tests. If not set, the tests that use these variables will be ignored.

Cases/Scenarios which are dependent on GCS Error response and where these can't be reproducible are ignored in the test cases.
 
Integration test cases are written to cover all of the scenarios of snapshot and cdc iterators except when tomb dead as it happens only in the above mentioned case.
 
## GCS Source

The Google Cloud Storage Source Connector connects to a GCS bucket with the provided configurations, using serviceAccountKey(`JSON`) and bucket(`BucketName`) details . Then will call `Configure` to parse the
configurations. After that, the
`Open` method is called to make sure the bucket exists and to start the connection from the provided position. If the bucket doesn't exist, or the permissions fail, then an error will occur.

### Change Data Capture (CDC)

This connector implements CDC features for GCS by scanning the bucket for changes every
`pollingPeriod` and detecting any change that happened after a certain timestamp. These changes (update, delete, insert)
are then inserted into a buffer that is checked on each Read request.

* To capture "delete" actions, the GCS bucket versioning must be enabled.
* To capture "insert" or "update" actions, the bucket versioning doesn't matter.

If the object as "N" versions in a bucket only the live version is considered as a Record.

#### Position Handling

Here the position is constructed using the below custom type which includes the object key which was last read (Used to compare lexicographically when the timestamp is equal),Timestamp of the object concerned event, and type of the reading mode.

```
type Position struct {
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp"`
	Type      Type      `json:"type"`
}
```

The connector goes through two reading modes.

* Snapshot mode (Value 0): which loops through the GCS bucket and returns the objects that are already in there. The _position type_ during this mode is 0. which makes the connector know at what mode it is and what object it last
 read. The _position Timestamp_ will be used when changing to CDC mode, the iterator will capture changes that
 happened after that.

* CDC mode: (Value 1) this mode iterates through the GCS bucket every `pollingPeriod` and captures new actions made on the bucket.
 the _Position Type_ during this mode is 1. This position is used to return only the
 actions with a _Position Timestamp_ higher than the last record returned even if the timestamp got matched then the decision would be based on lexicographical comparison, which will ensure that no duplications are in
 place.

 The CDC mode will start after a single action(Insert/Update/Delete) completed post snapshot.
 
 Here the CDC mode works on the default google iterator provided by the GO SDK with a configurable pollingPeriod and it doesn't support the notifications through pub/sub because making it seperate service would be more flexible.

### Record Keys

The GCS object key uniquely identifies the objects in an Google Cloud Storage bucket, which is why a record key is the key read from
the GCS bucket.


### Configuration

The config passed to `Configure` can contain the following fields.

| name                  | description                                                                            | required  | example             |
|-----------------------|----------------------------------------------------------------------------------------|-----------|---------------------|
| `serviceAccountKey` | GCP service account key in JSON                                              | yes       | "{\\"key\\":\\"value\\",....}" |
| `bucket`          | the GCS bucket name                                                                 | yes       | "bucket_name"       |
| `pollingPeriod`       | polling period for the CDC mode, formatted as a time.Duration string. default is "1s"  | no        | "2s", "500ms"       |

When testing from the swagger or any rest client provide the stringified value in `serviceAccountKey`
Example: console.log(JSON.stringify(JSON.stringify(key file content))) run in the JavaScript to get the preferred stringified value because wrapping JSON around the single quotes is not accepted in swagger OR
Refer to the above example provided in the table.

### Known Limitations

* If a pipeline restarts during the snapshot, then the connector will start scanning the objects from the beginning of
the bucket, which could result in duplications.
