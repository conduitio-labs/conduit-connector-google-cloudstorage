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
	"testing"

	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/source/config"
	"github.com/matryer/is"
)

func TestSource_Configure_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	con := Source{}

	err := con.Configure(context.Background(), map[string]string{
		config.ConfigServiceAccountKey: "testKey",
	})

	is.True(err != nil)
	is.Equal(err.Error(), `error parsing config: config invalid: error validating "bucket": required parameter is not provided`)
}

func TestTeardownSource_NoOpen(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	con := NewSource()
	err := con.Teardown(context.Background())

	is.NoErr(err)
}

func TestSource_ReadWithNilIterator(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	con := NewSource()
	_, err := con.Read(context.Background())

	is.Equal(err.Error(), "iterator is not initialized")
}
