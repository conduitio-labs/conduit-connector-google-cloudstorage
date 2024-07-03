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
	"errors"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-google-cloudstorage/config"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	ctx := context.Background()
	err := con.Configure(ctx, make(map[string]string))

	if !errors.Is(err, config.ErrEmptyConfig) {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
	con := Source{}
	ctx := context.Background()
	err := con.Configure(ctx, map[string]string{"foobar": "foobar"})

	if errors.Is(err, config.RequiredConfigErr(config.ConfigKeyGCPServiceAccountKey)) {
		t.Errorf("expected error serviceAccountKey config value must be set, got %v", err)
	}
}

func TestTeardownSource_NoOpen(t *testing.T) {
	con := NewSource()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error but, got %v", err)
	}
}

func TestSource_ReadWithNilIterator(t *testing.T) {
	con := NewSource()
	_, err := con.Read(context.Background())
	value := "iterator is not initialized"

	if strings.Compare(err.Error(), value) != 0 {
		t.Errorf("Expected: %v but Got: %v", value, err.Error())
	}
}
