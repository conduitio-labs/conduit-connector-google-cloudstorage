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

package config

import (
	"context"
	"reflect"
	"testing"
)

func TestParseGlobalConfig(t *testing.T) {
	configTests := []struct {
		name        string
		wantErr     bool
		in          map[string]string
		expectedCon Config
	}{
		{
			name:        "Empty Input",
			wantErr:     true,
			in:          map[string]string{},
			expectedCon: Config{},
		},
		{
			name:    "Input having only service account key",
			wantErr: true,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
			},
			expectedCon: Config{},
		},
		{
			name:    "Input having only service account key",
			wantErr: true,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
			},
			expectedCon: Config{},
		},
		{
			name:    "Input having only bucket name",
			wantErr: true,
			in: map[string]string{
				"bucket": "bucket",
			},
			expectedCon: Config{},
		},
		{
			name:    "Input having both service account key and bucket name",
			wantErr: false,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
				"bucket":            "bucket",
			},
			expectedCon: Config{
				GoogleCloudStorageBucket:     "bucket",
				GoogleCloudServiceAccountKey: "serviceAccountKey",
			},
		},
	}

	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			actualCon, err := ParseGlobalConfig(context.Background(), tt.in)
			if (!(err != nil && tt.wantErr)) && (err != nil || tt.wantErr) {
				t.Errorf("Expected want error is %v but got parse error as : %v ", tt.wantErr, err)
			}
			if !reflect.DeepEqual(tt.expectedCon, actualCon) {
				t.Errorf("Expected Config %v doesn't match with actual config %v ", tt.expectedCon, actualCon)
			}
		})
	}
}
