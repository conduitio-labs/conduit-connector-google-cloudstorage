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

package position

import (
	"bytes"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

func Test_ParseRecordPosition(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      opencdc.Position
		out     Position
	}{
		{
			name:    "zero position",
			wantErr: false,
			in:      []byte("{\"key\":\"test\",\"timestamp\":\"0001-01-01T00:00:00Z\",\"type\":0}"),
			out: Position{
				Key:  "test",
				Type: TypeSnapshot,
			},
		},
		{
			name:    "nil position returns empty Position with default values",
			wantErr: false,
			in:      nil,
			out:     Position{},
		},
		{
			name:    "wrong position format returns error",
			wantErr: true,
			in:      []byte("test0"),
			out:     Position{},
		},
		{
			name:    "cdc type position",
			wantErr: false,
			in:      []byte("{\"key\":\"test\",\"timestamp\":\"0001-01-01T00:00:00Z\",\"type\":1}"),
			out: Position{
				Key:  "test",
				Type: TypeCDC,
			},
		},
		{
			name:    "invalid timestamp returns error",
			wantErr: true,
			in:      []byte("{\"key\":\"test\",\"timestamp\":\"invalid\",\"type\":1}"),
			out:     Position{},
		},
		{
			name:    "invalid type",
			wantErr: true,
			in:      []byte("{\"key\":\"test\",\"timestamp\":\"0001-01-01T00:00:00Z\",\"type\":2}"),
			out:     Position{},
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParseRecordPosition(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRecordPosition() name: %s\n error = %v, wantErr %v", tt.name, err, tt.wantErr)
			} else if p != tt.out {
				t.Errorf("ParseRecordPosition(): name: %s\n Got : %v,Expected : %v", tt.name, p, tt.out)
			}
		})
	}
}

func Test_ConvertSnapshotPositionToCDC(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      opencdc.Position
		out     opencdc.Position
	}{
		{
			name:    "convert snapshot position to cdc",
			wantErr: false,
			in:      []byte("{\"key\":\"test\",\"timestamp\":\"0001-01-01T00:00:00Z\",\"type\":0}"),
			out:     []byte("{\"key\":\"test\",\"timestamp\":\"0001-01-01T00:00:00Z\",\"type\":1}"),
		},
		{
			name:    "convert invalid snapshot should produce error",
			wantErr: true,
			in:      []byte("s100"),
			out:     []byte(""),
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ConvertToCDCPosition(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertToCDCPosition() name:%s error = %v, wantErr %v", tt.name, err, tt.wantErr)
			} else if !bytes.Equal(p, tt.out) {
				t.Errorf("ConvertToCDCPosition(): name:%s Got : %v,Expected : %v", tt.name, p, tt.out)
			}
		})
	}
}
