package config

import (
	"context"
	"reflect"
	"testing"
	"time"

	globalConfig "github.com/conduitio/conduit-connector-google-cloudstorage/config"
)

func TestParseGlobalConfig(t *testing.T) {
	var configTests = []struct {
		name        string
		wantErr     bool
		in          map[string]string
		expectedCon SourceConfig
	}{
		{
			name:        "Empty Input",
			wantErr:     true,
			in:          map[string]string{},
			expectedCon: SourceConfig{},
		},
		{
			name:    "Empty Polling period",
			wantErr: false,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
				"bucket":            "bucket",
			},
			expectedCon: SourceConfig{
				Config: globalConfig.Config{
					GoogleCloudStorageBucket:     "bucket",
					GoogleCloudServiceAccountKey: "serviceAccountKey",
				},
				PollingPeriod: time.Duration(1000000000),
			},
		},
		{
			name:    "Polling period which is not parsed",
			wantErr: true,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
				"bucket":            "bucket",
				"pollingPeriod":     "hello",
			},
			expectedCon: SourceConfig{},
		},
		{
			name:    "Polling period which is negative",
			wantErr: true,
			in: map[string]string{
				"serviceAccountKey": "serviceAccountKey",
				"bucket":            "bucket",
				"pollingPeriod":     "-1s",
			},
			expectedCon: SourceConfig{},
		},
	}

	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			actualCon, err := ParseSourceConfig(context.Background(), tt.in)
			if (!(err != nil && tt.wantErr)) && (err != nil || tt.wantErr) {
				t.Errorf("Expected want error is %v but got parse error as : %v ", tt.wantErr, err)
			}
			if !reflect.DeepEqual(tt.expectedCon, actualCon) {
				t.Errorf("Expected Config %v doesn't match with actual config %v ", tt.expectedCon, actualCon)
			}
		})
	}
}
