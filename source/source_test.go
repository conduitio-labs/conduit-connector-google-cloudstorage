package source

import (
	"context"
	"strings"
	"testing"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))

	if !strings.HasPrefix(err.Error(), "missing or empty config") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), map[string]string{"foobar": "foobar"})

	if !strings.HasSuffix(err.Error(), "config value must be set") {
		t.Errorf("expected error to be some config value must be set, got %v", err)
	}
}

func TestTeardownSource_NoOpen(t *testing.T) {
	con := NewSource()
	err := con.Teardown(context.Background())

	if err != nil {
		t.Errorf("expected no error but, got %v", err)
	}
}
