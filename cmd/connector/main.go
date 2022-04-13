package main

import (
	source "github.com/conduitio/conduit-connector-google-cloudstorage/source"
	sdk "github.com/conduitio/conduit-connector-sdk"

	connector "github.com/conduitio/conduit-connector-google-cloudstorage"
)

func main() {
	sdk.Serve(connector.Specification, source.NewSource, nil)
}
