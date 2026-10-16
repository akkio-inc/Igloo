package catalog

import (
	"encoding/json"
	"maps"
	"testing"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/config"
)

func TestIOProperties(t *testing.T) {
	local := ioProperties(config.Storage{Type: config.StorageS3, S3: config.S3{
		Endpoint: "storage:9000", Region: "us-east-1", URLStyle: "path",
		AccessKeyID: "key", SecretAccessKey: "secret",
	}})
	want := map[string]string{
		iceio.S3EndpointURL: "http://storage:9000", iceio.S3Region: "us-east-1",
		iceio.S3ForceVirtualAddressing: "false",
		iceio.S3AccessKeyID:            "key", iceio.S3SecretAccessKey: "secret",
	}
	if !maps.Equal(local, want) {
		t.Errorf("local S3: got %v", local)
	}

	// Credentials from the chain, virtual-hosted AWS endpoint.
	aws := ioProperties(config.Storage{Type: config.StorageS3, S3: config.S3{Region: "eu-west-1", URLStyle: "vhost", UseSSL: true}})
	if _, ok := aws[iceio.S3AccessKeyID]; ok || aws[iceio.S3ForceVirtualAddressing] != "true" || aws[iceio.S3EndpointURL] != "" {
		t.Errorf("AWS: got %v", aws)
	}

	azureKey := ioProperties(config.Storage{Type: config.StorageAzure, Azure: config.Azure{Account: "acct", AccountKey: "k"}})
	if azureKey[iceio.ADLSSharedKeyAccountName] != "acct" || azureKey[iceio.ADLSSharedKeyAccountKey] != "k" {
		t.Errorf("Azure with key: got %v", azureKey)
	}
	// Workload or managed identity: no shared-key settings at all.
	if azure := ioProperties(config.Storage{Type: config.StorageAzure, Azure: config.Azure{Account: "acct"}}); len(azure) != 0 {
		t.Errorf("Azure without key: got %v", azure)
	}
}

func TestStripTableConfig(t *testing.T) {
	in := `{"metadata-location":"s3://b/m.json","metadata":{"format-version":2},` +
		`"config":{"s3.remote-signing-enabled":"true"},"storage-credentials":[]}`
	var got map[string]any
	if err := json.Unmarshal(stripTableConfig([]byte(in)), &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["config"]; ok {
		t.Error("config kept")
	}
	if _, ok := got["storage-credentials"]; ok {
		t.Error("storage-credentials kept")
	}
	if got["metadata-location"] != "s3://b/m.json" || got["metadata"] == nil {
		t.Errorf("other fields changed: %v", got)
	}

	for _, body := range []string{`{"overrides":{"uri":"x"}}`, `[1,2]`, `not json`} {
		if out := string(stripTableConfig([]byte(body))); out != body {
			t.Errorf("%s changed to %s", body, out)
		}
	}
}
