package catalog

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	// Registers the s3://, gs:// and abfss:// file systems.
	_ "github.com/apache/iceberg-go/io/gocloud"

	"github.com/akkio-inc/igloo/internal/config"
)

// ioProperties gives iceberg-go the same object storage credentials DuckDB
// uses. Leaving keys out makes iceberg-go use each cloud's credential chain
// (IAM roles for service accounts, workload identity, managed identity).
func ioProperties(s config.Storage) map[string]string {
	props := map[string]string{}
	switch s.Type {
	case config.StorageAzure:
		// Without a key, iceberg-go uses the credential chain only if no
		// shared-key account name is set either.
		if s.Azure.AccountKey != "" {
			props[iceio.ADLSSharedKeyAccountName] = s.Azure.Account
			props[iceio.ADLSSharedKeyAccountKey] = s.Azure.AccountKey
		}
	case config.StorageGCS:
		// DuckDB reaches GCS with an HMAC key through the S3 API;
		// iceberg-go uses the GCS API and application default credentials.
	default:
		props[iceio.S3Region] = s.S3.Region
		if s.S3.Endpoint != "" {
			scheme := "https://"
			if !s.S3.UseSSL {
				scheme = "http://"
			}
			props[iceio.S3EndpointURL] = scheme + s.S3.Endpoint
		}
		if s.S3.URLStyle == "vhost" {
			props[iceio.S3ForceVirtualAddressing] = "true"
		} else {
			props[iceio.S3ForceVirtualAddressing] = "false"
		}
		if s.S3.AccessKeyID != "" {
			props[iceio.S3AccessKeyID] = s.S3.AccessKeyID
			props[iceio.S3SecretAccessKey] = s.S3.SecretAccessKey
		}
	}
	return props
}

// ownCredentials makes iceberg-go use Igloo's storage settings, as DuckDB
// does, rather than what the catalog hands out. iceberg-go asks for vended
// credentials on every call, and lets the per-table config in catalog
// responses override its own settings; Lakekeeper's always turns on remote
// request signing, which iceberg-go cannot do. So it drops the request
// header and strips "config" and "storage-credentials" from responses.
type ownCredentials struct{}

func (ownCredentials) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Del("X-Iceberg-Access-Delegation")
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		return resp, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close() //nolint:errcheck
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(stripTableConfig(body)))
	resp.ContentLength = -1
	resp.Header.Del("Content-Length")
	return resp, nil
}

// stripTableConfig removes the catalog's per-table client settings from a
// JSON response, leaving anything else untouched.
func stripTableConfig(body []byte) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil {
		return body
	}
	_, hasConfig := obj["config"]
	_, hasCreds := obj["storage-credentials"]
	if !hasConfig && !hasCreds {
		return body
	}
	delete(obj, "config")
	delete(obj, "storage-credentials")
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}
