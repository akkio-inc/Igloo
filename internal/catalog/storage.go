package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	iceio "github.com/apache/iceberg-go/io"
	// Registers the s3://, gs:// and abfss:// file systems.
	"github.com/apache/iceberg-go/io/gocloud"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/akkio-inc/igloo/internal/awscreds"
	"github.com/akkio-inc/igloo/internal/azurecreds"
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
		} else {
			useSharedAzureIdentity()
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
		} else {
			useSharedCredentials(s.S3.Region)
		}
	}
	return props
}

// sharedCredentials sets the process's one provider of AWS credentials for
// S3 file systems without keys of their own, once (see package awscreds).
var sharedCredentials sync.Once

// useSharedCredentials has iceberg-go's S3 file systems without keys of
// their own take their credentials from one provider for the process, of
// the default chain resolved for region, which refreshes them in the
// background (see package awscreds), rather than each from the instance
// metadata service on its first request, under its statement's deadline.
func useSharedCredentials(region string) {
	sharedCredentials.Do(func() {
		gocloud.SetDefaultCredentials(awscreds.New(func(ctx context.Context) (aws.CredentialsProvider, error) {
			cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
			if err != nil {
				return nil, err
			}
			return cfg.Credentials, nil
		}, slog.Default().With("component", "aws-credentials")))
	})
}

// sharedAzureIdentity sets the process's one provider of Azure tokens for Azure file
// systems without keys, once (see package azurecreds).
var sharedAzureIdentity sync.Once

// useSharedAzureIdentity has iceberg-go's Azure file systems without keys take their
// tokens from one provider for the process, rather than each from the instance
// metadata service, which allows a VM few requests a second.
func useSharedAzureIdentity() {
	sharedAzureIdentity.Do(func() {
		log := slog.Default().With("component", "azure-credentials")
		p, err := azurecreds.Shared(log)
		if err != nil {
			log.Warn("no Azure identity for Igloo's own clients; each makes its own", "err", err)
			return
		}
		gocloud.SetDefaultAzureCredential(p)
	})
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
