package federation

import (
	"context"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestIcebergSigV4 checks that requests to AWS's Iceberg REST endpoints are
// signed with the connection's access key.
func TestIcebergSigV4(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKIDTEST/") || r.Header.Get("X-Amz-Date") == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/iceberg/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{"prefix":"catalogs/123456789012"}}`))
		case "/iceberg/v1/catalogs/123456789012/namespaces":
			_, _ = w.Write([]byte(`{"namespaces":[["db"]]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	c := New("glue", Iceberg)
	for k, v := range map[string]string{
		"endpoint": srv.URL + "/iceberg", "warehouse": "123456789012", "signing_name": "glue", "region": "us-east-1",
		"credential": "AKIDTEST:a-secret-access-key", "tls": "verify-ca", "tls_ca": string(ca),
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Validate(nil); err != nil {
		t.Fatal(err)
	}
	p, err := newIcebergProxy(c, "", newLocationRules(nil, false), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || !strings.Contains(seen[0], "/us-east-1/glue/aws4_request") || strings.Contains(seen[0], "a-secret-access-key") {
		t.Errorf("signatures: %q", seen)
	}
	if got := c.PasswordSource(); got != "credential stored" {
		t.Errorf("source: %s", got)
	}
	bad := c
	bad.Iceberg.Scope = "x"
	if err := bad.Validate(nil); err == nil {
		t.Error("signing_name with an OAuth2 scope")
	}
	noRegion := c
	noRegion.Iceberg.Region = ""
	if err := noRegion.Validate(nil); err == nil {
		t.Error("signing_name without a region")
	}
	if _, err := newLocationRules(nil, false).check("s3://f1a2b3c4-d5e6--table-s3", false); err != nil {
		t.Errorf("an S3 Tables table's bucket: %v", err)
	}
}
