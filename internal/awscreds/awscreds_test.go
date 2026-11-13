package awscreds

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/io/gocloud"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// fakeChain is a provider whose fetches take delay, fail with err, and
// give credentials that expire after life.
type fakeChain struct {
	mu    sync.Mutex
	delay time.Duration
	err   error
	life  time.Duration
	calls atomic.Int32
}

func (f *fakeChain) set(delay time.Duration, err error) {
	f.mu.Lock()
	f.delay, f.err = delay, err
	f.mu.Unlock()
}

func (f *fakeChain) Retrieve(ctx context.Context) (aws.Credentials, error) {
	f.calls.Add(1)
	f.mu.Lock()
	delay, err, life := f.delay, f.err, f.life
	f.mu.Unlock()
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return aws.Credentials{}, ctx.Err()
	}
	if err != nil {
		return aws.Credentials{}, err
	}
	return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret", SessionToken: "token",
		CanExpire: true, Expires: time.Now().Add(life)}, nil
}

// retrieveAll runs n Retrieves at once, each with a deadline of within,
// and returns how many failed.
func retrieveAll(p aws.CredentialsProvider, n int, within time.Duration) int {
	var failed atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), within)
			defer cancel()
			if c, err := p.Retrieve(ctx); err != nil || c.AccessKeyID == "" {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(failed.Load())
}

// TestProvider fetches credentials once for every caller waiting, under a
// deadline of its own; hands out those held at once while they are valid,
// whatever a refresh in the background is doing, slow or failing; and
// once they expire with the chain still failing, fails as it does.
func TestProvider(t *testing.T) {
	defer func(m time.Duration) { validMargin = m }(validMargin)
	validMargin = 100 * time.Millisecond
	chain := &fakeChain{delay: 200 * time.Millisecond, life: 2 * time.Second}
	p := New(func(context.Context) (aws.CredentialsProvider, error) { return chain, nil }, nil)
	// Cold: a hundred callers wait on one fetch.
	if n := retrieveAll(p, 100, 5*time.Second); n > 0 {
		t.Fatalf("%d of 100 callers got no credentials", n)
	}
	if n := chain.calls.Load(); n != 1 {
		t.Errorf("%d fetches for a hundred callers, want 1", n)
	}
	// The chain turns slow and failing; the background refreshes halfway
	// through the credentials' life, a second on. Callers with short
	// deadlines get those held.
	chain.set(3*time.Second, errors.New("ec2imds: GetMetadata, request canceled, context deadline exceeded"))
	deadline := time.Now().Add(1700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := retrieveAll(p, 50, 50*time.Millisecond); n > 0 {
			t.Fatalf("%d of 50 callers got no credentials while those held were valid", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := chain.calls.Load(); n > 2 {
		t.Errorf("%d fetches, want the first and one refresh at most", n)
	}
	// Expired, with the chain still failing: no credentials.
	time.Sleep(time.Until(deadline.Add(300 * time.Millisecond)))
	if n := retrieveAll(p, 10, 50*time.Millisecond); n != 10 {
		t.Errorf("%d of 10 callers failed with the credentials expired and the chain failing, want 10", n)
	}
	// Back: credentials again.
	chain.set(0, nil)
	if n := retrieveAll(p, 10, 5*time.Second); n > 0 {
		t.Errorf("%d of 10 callers got no credentials once the chain answered", n)
	}
}

// TestRefreshThroughTheSDKCache runs the provider over the chain as the SDK
// resolves it, wrapped in its credentials cache, which hands out what it
// holds until that has expired. Refreshes ahead of the expiry got the same
// credentials back, and every caller failed for the last validMargin of
// their life.
func TestRefreshThroughTheSDKCache(t *testing.T) {
	defer func(m time.Duration) { validMargin = m }(validMargin)
	validMargin = 300 * time.Millisecond
	chain := &fakeChain{life: 2 * time.Second}
	cache := aws.NewCredentialsCache(chain)
	p := New(func(context.Context) (aws.CredentialsProvider, error) { return cache, nil }, nil)
	start := time.Now()
	for time.Since(start) < 5*time.Second {
		if n := retrieveAll(p, 1, time.Second); n > 0 {
			t.Fatalf("a caller got no credentials %v in, with credentials that last %v (%d fetches)",
				time.Since(start).Round(time.Millisecond), chain.life, chain.calls.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// imds is a fake EC2 instance metadata service handing out an instance
// profile's credentials, its answers to their requests delayed by delay
// after the first, as under load.
type imds struct {
	delay atomic.Int64
	gets  atomic.Int32
}

func (m *imds) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
		w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
		_, _ = w.Write([]byte("imds-token"))
	case r.URL.Path == "/latest/meta-data/iam/security-credentials/":
		_, _ = w.Write([]byte("igloo-test-role"))
	case r.URL.Path == "/latest/meta-data/iam/security-credentials/igloo-test-role":
		if m.gets.Add(1) > 1 {
			time.Sleep(time.Duration(m.delay.Load()))
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"Code": "Success", "Type": "AWS-HMAC", "AccessKeyId": "AKIDEXAMPLE", "SecretAccessKey": "secret", "Token": "token",
			"LastUpdated": time.Now().UTC().Format(time.RFC3339), "Expiration": time.Now().Add(6 * time.Hour).UTC().Format(time.RFC3339),
		})
	default:
		http.NotFound(w, r)
	}
}

// TestConcurrentPlansSlowMetadata plans, as statements do, many scans at
// once against an instance metadata service that answers in a second: each
// builds an S3 file system the way iceberg-go does for a table, and gets
// its credentials under a deadline shorter than the service's answers.
// Each file system with a provider of its own fetched from the service,
// and timed out; with the process's provider, one fetch serves them all.
func TestConcurrentPlansSlowMetadata(t *testing.T) {
	m := &imds{}
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := t.TempDir()
	for k, v := range map[string]string{
		"AWS_EC2_METADATA_SERVICE_ENDPOINT": srv.URL, "AWS_EC2_METADATA_DISABLED": "false",
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "",
		"AWS_CONFIG_FILE": filepath.Join(dir, "config"), "AWS_SHARED_CREDENTIALS_FILE": filepath.Join(dir, "credentials"),
		"AWS_WEB_IDENTITY_TOKEN_FILE": "", "AWS_ROLE_ARN": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
	} {
		t.Setenv(k, v)
	}
	m.delay.Store(int64(time.Second))
	props := map[string]string{"s3.region": "us-east-1"}
	plan := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		cfg, err := gocloud.ParseAWSConfig(ctx, props)
		if err != nil {
			return err
		}
		_, err = cfg.Credentials.Retrieve(ctx)
		return err
	}
	planAll := func(n int) (failed int, first error) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := plan(); err != nil {
					mu.Lock()
					failed++
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		return failed, first
	}
	// A provider a file system: the service's first answer is quick, the
	// rest are late.
	if failed, first := planAll(32); failed < 31 || !strings.Contains(first.Error(), "deadline") {
		t.Fatalf("%d of 32 plans failed with a provider each (first: %v); the test proves nothing", failed, first)
	}
	// The process's provider: one fetch, at the service's pace, before
	// the plans, as the first statement's; then none fails.
	m.gets.Store(0)
	p := New(func(ctx context.Context) (aws.CredentialsProvider, error) {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
		if err != nil {
			return nil, err
		}
		return cfg.Credentials, nil
	}, nil)
	gocloud.SetDefaultCredentials(p)
	defer gocloud.SetDefaultCredentials(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := p.Retrieve(ctx); err != nil {
		t.Fatal(err)
	}
	if failed, first := planAll(64); failed > 0 {
		t.Errorf("%d of 64 plans failed with the process's provider: %v", failed, first)
	}
	if n := m.gets.Load(); n != 1 {
		t.Errorf("%d fetches from the metadata service, want 1", n)
	}
}
