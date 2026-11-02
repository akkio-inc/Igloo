package azurecreds

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

const storage = "https://storage.azure.com/.default"

// identity is a fake identity, which gives the token next says.
type identity struct {
	mu    sync.Mutex
	calls atomic.Int64
	next  func(call int64) (azcore.AccessToken, error)
	gate  chan struct{}
}

func (f *identity) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	n := f.calls.Add(1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return azcore.AccessToken{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.next(n)
}

func tokenIn(d time.Duration, name string) azcore.AccessToken {
	return azcore.AccessToken{Token: name, ExpiresOn: time.Now().Add(d)}
}

// noBackground has a provider's background wait forever.
func noBackground(p *Provider) *Provider {
	p.after = func(time.Duration) <-chan time.Time { return nil }
	return p
}

// TestFetchesOnce checks that callers at once share one fetch, that a token held is
// handed out without asking, and that a caller with a short deadline does not end the
// fetch for the others.
func TestFetchesOnce(t *testing.T) {
	f := &identity{gate: make(chan struct{}), next: func(int64) (azcore.AccessToken, error) { return tokenIn(time.Hour, "a"), nil }}
	p := noBackground(New(f, nil))
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.GetToken(short, policy.TokenRequestOptions{Scopes: []string{storage}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a short deadline: %v", err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := p.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{storage}})
			if err != nil || got.Token != "a" {
				t.Errorf("%v, %v", got.Token, err)
			}
		}()
	}
	close(f.gate)
	wg.Wait()
	if _, err := p.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{storage}}); err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Errorf("asked the identity %d times", n)
	}
}

// TestRefreshAt checks when a token is refreshed.
func TestRefreshAt(t *testing.T) {
	now := time.Now()
	for left, want := range map[time.Duration]time.Duration{
		24 * time.Hour:   24*time.Hour - refreshAhead,
		20 * time.Minute: 10 * time.Minute,
		time.Minute:      30 * time.Second,
		-time.Minute:     0,
	} {
		if got := refreshAt(now.Add(left), now).Sub(now); got != want {
			t.Errorf("%v left: in %v, want %v", left, got, want)
		}
	}
}

// TestRefreshes checks that the background replaces a token ahead of its expiry, keeps
// it through a failure and through answers of the same token, as an identity's cache
// gives until shortly before its expiry, and hands out the new one as soon as it has it.
func TestRefreshes(t *testing.T) {
	first := tokenIn(time.Hour, "first")
	f := &identity{next: func(n int64) (azcore.AccessToken, error) {
		switch n {
		case 1, 3:
			return first, nil
		case 2:
			return azcore.AccessToken{}, errors.New("the service is busy")
		}
		return tokenIn(2*time.Hour, "second"), nil
	}}
	p := New(f, nil)
	var stopped atomic.Bool
	t.Cleanup(func() { stopped.Store(true) })
	p.after = func(time.Duration) <-chan time.Time {
		c := make(chan time.Time, 1)
		if !stopped.Load() {
			c <- time.Now()
		}
		return c
	}
	get := func() string {
		t.Helper()
		got, err := p.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{storage}})
		if err != nil {
			t.Fatal(err)
		}
		return got.Token
	}
	if got := get(); got != "first" {
		t.Fatalf("got %s", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for get() != "second" {
		if time.Now().After(deadline) {
			t.Fatal("never refreshed")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestExpiredFetchesAgain checks that a token past its expiry is not handed out, and that
// an identity's failure then is the caller's to see.
func TestExpiredFetchesAgain(t *testing.T) {
	f := &identity{next: func(n int64) (azcore.AccessToken, error) {
		if n == 1 {
			return tokenIn(30*time.Second, "nearly gone"), nil
		}
		return azcore.AccessToken{}, errors.New("no identity")
	}}
	p := noBackground(New(f, nil))
	if _, err := p.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{storage}}); err == nil {
		t.Error("a token within the margin of its expiry was handed out")
	}
	if _, err := p.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{storage}}); err == nil || f.calls.Load() != 2 {
		t.Errorf("%v after %d calls", err, f.calls.Load())
	}
}

// TestHandler checks the endpoint: as Azure App Service's speaks, to those that send the
// header alone, and that it holds the token rather than ask again.
func TestHandler(t *testing.T) {
	f := &identity{next: func(int64) (azcore.AccessToken, error) { return tokenIn(time.Hour, "the-token"), nil }}
	p := noBackground(New(f, nil))
	srv := httptest.NewServer(p.Handler("the-header"))
	defer srv.Close()
	ask := func(header, query string) (int, map[string]string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/msi/token?api-version=2019-08-01"+query, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-IDENTITY-HEADER", header)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close() //nolint:errcheck
		var body map[string]string
		_ = json.NewDecoder(res.Body).Decode(&body)
		return res.StatusCode, body
	}
	if code, _ := ask("another", "&resource=https://storage.azure.com"); code != http.StatusForbidden {
		t.Errorf("without the header: %d", code)
	}
	if code, _ := ask("the-header", ""); code != http.StatusBadRequest {
		t.Errorf("without a resource: %d", code)
	}
	for range 3 {
		code, body := ask("the-header", "&resource=https://storage.azure.com/")
		if code != http.StatusOK || body["access_token"] != "the-token" || body["token_type"] != "Bearer" {
			t.Fatalf("%d, %v", code, body)
		}
		on, err := strconv.ParseInt(body["expires_on"], 10, 64)
		if err != nil || time.Unix(on, 0).Before(time.Now().Add(50*time.Minute)) {
			t.Errorf("expires_on %q", body["expires_on"])
		}
	}
	if n := f.calls.Load(); n != 1 {
		t.Errorf("asked the identity %d times", n)
	}
}
