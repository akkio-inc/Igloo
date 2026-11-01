package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeScale is a Deployment's scale subresource as the Kubernetes API
// serves it, with optimistic concurrency on its resource version.
type fakeScale struct {
	mu       sync.Mutex
	replicas int
	version  int
	patches  []string
	// race, when set, is a change someone else makes after the next read.
	race func()
}

func (f *fakeScale) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"resourceVersion": itoa(f.version)},
			"spec":     map[string]any{"replicas": f.replicas},
		})
		if f.race != nil {
			f.race()
			f.race = nil
		}
	case http.MethodPatch:
		body, _ := io.ReadAll(r.Body)
		f.patches = append(f.patches, string(body))
		var p struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
			Spec struct {
				Replicas int `json:"replicas"`
			} `json:"spec"`
		}
		_ = json.Unmarshal(body, &p)
		if p.Metadata.ResourceVersion != "" && p.Metadata.ResourceVersion != itoa(f.version) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		f.replicas, f.version = p.Spec.Replicas, f.version+1
		w.WriteHeader(http.StatusOK)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

// ShrinkTo lowers the count only from what the caller saw, and loses a
// race rather than undo another change.
func TestShrinkTo(t *testing.T) {
	fake := &fakeScale{replicas: 6, version: 3}
	srv := httptest.NewTLSServer(fake)
	defer srv.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	k := &Kubernetes{Namespace: "ns", Deployment: "workers", client: srv.Client(), base: srv.URL, tokenFile: token}
	ctx := context.Background()
	// Pods starting: 6 asked for, 5 seen.
	if ok, err := k.ShrinkTo(ctx, 3, 5); ok || err != nil || fake.replicas != 6 {
		t.Fatalf("from 5 of 6: %v %v, replicas %d", ok, err, fake.replicas)
	}
	if ok, err := k.ShrinkTo(ctx, 4, 6); !ok || err != nil || fake.replicas != 4 || !strings.Contains(fake.patches[0], `"resourceVersion":"3"`) {
		t.Fatalf("from 6 to 4: %v %v, replicas %d, patches %v", ok, err, fake.replicas, fake.patches)
	}
	// Scaled up by another between the read and the patch: left alone.
	fake.race = func() { fake.replicas, fake.version = 8, fake.version+1 }
	if ok, err := k.ShrinkTo(ctx, 2, 4); ok || err != nil || fake.replicas != 8 {
		t.Fatalf("during a race: %v %v, replicas %d", ok, err, fake.replicas)
	}
}

func TestScaleAtLeast(t *testing.T) {
	fake := &fakeScale{replicas: 3, version: 7}
	srv := httptest.NewTLSServer(fake)
	defer srv.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	k := &Kubernetes{Namespace: "ns", Deployment: "workers", client: srv.Client(), base: srv.URL, tokenFile: token}
	ctx := context.Background()

	// Fewer than running: nothing changes.
	if err := k.ScaleAtLeast(ctx, 2); err != nil || fake.replicas != 3 || len(fake.patches) != 0 {
		t.Fatalf("scale to 2: %v, replicas %d, patches %v", err, fake.replicas, fake.patches)
	}
	// More: it grows, conditionally on the version it read.
	if err := k.ScaleAtLeast(ctx, 5); err != nil || fake.replicas != 5 {
		t.Fatalf("scale to 5: %v, replicas %d", err, fake.replicas)
	}
	if !strings.Contains(fake.patches[0], `"resourceVersion":"7"`) {
		t.Errorf("patch was not conditional: %s", fake.patches[0])
	}
	// Someone else scales up between the read and the patch: the patch
	// fails, the count is read again, and it is high enough.
	fake.race = func() { fake.replicas, fake.version = 9, fake.version+1 }
	if err := k.ScaleAtLeast(ctx, 6); err != nil || fake.replicas != 9 {
		t.Fatalf("scale to 6 during a race: %v, replicas %d", err, fake.replicas)
	}
	// Zero stops them all.
	if err := k.ScaleAtLeast(ctx, 0); err != nil || fake.replicas != 0 {
		t.Fatalf("scale to 0: %v, replicas %d", err, fake.replicas)
	}
}
