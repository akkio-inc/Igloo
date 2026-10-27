package erasure

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/akkio-inc/igloo/internal/policy"
)

// fakeModels stands in for the models (package pgwire's ModelRetrainer):
// each with the tables it read, the epoch its training began in, and its
// blob, and the blobs object storage has.
type fakeModels struct {
	mu     sync.Mutex
	epoch  int64
	models map[string]*fakeModel
	blobs  map[string]bool
	// fail says why a model cannot be trained again; dropFails how many
	// DropBlobs calls still fail to delete one blob.
	fail      map[string]string
	dropFails int
	retrained []string
	n         int
}

type fakeModel struct {
	tables []string
	epoch  int64
	blob   string
}

func newFakeModels() *fakeModels {
	return &fakeModels{epoch: 1, models: map[string]*fakeModel{}, blobs: map[string]bool{}, fail: map[string]string{}}
}

func (f *fakeModels) add(name string, tables ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	b := fmt.Sprintf("blob-%d", f.n)
	f.models[name] = &fakeModel{tables: tables, epoch: f.epoch, blob: b}
	f.blobs[b] = true
}

func (f *fakeModels) BumpEpoch(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch++
	return f.epoch, nil
}

func (f *fakeModels) staleLocked(tables []string, epoch int64) []string {
	var out []string
	for name, m := range f.models {
		trained := m.tables == nil
		for _, t := range m.tables {
			trained = trained || t == "*" || slices.Contains(tables, t)
		}
		if trained && (epoch == 0 || m.epoch < epoch) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func (f *fakeModels) Retrain(_ context.Context, tables []string, epoch int64) (Retrained, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out Retrained
	for _, name := range f.staleLocked(tables, epoch) {
		m := f.models[name]
		out.Blobs = append(out.Blobs, m.blob)
		if why := f.fail[name]; why != "" {
			out.Failed = append(out.Failed, name+": "+why)
			continue
		}
		f.n++
		m.blob, m.epoch = fmt.Sprintf("blob-%d", f.n), f.epoch
		f.blobs[m.blob] = true
		f.retrained = append(f.retrained, name)
		out.Models++
	}
	return out, nil
}

func (f *fakeModels) Stale(_ context.Context, tables []string, epoch int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.staleLocked(tables, epoch), nil
}

func (f *fakeModels) DropBlobs(_ context.Context, blobs []string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var left int64
	for _, b := range blobs {
		used := false
		for _, m := range f.models {
			used = used || m.blob == b
		}
		if used || !f.blobs[b] {
			continue
		}
		if f.dropFails > 0 {
			f.dropFails--
			left++
			continue
		}
		delete(f.blobs, b)
	}
	return left, nil
}

func (f *fakeModels) blobOf(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.models[name].blob
}

func (f *fakeModels) stored(blob string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blobs[blob]
}

// TestModelsRetrained: a request has the models trained from its tables,
// from what Igloo could not name, or that did not record what they read,
// trained again after its history step, deletes the blobs they had, and
// completes only then; the others stay as they are.
func TestModelsRetrained(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	m := newFakeModels()
	f.svc.opts.Models = m
	m.add("demo.recommender", "crm.people")
	m.add("demo.spend", "demo.campaigns")
	m.add("demo.from_files", "*")
	m.add("demo.unrecorded")
	m.models["demo.unrecorded"].tables = nil
	before := map[string]string{}
	for name := range m.models {
		before[name] = m.blobOf(name)
	}

	r := f.erase(byEmail)
	f.completed(r)
	if strings.Join(m.retrained, ",") != "demo.from_files,demo.recommender,demo.unrecorded" || r.ModelsRetrained != 3 {
		t.Errorf("trained again %q (%d)", m.retrained, r.ModelsRetrained)
	}
	for _, name := range []string{"demo.recommender", "demo.from_files", "demo.unrecorded"} {
		if m.stored(before[name]) || m.blobOf(name) == before[name] || !m.stored(m.blobOf(name)) {
			t.Errorf("%s: old blob kept %v, new blob %s", name, m.stored(before[name]), m.blobOf(name))
		}
	}
	if m.blobOf("demo.spend") != before["demo.spend"] || !m.stored(before["demo.spend"]) {
		t.Errorf("a model trained from another table was trained again")
	}
	if r.Verification.Models != 0 || r.ModelEpoch != 2 || len(r.ModelBlobs) != 3 || r.DeletedThrough.IsZero() {
		t.Errorf("verification %+v, epoch %d, blobs %v, deleted through %v", r.Verification, r.ModelEpoch, r.ModelBlobs, r.DeletedThrough)
	}
	// The models step ran after the history step, before the cache step.
	var order []string
	for _, s := range r.Steps {
		order = append(order, s.Step)
	}
	if i := slices.Index(order, StepModels); i < 1 || order[i-1] != StepHistory || order[i+1] != StepCache {
		t.Errorf("steps %v", order)
	}
}

// TestModelRetrainBlocks: a model that cannot be trained again blocks the
// request, with an alert, until it can; then the request completes,
// training only that one again.
func TestModelRetrainBlocks(t *testing.T) {
	f := newFixture(t)
	alerts := f.alerts()
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	m := newFakeModels()
	f.svc.opts.Models = m
	m.add("demo.recommender", "crm.people")
	m.add("demo.segments", "crm.people")
	m.fail["demo.segments"] = "its owner nora no longer exists"
	old := m.blobOf("demo.segments")

	r := f.erase(byEmail)
	f.blocked(r, StepModels, "demo.segments: its owner nora no longer exists", *alerts)
	if strings.Join(r.ModelsFailed, ",") != "demo.segments: its owner nora no longer exists" || r.ModelsRetrained != 1 {
		t.Errorf("failed %q, trained again %d", r.ModelsFailed, r.ModelsRetrained)
	}
	if !m.stored(old) {
		t.Error("the blob of a model not trained again is gone")
	}
	// Still failing an hour later, it stays blocked, and does not train
	// the other again.
	if got := f.runTimes(r.ID, 1); got.Status != StatusBlocked || len(m.retrained) != 1 {
		t.Errorf("status %s, trained again %q", got.Status, m.retrained)
	}
	delete(m.fail, "demo.segments")
	got := f.runTimes(r.ID, 1)
	f.completed(got)
	if strings.Join(m.retrained, ",") != "demo.recommender,demo.segments" || m.stored(old) || len(got.ModelsFailed) != 0 {
		t.Errorf("trained again %q, old blob kept %v, failed %q", m.retrained, m.stored(old), got.ModelsFailed)
	}
}

// TestModelsLeftOnClose: closing a request blocked by a model records the
// model it leaves, with why, and deletes the blobs of those it trained
// again.
func TestModelsLeftOnClose(t *testing.T) {
	f := newFixture(t)
	f.alerts()
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	m := newFakeModels()
	f.svc.opts.Models = m
	m.add("demo.recommender", "crm.people")
	m.add("demo.segments", "crm.people")
	m.fail["demo.segments"] = "nora may not read crm.people"
	old := m.blobOf("demo.recommender")
	// The first try to delete the old blob fails too.
	m.dropFails = 1
	r := f.erase(byEmail)
	if r.Status != StatusBlocked {
		t.Fatalf("status %s at %s: %s", r.Status, r.Step, r.Error)
	}
	closed, err := f.svc.Close(f.ctx, r.ID, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != StatusClosed || strings.Join(closed.ModelsLeft, ",") != "demo.segments: nora may not read crm.people" ||
		!strings.Contains(closed.Error, "it leaves 1 models trained from its tables") {
		t.Errorf("status %s, left %q, error %q", closed.Status, closed.ModelsLeft, closed.Error)
	}
	if m.stored(old) {
		t.Error("closing kept the old blob of a model trained again")
	}
	// What it records holds no identifier.
	if b, _ := json.Marshal(closed); strings.Contains(string(b), storedEmail) {
		t.Errorf("the closed request holds an identifier: %s", b)
	}
}

// TestModelBlobLeftRetries: an old blob object storage would not delete
// keeps the request from going on, and it tries again.
func TestModelBlobLeftRetries(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	m := newFakeModels()
	f.svc.opts.Models = m
	m.add("demo.recommender", "crm.people")
	old := m.blobOf("demo.recommender")
	m.dropFails = 1
	r := f.erase(byEmail)
	if r.Status != StatusRetrying || r.Step != StepModels {
		t.Fatalf("status %s at %s: %s", r.Status, r.Step, r.Error)
	}
	got := f.runTimes(r.ID, 1)
	f.completed(got)
	if m.stored(old) || len(m.retrained) != 1 {
		t.Errorf("old blob kept %v, trained again %q", m.stored(old), m.retrained)
	}
}
