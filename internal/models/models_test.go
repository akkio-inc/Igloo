package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// stores runs test on a store in memory, and on one in PostgreSQL when
// IGLOO_TEST_DATABASE_URL is set.
func stores(t *testing.T, test func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { test(t, NewMemoryStore()) })
	url := testdb.URL()
	if url == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		s, err := OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.pool.Exec(context.Background(), "TRUNCATE igloo_ml.models CASCADE"); err != nil {
			t.Fatal(err)
		}
		// Migrations are idempotent, as several replicas run them.
		again, err := OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		again.Close()
		test(t, s)
	})
}

func TestStore(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		m := Model{Schema: "Demo", Name: "Conversion", Class: ClassClassification, Owner: "ana", Comment: "who converts",
			TrainingSQL: "CREATE ...", Config: `{"evaluate": true}`, Metrics: `{"macro_auc": 0.9}`, Blob: strings.Repeat("a", 64), BlobSize: 10}
		if old, err := s.Put(ctx, m, false); err != nil || old != nil {
			t.Fatalf("put: %v, %v", old, err)
		}
		if _, err := s.Put(ctx, m, false); !errors.Is(err, ErrExists) {
			t.Errorf("a second put: %v", err)
		}
		got, err := s.Get(ctx, "DEMO", "conversion")
		if err != nil || got.Schema != "demo" || got.Name != "conversion" || got.Owner != "ana" || got.Comment != "who converts" ||
			got.Blob != m.Blob || got.CreatedAt.IsZero() || !strings.Contains(got.Metrics, "macro_auc") {
			t.Errorf("get: %+v, %v", got, err)
		}
		g := Grant{Schema: "demo", Name: "conversion", Grantee: "analysts", GranteeType: "role", Privilege: PrivilegePredict, GrantedBy: "ana"}
		if err := s.AddGrant(ctx, g); err != nil {
			t.Fatal(err)
		}
		if err := s.AddGrant(ctx, g); err != nil {
			t.Errorf("granting again: %v", err)
		}
		if err := s.AddGrant(ctx, Grant{Schema: "demo", Name: "nope", Grantee: "x", GranteeType: "user", Privilege: PrivilegeUsage}); !errors.Is(err, ErrNotFound) {
			t.Errorf("a grant on no model: %v", err)
		}
		// Replacing keeps grants.
		m.Blob = strings.Repeat("b", 64)
		old, err := s.Put(ctx, m, true)
		if err != nil || old == nil || old.Blob != strings.Repeat("a", 64) {
			t.Fatalf("replace: %+v, %v", old, err)
		}
		if gs, _ := s.Grants(ctx); len(gs) != 1 || gs[0].Privilege != PrivilegePredict {
			t.Errorf("grants after replacing: %+v", gs)
		}
		if used, _ := s.BlobUsed(ctx, strings.Repeat("a", 64)); used {
			t.Error("the replaced blob is still used")
		}
		if used, _ := s.BlobUsed(ctx, strings.Repeat("b", 64)); !used {
			t.Error("the new blob is not used")
		}
		if err := s.RevokeGrant(ctx, g); err != nil {
			t.Fatal(err)
		}
		if gs, _ := s.Grants(ctx); len(gs) != 0 {
			t.Errorf("grants after revoking: %+v", gs)
		}
		if err := s.AddGrant(ctx, g); err != nil {
			t.Fatal(err)
		}
		if list, _ := s.List(ctx); len(list) != 1 {
			t.Errorf("list: %+v", list)
		}
		if _, err := s.Delete(ctx, "demo", "conversion"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "demo", "conversion"); !errors.Is(err, ErrNotFound) {
			t.Errorf("get after delete: %v", err)
		}
		if gs, _ := s.Grants(ctx); len(gs) != 0 {
			t.Errorf("grants outlive their model: %+v", gs)
		}
		if _, err := s.Delete(ctx, "demo", "conversion"); !errors.Is(err, ErrNotFound) {
			t.Errorf("delete again: %v", err)
		}
	})
}

// TestStoreDroppedGrantees: a dropped user's models are given to a name no
// user can have, and the grants to a dropped user or role go, leaving those
// to others of the same name.
func TestStoreDroppedGrantees(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		for _, m := range []Model{{Schema: "demo", Name: "anas", Owner: "ana"}, {Schema: "demo", Name: "bobs", Owner: "bob"}} {
			m.Class, m.TrainingSQL, m.Blob = ClassClassification, "CREATE ...", strings.Repeat("a", 64)
			if _, err := s.Put(ctx, m, false); err != nil {
				t.Fatal(err)
			}
		}
		for _, g := range []Grant{
			{Grantee: "ana", GranteeType: "user", Privilege: PrivilegeUsage},
			{Grantee: "ana", GranteeType: "role", Privilege: PrivilegeUsage},
			{Grantee: "analysts", GranteeType: "role", Privilege: PrivilegePredict},
			{Grantee: "analysts", GranteeType: "user", Privilege: PrivilegePredict},
		} {
			g.Schema, g.Name = "demo", "bobs"
			if err := s.AddGrant(ctx, g); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.DisownModels(ctx, "ana", "ana (dropped)"); err != nil {
			t.Fatal(err)
		}
		if err := s.ForgetGrantee(ctx, "user", "ana"); err != nil {
			t.Fatal(err)
		}
		if err := s.ForgetGrantee(ctx, "role", "analysts"); err != nil {
			t.Fatal(err)
		}
		if m, _ := s.Get(ctx, "demo", "anas"); m.Owner != "ana (dropped)" {
			t.Errorf("a dropped user's model is %q's", m.Owner)
		}
		if m, _ := s.Get(ctx, "demo", "bobs"); m.Owner != "bob" {
			t.Errorf("another's model is %q's", m.Owner)
		}
		gs, err := s.Grants(ctx)
		var left []string
		for _, g := range gs {
			left = append(left, g.GranteeType+" "+g.Grantee)
		}
		if err != nil || strings.Join(left, ", ") != "role ana, user analysts" {
			t.Errorf("grants left: %q, %v", left, err)
		}
	})
}

// TestStoreBlobLock: a blob's lock is held by one caller at a time, on
// every replica, and leaves other blobs' free.
func TestStoreBlobLock(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		// Another replica.
		other := s
		if _, ok := s.(*PostgresStore); ok {
			o, err := OpenPostgres(ctx, testdb.URL())
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			other = o
		}
		a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
		unlock, err := s.LockBlob(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		if u, err := other.LockBlob(short, a); err == nil {
			u()
			t.Fatal("a blob's lock taken twice")
		}
		unlockB, err := other.LockBlob(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		unlockB()
		taken := make(chan func())
		go func() {
			if u, err := other.LockBlob(ctx, a); err == nil {
				taken <- u
			}
			close(taken)
		}()
		select {
		case <-taken:
			t.Fatal("a blob's lock taken while held")
		case <-time.After(100 * time.Millisecond):
		}
		unlock()
		select {
		case u, ok := <-taken:
			if !ok {
				t.Fatal("the lock was not taken once let go")
			}
			u()
		case <-time.After(10 * time.Second):
			t.Fatal("the lock was not taken once let go")
		}
	})
}

// TestStoreErasureEpoch: a model keeps the tables its training read, and
// the epoch it began in; one whose training began before an erasure
// request moved the epoch on is not stored; and a model trained again
// replaces only the model it was trained from.
func TestStoreErasureEpoch(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		epoch, err := s.Epoch(ctx)
		if err != nil || epoch < 1 {
			t.Fatalf("epoch %d, %v", epoch, err)
		}
		m := Model{Schema: "demo", Name: "recommender", Class: ClassFactorization, Owner: "ana", TrainingSQL: "CREATE ...",
			Blob: strings.Repeat("a", 64), Tables: []string{"demo.ratings", AnyTable}, Epoch: epoch, SearchPath: "lake.demo",
			Dialect: "snowflake"}
		if _, err := s.Put(ctx, m, false); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "demo", "recommender")
		if err != nil || strings.Join(got.Tables, ",") != "demo.ratings,*" || got.Epoch != epoch || got.SearchPath != "lake.demo" ||
			got.Dialect != "snowflake" {
			t.Fatalf("get: %+v, %v", got, err)
		}
		// None read, and not recorded, differ.
		none := Model{Schema: "demo", Name: "insights", Class: ClassTopInsights, Owner: "ana", Blob: strings.Repeat("c", 64), Tables: []string{}}
		if _, err := s.Put(ctx, none, false); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Get(ctx, "demo", "insights"); got.Tables == nil || got.TrainedOn([]string{"demo.ratings"}) {
			t.Errorf("a model that read nothing: %+v", got.Tables)
		}
		if !(Model{}).TrainedOn([]string{"demo.ratings"}) || !got.TrainedOn([]string{"crm.people"}) {
			t.Error("a model that did not record what it read, or read files, counts as trained from every table")
		}

		next, err := s.BumpEpoch(ctx)
		if err != nil || next != epoch+1 {
			t.Fatalf("bumped to %d, %v", next, err)
		}
		// A CREATE that read its training data before is not stored.
		m.Blob = strings.Repeat("b", 64)
		if _, err := s.Put(ctx, m, true); !errors.Is(err, ErrStale) {
			t.Errorf("a model trained before the epoch moved on: %v", err)
		}
		if got, _ := s.Get(ctx, "demo", "recommender"); got.Blob != strings.Repeat("a", 64) {
			t.Errorf("a stale model replaced the one there: %+v", got)
		}
		// Trained again, it replaces the model it was trained from, and not
		// one replaced or dropped meanwhile.
		m.Epoch, m.IfBlob = next, strings.Repeat("a", 64)
		if old, err := s.Put(ctx, m, true); err != nil || old == nil || old.Blob != strings.Repeat("a", 64) {
			t.Fatalf("trained again: %+v, %v", old, err)
		}
		if got, _ := s.Get(ctx, "demo", "recommender"); got.Blob != strings.Repeat("b", 64) || got.Epoch != next || got.IfBlob != "" {
			t.Errorf("after training again: %+v", got)
		}
		if _, err := s.Put(ctx, m, true); !errors.Is(err, ErrChanged) {
			t.Errorf("trained again from a model replaced since: %v", err)
		}
		if _, err := s.Delete(ctx, "demo", "recommender"); err != nil {
			t.Fatal(err)
		}
		m.IfBlob = strings.Repeat("b", 64)
		if _, err := s.Put(ctx, m, true); !errors.Is(err, ErrChanged) {
			t.Errorf("trained again from a model dropped since: %v", err)
		}
		if _, err := s.Get(ctx, "demo", "recommender"); !errors.Is(err, ErrNotFound) {
			t.Errorf("a dropped model came back: %v", err)
		}
	})
}

func TestClassifier(t *testing.T) {
	c, err := TrainClassifier(testTable(1000), "Converted", ClassifierConfig{Evaluate: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.Target != "converted" || strings.Join(c.Classes, ",") != "False,True" || len(c.Features) != 2 || c.Rows != 1000 {
		t.Errorf("trained %+v", c)
	}
	if c.Eval == nil || c.Eval.Rows < 100 || c.Eval.Rows > 220 {
		t.Fatalf("evaluated on %+v", c.Eval)
	}
	for _, g := range c.Eval.Global {
		if g.Metric == "auc" && g.Value < 0.99 {
			t.Errorf("%s AUC %.3f", g.AverageType, g.Value)
		}
	}
	var sum float64
	for _, v := range c.Importance {
		sum += v
	}
	if sum < 0.999 || sum > 1.001 {
		t.Errorf("importances add up to %v", sum)
	}
	res, err := c.Method("SHOW_THRESHOLD_METRICS")
	if err != nil || len(res.Rows) != 202 || len(res.Rows[0]) != len(res.Columns) {
		t.Errorf("threshold metrics: %d rows, %v", len(res.Rows), err)
	}
	if _, err := c.Method("FORECAST"); !InputError(err) {
		t.Errorf("FORECAST of a classifier: %v", err)
	}
	// Probabilities as Snowflake shows them.
	probs := []float64{0.25, 0.75}
	if got := string(c.PredictJSON(nil, probs)); got != `{"class":"True","logs":null,"probability":{"False":0.25,"True":0.75}}` {
		t.Errorf("JSON %s", got)
	}
	// A label with quotes stays JSON.
	c.Classes[0] = `say "hi"` + "\n"
	if got := string(c.PredictJSON(nil, []float64{1, 0})); got != `{"class":"say \"hi\"\u000a","logs":null,"probability":{"say \"hi\"\u000a":1,"True":0}}` {
		t.Errorf("JSON %s", got)
	}

	// Three classes, from numbers.
	tbl := testTable(900)
	tier := &TableCol{Name: "tier", Type: TypeNumber, TypeName: "INTEGER"}
	for i := range tbl.N {
		v := 1.0
		switch {
		case tbl.Cols[1].Num[i] > 66:
			v = 3
		case tbl.Cols[1].Num[i] > 33:
			v = 2
		}
		tier.Num, tier.Null = append(tier.Num, v), append(tier.Null, false)
	}
	tbl.Cols = append(tbl.Cols[:2], tier)
	m, err := TrainClassifier(tbl, "tier", ClassifierConfig{Evaluate: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(m.Classes, ",") != "1,2,3" || m.Model.Classes != 3 {
		t.Errorf("classes %v", m.Classes)
	}
	for _, s := range m.Eval.PerClass {
		if s.F1 < 0.95 {
			t.Errorf("per class %+v", m.Eval.PerClass)
		}
	}

	for target, says := range map[string]string{"nope": "not a column", "region": "as many classes"} {
		tb := testTable(4)
		if _, err := TrainClassifier(tb, target, ClassifierConfig{}); err == nil || !strings.Contains(err.Error(), says) {
			t.Errorf("%s: %v", target, err)
		}
	}
}
