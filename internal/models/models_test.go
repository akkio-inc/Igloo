package models

import (
	"context"
	"errors"
	"strings"
	"testing"

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
