package models

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// benchTable is a million rows of 50 features (40 numeric, 10
// categorical), a binary target y and a numeric one v.
var benchTable = sync.OnceValue(func() *Table {
	n := 1_000_000
	r := rand.New(rand.NewPCG(1, 1))
	t := &Table{N: n}
	for j := range 40 {
		c := &TableCol{Name: fmt.Sprintf("n%d", j), Type: TypeNumber, TypeName: "DOUBLE", Num: make([]float64, n), Null: make([]bool, n)}
		for i := range n {
			c.Num[i] = r.NormFloat64()
		}
		t.Cols = append(t.Cols, c)
	}
	for j := range 10 {
		c := &TableCol{Name: fmt.Sprintf("c%d", j), Type: TypeString, TypeName: "VARCHAR", Str: make([]string, n), Null: make([]bool, n)}
		for i := range n {
			c.Str[i] = fmt.Sprintf("v%d", r.IntN(20+j*10))
		}
		t.Cols = append(t.Cols, c)
	}
	y := &TableCol{Name: "y", Type: TypeBool, TypeName: "BOOLEAN", Str: make([]string, n), Null: make([]bool, n)}
	v := &TableCol{Name: "v", Type: TypeNumber, TypeName: "DOUBLE", Num: make([]float64, n), Null: make([]bool, n)}
	for i := range n {
		eta := t.Cols[0].Num[i] - 0.5*t.Cols[1].Num[i] + 0.3*t.Cols[2].Num[i]*t.Cols[3].Num[i]
		if t.Cols[40].Str[i] == "v3" {
			eta += 0.7
		}
		v.Num[i] = eta + 0.5*r.NormFloat64()
		y.Str[i] = "false"
		if r.Float64() < 1/(1+math.Exp(-eta)) {
			y.Str[i] = "true"
		}
	}
	t.Cols = append(t.Cols, y, v)
	return t
})

func skipBench(b *testing.B) {
	if os.Getenv("IGLOO_ML_BENCH") == "" {
		b.Skip("set IGLOO_ML_BENCH=1")
	}
}

// BenchmarkClassifier1M trains CLASSIFICATION as CREATE does, evaluation
// included, on a million rows of 50 features (40 numeric, 10 categorical).
// It runs only with IGLOO_ML_BENCH=1.
func BenchmarkClassifier1M(b *testing.B) {
	skipBench(b)
	t := benchTable()
	t = only(t, append(otherNames(t, "y", "v"), "y")...)
	for b.Loop() {
		start := time.Now()
		c, err := TrainClassifier(t, "y", ClassifierConfig{Evaluate: true})
		if err != nil {
			b.Fatal(err)
		}
		auc := 0.0
		for _, g := range c.Eval.Global {
			if g.AverageType == "macro" && g.Metric == "auc" {
				auc = g.Value
			}
		}
		b.Logf("CLASSIFICATION on 1M rows x 50 features: %v, %d trees, held-out AUC %.3f", time.Since(start), len(c.Model.Trees), auc)
	}
}

// BenchmarkIgloo1M trains each of Igloo's classes on the million rows
// (MATRIX_FACTORIZATION on ten million interactions), as CREATE does,
// evaluation and explanations included. It runs only with
// IGLOO_ML_BENCH=1; -bench 'Igloo1M/pca' picks one by its name.
func BenchmarkIgloo1M(b *testing.B) {
	skipBench(b)
	full := benchTable()
	features := otherNames(full, "y", "v")
	cases := []struct {
		name   string
		class  string
		target string
		cfg    map[string]any
	}{
		{"regression_boosted_trees", ClassRegression, "v", nil},
		{"regression_random_forest", ClassRegression, "v", map[string]any{"algorithm": "random_forest"}},
		{"regression_linear", ClassRegression, "v", map[string]any{"algorithm": "linear"}},
		{"classification_boosted_trees", ClassIglooClassifier, "y", nil},
		{"classification_random_forest", ClassIglooClassifier, "y", map[string]any{"algorithm": "random_forest"}},
		{"classification_logistic", ClassIglooClassifier, "y", map[string]any{"algorithm": "logistic"}},
		{"dnn_classifier", ClassDNNClassifier, "y", nil},
		{"dnn_regressor", ClassDNNRegressor, "v", nil},
		{"automl_classifier", ClassAutoMLClassifier, "y", nil},
		{"kmeans_8", ClassKMeans, "", map[string]any{"num_clusters": 8.0}},
		{"kmeans_auto", ClassKMeans, "", nil},
		{"pca", ClassPCA, "", map[string]any{"pca_explained_variance_ratio": 0.9}},
		{"autoencoder", ClassAutoencoder, "", nil},
		{"matrix_factorization_explicit", ClassFactorization, "", map[string]any{"num_factors": 8.0, "l2_reg": 0.1}},
		{"matrix_factorization_implicit", ClassFactorization, "", map[string]any{"feedback_type": "implicit"}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			t, a := only(full, append(features, c.target)...), IglooArgs{Target: c.target, Config: c.cfg}
			if c.target == "" {
				t = only(full, features...)
			}
			if c.class == ClassFactorization {
				t = interactionsTable(c.cfg["feedback_type"] == "implicit")
				a = IglooArgs{User: "person_id", Item: "campaign", Rating: "rating", Config: c.cfg}
			}
			for b.Loop() {
				start := time.Now()
				art, summary, err := TrainIgloo(c.class, t, a)
				if err != nil {
					b.Fatal(err)
				}
				blob, _, err := art.Encode()
				if err != nil {
					b.Fatal(err)
				}
				b.Logf("%s: %v on %d rows, blob %d KB, %v", c.name, time.Since(start).Round(time.Millisecond), t.N, len(blob)/1024, summary)
			}
		})
	}
}

// interactionsTable is ten million interactions with 10,000 campaigns of
// 100,000 people, a hundred each: explicit ratings of rank four and noise,
// or, for implicit feedback, clicks mostly (80%) on the campaigns of the
// person's own group of twenty.
func interactionsTable(implicit bool) *Table {
	r := rand.New(rand.NewPCG(2, 2))
	users, items, per, groups := 100_000, 10_000, 100, 20
	uf, vf := make([]float64, users*4), make([]float64, items*4)
	for i := range uf {
		uf[i] = r.NormFloat64()
	}
	for i := range vf {
		vf[i] = r.NormFloat64()
	}
	n := users * per
	t := &Table{N: n}
	user := &TableCol{Name: "person_id", Type: TypeString, Str: make([]string, n), Null: make([]bool, n)}
	item := &TableCol{Name: "campaign", Type: TypeString, Str: make([]string, n), Null: make([]bool, n)}
	rating := &TableCol{Name: "rating", Type: TypeNumber, Num: make([]float64, n), Null: make([]bool, n)}
	names := make([]string, items)
	for i := range names {
		names[i] = "c" + strconv.Itoa(i)
	}
	k := 0
	for u := range users {
		name := "p" + strconv.Itoa(u)
		for range per {
			i := r.IntN(items)
			v := 3.0
			if implicit {
				if r.Float64() < 0.8 {
					i = (u%groups)*(items/groups) + r.IntN(items/groups)
				}
				v = float64(1 + r.IntN(3))
			} else {
				for f := range 4 {
					v += uf[u*4+f] * vf[i*4+f]
				}
				v += 0.3 * r.NormFloat64()
			}
			user.Str[k], item.Str[k], rating.Num[k] = name, names[i], v
			k++
		}
	}
	t.Cols = []*TableCol{user, item, rating}
	return t
}
