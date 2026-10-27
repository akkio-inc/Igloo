package models

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

// BenchmarkClassifier1M trains CLASSIFICATION as CREATE does, evaluation
// included, on a million rows of 50 features (40 numeric, 10 categorical).
// It runs only with IGLOO_ML_BENCH=1.
func BenchmarkClassifier1M(b *testing.B) {
	if os.Getenv("IGLOO_ML_BENCH") == "" {
		b.Skip("set IGLOO_ML_BENCH=1")
	}
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
	for i := range n {
		eta := t.Cols[0].Num[i] - 0.5*t.Cols[1].Num[i] + 0.3*t.Cols[2].Num[i]*t.Cols[3].Num[i]
		if t.Cols[40].Str[i] == "v3" {
			eta += 0.7
		}
		y.Str[i] = "false"
		if r.Float64() < 1/(1+math.Exp(-eta)) {
			y.Str[i] = "true"
		}
	}
	t.Cols = append(t.Cols, y)
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
