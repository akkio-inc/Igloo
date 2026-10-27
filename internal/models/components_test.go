package models

import (
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"
)

// factorTable is a synthetic table of five measures driven by two planted
// factors and a little noise, then outliers off the plane they span.
func factorTable(n, outliers int, seed uint64) *Table {
	r := rand.New(rand.NewPCG(seed, seed))
	load := [][]float64{{1, 2, 0, 1, 0.5}, {0, 1, 3, -1, 1}}
	names := []string{"impressions", "clicks", "spend", "visits", "signups"}
	t := &Table{N: n + outliers}
	for _, name := range names {
		t.Cols = append(t.Cols, &TableCol{Name: name, Type: TypeNumber, TypeName: "DOUBLE", Num: make([]float64, t.N), Null: make([]bool, t.N)})
	}
	for i := range t.N {
		z0, z1 := r.NormFloat64(), r.NormFloat64()
		for j := range names {
			v := 10 + z0*load[0][j] + z1*load[1][j] + 0.05*r.NormFloat64()
			if i >= n && j == 4 {
				v += 6 // off the factors' plane
			}
			t.Cols[j].Num[i] = v
		}
	}
	return t
}

func TestPCAModel(t *testing.T) {
	tbl := factorTable(1000, 5, 4)
	art := trainIgloo(t, ClassPCA, tbl, IglooArgs{Config: map[string]any{"pca_explained_variance_ratio": 0.98}})
	if k := len(art.Components.Model.Eigenvalues); k != 2 {
		t.Fatalf("kept %d components", k)
	}
	if r := metric(t, art, "total_explained_variance_ratio"); r < 0.98 {
		t.Errorf("explained %.4f", r)
	}
	info, _ := art.Method("PRINCIPAL_COMPONENT_INFO")
	if len(info.Rows) != 2 || info.Rows[1][3].(float64) < 0.98 || info.Rows[0][1].(float64) < info.Rows[1][1].(float64) {
		t.Errorf("PRINCIPAL_COMPONENT_INFO %v", info.Rows)
	}
	comps, _ := art.Method("PRINCIPAL_COMPONENTS")
	if len(comps.Rows) != 10 {
		t.Errorf("PRINCIPAL_COMPONENTS %v", comps.Rows)
	}
	for _, js := range scoreRows(t, art, "PREDICT", nil, head(tbl, 3)) {
		var p map[string]float64
		if err := json.Unmarshal([]byte(js), &p); err != nil || len(p) != 2 {
			t.Errorf("PREDICT %s: %v", js, err)
		}
	}
	flagged := 0
	for i, js := range scoreRows(t, art, "DETECT_ANOMALIES", map[string]any{"contamination": 0.01}, tbl) {
		if strings.Contains(js, `"is_anomaly":true`) {
			flagged++
		} else if i >= 1000 {
			t.Errorf("outlier %d: %s", i, js)
		}
	}
	if flagged > 20 {
		t.Errorf("%d flagged", flagged)
	}
	if e, err := art.EvaluateOn(head(tbl, 1000)); err != nil || e.Rows[0][0].(float64) < 0.98 {
		t.Errorf("EVALUATE on rows: %v, %v", e, err)
	}
	two := trainIgloo(t, ClassPCA, tbl, IglooArgs{Config: map[string]any{"num_principal_components": 3.0, "scale_features": false}})
	if len(two.Components.Model.Eigenvalues) != 3 {
		t.Errorf("num_principal_components 3 kept %d", len(two.Components.Model.Eigenvalues))
	}
	if _, _, err := TrainIgloo(ClassPCA, tbl, IglooArgs{}); err == nil || !strings.Contains(err.Error(), "one of num_principal_components") {
		t.Errorf("no number of components: %v", err)
	}
}
