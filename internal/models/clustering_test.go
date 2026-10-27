package models

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

// segmentTable is a synthetic table of people in three planted segments
// by spend and visits, each mostly from one region, and outliers, the last
// rows, far from all of them.
func segmentTable(n, outliers int, seed uint64) *Table {
	r := rand.New(rand.NewPCG(seed, seed))
	centers := [][2]float64{{10, 1}, {50, 8}, {90, 2}}
	regions := []string{"north", "south", "east"}
	t := &Table{N: n + outliers}
	spend := &TableCol{Name: "spend", Type: TypeNumber, TypeName: "DOUBLE", Num: make([]float64, t.N), Null: make([]bool, t.N)}
	visits := &TableCol{Name: "visits", Type: TypeNumber, TypeName: "DOUBLE", Num: make([]float64, t.N), Null: make([]bool, t.N)}
	region := &TableCol{Name: "region", Type: TypeString, TypeName: "VARCHAR", Str: make([]string, t.N), Null: make([]bool, t.N)}
	for i := range t.N {
		k := i % 3
		c := centers[k]
		spend.Num[i], visits.Num[i] = c[0]+3*r.NormFloat64(), c[1]+0.5*r.NormFloat64()
		region.Str[i] = regions[k]
		if r.Float64() < 0.1 {
			region.Str[i] = regions[r.IntN(3)]
		}
		if i >= n {
			spend.Num[i], visits.Num[i] = 30+200*r.Float64(), 20+10*r.Float64()
		}
	}
	t.Cols = []*TableCol{spend, visits, region}
	return t
}

func TestKMeansModel(t *testing.T) {
	tbl := segmentTable(900, 10, 3)
	art := trainIgloo(t, ClassKMeans, tbl, IglooArgs{})
	if k := art.Clusters.Model.K; k != 3 {
		t.Fatalf("chose %d clusters (%v)", k, art.Clusters.Hyper)
	}
	again := trainIgloo(t, ClassKMeans, tbl, IglooArgs{})
	_, sha1, _ := art.Encode()
	_, sha2, _ := again.Encode()
	if sha1 != sha2 {
		t.Error("two trainings differ")
	}
	// The centroids, in the features' own units, sit on the planted
	// centers.
	cents, err := art.Method("CENTROIDS")
	if err != nil {
		t.Fatal(err)
	}
	found := map[int]bool{}
	for _, row := range cents.Rows {
		if row[1] != "spend" {
			continue
		}
		v := row[2].(float64)
		for k, want := range []float64{10, 50, 90} {
			if math.Abs(v-want) < 2 {
				found[k] = true
			}
		}
	}
	if len(found) != 3 {
		t.Errorf("centroids' spend: %v", cents.Rows)
	}
	// Rows of a segment share a centroid.
	preds := scoreRows(t, art, "PREDICT", nil, tbl)
	bySegment := map[int]map[int]int{}
	for i, js := range preds[:900] {
		var p struct {
			CentroidID int `json:"centroid_id"`
			Nearest    []struct {
				CentroidID int     `json:"centroid_id"`
				Distance   float64 `json:"distance"`
			} `json:"nearest_centroids_distance"`
		}
		if err := json.Unmarshal([]byte(js), &p); err != nil || len(p.Nearest) != 3 || p.Nearest[0].CentroidID != p.CentroidID ||
			p.Nearest[0].Distance > p.Nearest[1].Distance {
			t.Fatalf("PREDICT %s: %v", js, err)
		}
		if bySegment[i%3] == nil {
			bySegment[i%3] = map[int]int{}
		}
		bySegment[i%3][p.CentroidID]++
	}
	for seg, counts := range bySegment {
		if len(counts) != 1 {
			t.Errorf("segment %d spread over centroids %v", seg, counts)
		}
	}
	// The outliers are the anomalies, at a contamination of 1%.
	flagged := 0
	for i, js := range scoreRows(t, art, "DETECT_ANOMALIES", map[string]any{"contamination": 0.012}, tbl) {
		anomaly := strings.Contains(js, `"is_anomaly":true`)
		if anomaly {
			flagged++
		}
		if i >= 900 && !anomaly {
			t.Errorf("outlier %d not flagged: %s", i, js)
		}
	}
	if flagged > 15 {
		t.Errorf("%d rows flagged", flagged)
	}
	if _, err := art.RowMethod("DETECT_ANOMALIES", nil); err == nil || !strings.Contains(err.Error(), "contamination") {
		t.Errorf("DETECT_ANOMALIES without contamination: %v", err)
	}
	if db := metric(t, art, "davies_bouldin_index"); db > 0.6 {
		t.Errorf("Davies-Bouldin %.3f", db)
	}
	// Given the number of clusters, it is kept.
	two := trainIgloo(t, ClassKMeans, tbl, IglooArgs{Config: map[string]any{"num_clusters": 2.0, "standardize_features": false}})
	if two.Clusters.Model.K != 2 {
		t.Errorf("num_clusters 2 gave %d", two.Clusters.Model.K)
	}
	if _, _, err := TrainIgloo(ClassKMeans, tbl, IglooArgs{Config: map[string]any{"distance_type": "cosine"}}); err == nil {
		t.Error("cosine distance was accepted")
	}
}
