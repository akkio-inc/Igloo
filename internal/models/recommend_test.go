package models

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

// ratingsTable is a synthetic table of users' ratings of items, from two
// planted factors each, a little noise, and some 40% of pairs rated.
func ratingsTable(users, items int, implicit bool, seed uint64) (*Table, func(u, i int) float64) {
	r := rand.New(rand.NewPCG(seed, seed))
	uf, vf := make([][2]float64, users), make([][2]float64, items)
	for u := range uf {
		uf[u] = [2]float64{r.NormFloat64(), r.NormFloat64()}
	}
	for i := range vf {
		vf[i] = [2]float64{r.NormFloat64(), r.NormFloat64()}
	}
	truth := func(u, i int) float64 { return 3 + uf[u][0]*vf[i][0] + uf[u][1]*vf[i][1] }
	t := &Table{}
	user := &TableCol{Name: "person_id", Type: TypeString, TypeName: "VARCHAR"}
	item := &TableCol{Name: "campaign", Type: TypeString, TypeName: "VARCHAR"}
	rating := &TableCol{Name: "rating", Type: TypeNumber, TypeName: "DOUBLE"}
	for u := range users {
		for i := range items {
			v := truth(u, i) + 0.1*r.NormFloat64()
			if implicit {
				// Two groups of users, each clicking its own half of the items.
				if (u%2 == 0) != (i < items/2) || r.Float64() > 0.4 {
					continue
				}
				v = float64(1 + r.IntN(3))
			} else if r.Float64() > 0.4 {
				continue
			}
			user.Str, item.Str = append(user.Str, "p"+strconv.Itoa(u)), append(item.Str, "c"+strconv.Itoa(i))
			rating.Num = append(rating.Num, v)
			t.N++
		}
	}
	user.Null, item.Null, rating.Null = make([]bool, t.N), make([]bool, t.N), make([]bool, t.N)
	t.Cols = []*TableCol{user, item, rating}
	return t, truth
}

func mfArgs(cfg map[string]any) IglooArgs {
	return IglooArgs{User: "person_id", Item: "campaign", Rating: "rating", Config: cfg}
}

func TestMatrixFactorization(t *testing.T) {
	tbl, truth := ratingsTable(200, 60, false, 5)
	art := trainIgloo(t, ClassFactorization, tbl, mfArgs(map[string]any{"num_factors": 4.0, "l2_reg": 0.05,
		"data_split_method": "random", "data_split_eval_fraction": 0.1}))
	if rmse := math.Sqrt(metric(t, art, "mean_squared_error")); rmse > 0.35 {
		t.Errorf("held-out RMSE %.3f", rmse)
	}
	again := trainIgloo(t, ClassFactorization, tbl, mfArgs(map[string]any{"num_factors": 4.0, "l2_reg": 0.05,
		"data_split_method": "random", "data_split_eval_fraction": 0.1}))
	_, sha1, _ := art.Encode()
	_, sha2, _ := again.Encode()
	if sha1 != sha2 {
		t.Error("two trainings differ")
	}
	// A pair's prediction is near its planted rating, rated or not.
	pairs := &Table{N: 3, Cols: []*TableCol{
		{Name: "person_id", Type: TypeString, Str: []string{"p3", "p7", "nobody"}, Null: make([]bool, 3)},
		{Name: "campaign", Type: TypeString, Str: []string{"c5", "c9", "c1"}, Null: make([]bool, 3)}}}
	preds := scoreRows(t, art, "PREDICT", nil, pairs)
	for k, want := range []float64{truth(3, 5), truth(7, 9)} {
		var p struct{ Value float64 }
		if err := json.Unmarshal([]byte(preds[k]), &p); err != nil || math.Abs(p.Value-want) > 0.5 {
			t.Errorf("PREDICT %s, planted %.3f", preds[k], want)
		}
	}
	if !strings.Contains(preds[2], `"value":`) || strings.Contains(preds[2], "null") {
		t.Errorf("an unknown user's prediction: %s", preds[2])
	}
	// RECOMMEND ranks a user's items by their planted ratings.
	users := &Table{N: 1, Cols: []*TableCol{{Name: "person_id", Type: TypeString, Str: []string{"p11"}, Null: []bool{false}}}}
	rec := scoreRows(t, art, "RECOMMEND", map[string]any{"top_k": 5.0}, users)[0]
	var r struct {
		Recommendations []struct {
			Item  string  `json:"item"`
			Value float64 `json:"value"`
		} `json:"recommendations"`
	}
	if err := json.Unmarshal([]byte(rec), &r); err != nil || len(r.Recommendations) != 5 {
		t.Fatalf("RECOMMEND %s: %v", rec, err)
	}
	best := 0.0
	for i := range 60 {
		best = math.Max(best, truth(11, i))
	}
	top, _ := strconv.Atoi(strings.TrimPrefix(r.Recommendations[0].Item, "c"))
	if truth(11, top) < best-0.5 || r.Recommendations[0].Value < r.Recommendations[4].Value {
		t.Errorf("recommended %v; the best planted rating is %.3f", r.Recommendations, best)
	}
	// exclude_seen leaves out what p11 rated.
	rated := map[string]bool{}
	for i := range tbl.N {
		if tbl.Cols[0].Str[i] == "p11" {
			rated[tbl.Cols[1].Str[i]] = true
		}
	}
	rec = scoreRows(t, art, "RECOMMEND", map[string]any{"top_k": 30.0, "exclude_seen": true}, users)[0]
	if err := json.Unmarshal([]byte(rec), &r); err != nil || len(r.Recommendations) != min(30, 60-len(rated)) {
		t.Fatalf("RECOMMEND excluding seen %s: %v", rec, err)
	}
	for _, x := range r.Recommendations {
		if rated[x.Item] {
			t.Errorf("recommended %s, which p11 rated", x.Item)
		}
	}
	if res := art.TrainingInputs("RECOMMEND"); res == nil || len(res.Rows) != 200 {
		t.Error("RECOMMEND's users without INPUT_DATA")
	}
	info, _ := art.Method("TRAINING_INFO")
	if len(info.Rows) != 15 || info.Rows[14][1].(float64) >= info.Rows[0][1].(float64) {
		t.Errorf("TRAINING_INFO %v", info.Rows)
	}
}

func TestImplicitFactorization(t *testing.T) {
	tbl, _ := ratingsTable(200, 60, true, 6)
	art := trainIgloo(t, ClassFactorization, tbl, mfArgs(map[string]any{"feedback_type": "implicit", "num_factors": 4.0,
		"l2_reg": 0.1, "data_split_method": "random"}))
	// A held-out item is one of some 19 of its group's a user did not
	// click, which the model cannot tell apart: about 0.2 is the best mean
	// average precision, and a random ranking's about 0.08.
	if m := metric(t, art, "mean_average_precision"); m < 0.15 {
		t.Errorf("mean average precision %.3f", m)
	}
	if r := metric(t, art, "average_rank"); r > 0.3 {
		t.Errorf("average rank %.3f", r)
	}
	for u := range 4 {
		users := &Table{N: 1, Cols: []*TableCol{{Name: "person_id", Type: TypeString, Str: []string{fmt.Sprint("p", u)}, Null: []bool{false}}}}
		var r struct {
			Recommendations []struct{ Item string } `json:"recommendations"`
		}
		js := scoreRows(t, art, "RECOMMEND", map[string]any{"exclude_seen": true}, users)[0]
		if err := json.Unmarshal([]byte(js), &r); err != nil || len(r.Recommendations) != 10 {
			t.Fatalf("%s: %v", js, err)
		}
		for _, x := range r.Recommendations {
			i, _ := strconv.Atoi(strings.TrimPrefix(x.Item, "c"))
			if (u%2 == 0) != (i < 30) {
				t.Errorf("user %d is recommended %s, of the other group's items", u, x.Item)
			}
		}
	}
	none := &Table{N: 1, Cols: []*TableCol{{Name: "person_id", Type: TypeString, Str: []string{"nobody"}, Null: []bool{false}}}}
	if js := scoreRows(t, art, "RECOMMEND", nil, none)[0]; js != `{"recommendations":[]}` {
		t.Errorf("an unknown user: %s", js)
	}
}
