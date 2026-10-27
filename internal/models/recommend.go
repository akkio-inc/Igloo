package models

import (
	"container/heap"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/akkio-inc/igloo/internal/ml"
)

// MATRIX_FACTORIZATION: users' ratings of items factorized by alternating
// least squares, as BigQuery's: explicit ratings with user and item biases,
// or implicit feedback (counts, minutes, purchases) as preferences weighted
// by confidence. Users and items are read as text.

// maxSeen bounds the interactions a model keeps, for RECOMMEND to leave
// out what a user already rated.
const maxSeen = 50_000_000

// Factors is an IGLOO.ML.MATRIX_FACTORIZATION model.
type Factors struct {
	UserCol   string `json:"user_col"`
	ItemCol   string `json:"item_col"`
	RatingCol string `json:"rating_col"`
	Implicit  bool   `json:"implicit"`
	// Users and Items are the factors' rows, sorted.
	Users []string `json:"users"`
	Items []string `json:"items"`
	Model *ml.ALS  `json:"model"`
	// SeenItems[SeenStart[u]:SeenStart[u+1]] are user u's rated items,
	// sorted, unless the input had more than maxSeen interactions.
	SeenStart ml.Int32s      `json:"seen_start,omitempty"`
	SeenItems ml.Int32s      `json:"seen_items,omitempty"`
	Eval      *Evaluation    `json:"eval"`
	Training  []Iteration    `json:"training"`
	Rows      int            `json:"rows"`
	Hyper     map[string]any `json:"hyperparameters"`

	userIndex, itemIndex map[string]int
	pool                 sync.Pool
}

func (f *Factors) prepare() {
	f.userIndex = make(map[string]int, len(f.Users))
	for i, u := range f.Users {
		f.userIndex[u] = i
	}
	f.itemIndex = make(map[string]int, len(f.Items))
	for i, it := range f.Items {
		f.itemIndex[it] = i
	}
}

func (f *Factors) summary(out map[string]any) {
	out["rows"], out["users"], out["items"] = f.Rows, len(f.Users), len(f.Items)
	addEval(out, f.Eval)
}

// interactions are ratings by user and item number.
type interactions struct {
	user, item []int32
	value      []float64
}

func trainFactorization(t *Table, a IglooArgs, cfg *config) (*Factors, error) {
	uc, ic, rc := t.Col(a.User), t.Col(a.Item), t.Col(a.Rating)
	switch {
	case uc == nil:
		return nil, inputErr("USER_COLNAME %s is not a column of INPUT_DATA", a.User)
	case ic == nil:
		return nil, inputErr("ITEM_COLNAME %s is not a column of INPUT_DATA", a.Item)
	case rc == nil:
		return nil, inputErr("RATING_COLNAME %s is not a column of INPUT_DATA", a.Rating)
	case rc.Type != TypeNumber:
		return nil, inputErr("RATING_COLNAME %s is of type %s; ratings are numbers", a.Rating, rc.TypeName)
	}
	feedback, err := cfg.str("feedback_type", "explicit", "explicit", "implicit")
	if err != nil {
		return nil, err
	}
	p := ml.ALSParams{Implicit: feedback == "implicit", Interrupt: a.Interrupt}
	if p.Factors, err = cfg.integer("num_factors", 16, 1, 256); err != nil {
		return nil, err
	}
	if p.L2, err = cfg.num("l2_reg", 1, 1e-9, 1e9); err != nil {
		return nil, err
	}
	if p.Iterations, err = cfg.integer("max_iterations", 15, 1, 1000); err != nil {
		return nil, err
	}
	if p.Alpha, err = cfg.num("wals_alpha", 40, 1e-9, 1e9); err != nil {
		return nil, err
	}
	if p.Seed, err = cfg.seed(); err != nil {
		return nil, err
	}
	if _, err := cfg.boolean("early_stop", true); err != nil {
		return nil, err
	}
	if _, err := cfg.num("min_rel_progress", 0, 0, 1); err != nil {
		return nil, err
	}
	// Each user's rating of an item once: the mean of explicit ratings,
	// the sum of implicit feedback.
	type pair struct{ u, i string }
	sums := map[pair][2]float64{}
	for r := range t.N {
		if uc.Null[r] || ic.Null[r] || rc.Null[r] || math.IsNaN(rc.Num[r]) {
			continue
		}
		k := pair{labelText(uc, r), labelText(ic, r)}
		s := sums[k]
		sums[k] = [2]float64{s[0] + rc.Num[r], s[1] + 1}
	}
	if len(sums) < 2 {
		return nil, inputErr("INPUT_DATA has fewer than two ratings with a user, an item and a rating")
	}
	pairs := make([]pair, 0, len(sums))
	userSet, itemSet := map[string]bool{}, map[string]bool{}
	for k := range sums {
		pairs = append(pairs, k)
		userSet[k.u], itemSet[k.i] = true, true
	}
	sort.Slice(pairs, func(a, b int) bool {
		if pairs[a].u != pairs[b].u {
			return pairs[a].u < pairs[b].u
		}
		return pairs[a].i < pairs[b].i
	})
	f := &Factors{UserCol: uc.Name, ItemCol: ic.Name, RatingCol: rc.Name, Implicit: p.Implicit,
		Users: sortedKeys(userSet), Items: sortedKeys(itemSet)}
	f.prepare()
	all := interactions{}
	for _, k := range pairs {
		s := sums[k]
		v := s[0]
		if !p.Implicit {
			v /= s[1]
		}
		all.user = append(all.user, int32(f.userIndex[k.u]))
		all.item = append(all.item, int32(f.itemIndex[k.i]))
		all.value = append(all.value, v)
	}
	train, eval, err := splitRows(cfg, len(all.user), nil, false, p.Seed)
	if err != nil {
		return nil, err
	}
	tr := all.take(train)
	m, info, err := ml.TrainALS(ml.Ratings{User: tr.user, Item: tr.item, Value: tr.value}, len(f.Users), len(f.Items), p)
	if err != nil {
		return nil, inputErr("%v", err)
	}
	f.Model, f.Rows = m, len(train)
	f.Hyper = map[string]any{"feedback_type": feedback, "num_factors": p.Factors, "l2_reg": p.L2, "max_iterations": p.Iterations}
	if p.Implicit {
		f.Hyper["wals_alpha"] = p.Alpha
	}
	for i, l := range info.Loss {
		f.Training = append(f.Training, Iteration{Iteration: i + 1, Loss: finiteOr(l, 0)})
	}
	if len(all.user) <= maxSeen {
		f.SeenStart, f.SeenItems = seenLists(all, len(f.Users))
	}
	if len(eval) > 0 {
		f.Eval = f.evaluate(all.take(eval), tr)
	}
	return f, nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (in interactions) take(rows []int) interactions {
	out := interactions{user: make([]int32, len(rows)), item: make([]int32, len(rows)), value: make([]float64, len(rows))}
	for k, r := range rows {
		out.user[k], out.item[k], out.value[k] = in.user[r], in.item[r], in.value[r]
	}
	return out
}

// seenLists groups interactions' items by user, sorted.
func seenLists(in interactions, users int) (ml.Int32s, ml.Int32s) {
	start := make(ml.Int32s, users+1)
	for _, u := range in.user {
		start[u+1]++
	}
	for u := range users {
		start[u+1] += start[u]
	}
	items := make(ml.Int32s, len(in.item))
	pos := slices.Clone(start[:users])
	for k, u := range in.user {
		items[pos[u]] = in.item[k]
		pos[u]++
	}
	for u := range users {
		slices.Sort(items[start[u]:start[u+1]])
	}
	return start, items
}

// seen returns user u's rated items, sorted, or nil.
func (f *Factors) seen(u int) []int32 {
	if u < 0 || len(f.SeenStart) == 0 {
		return nil
	}
	return f.SeenItems[f.SeenStart[u]:f.SeenStart[u+1]]
}

// evaluate measures the model on held-out interactions: explicit ratings'
// errors, as a regression's; implicit preferences' ranking, for up to 1,000
// users: mean average precision and NDCG of their held-out items among the
// items they did not interact with in training, and their mean percentile
// rank (0 is the top).
func (f *Factors) evaluate(held, train interactions) *Evaluation {
	n := len(held.user)
	pred := make([]float64, n)
	for k := range n {
		pred[k] = f.Model.Predict(int(held.user[k]), int(held.item[k]))
	}
	if !f.Implicit {
		return &Evaluation{Rows: n, Metrics: finiteMetrics(regressionMetrics(held.value, pred))}
	}
	var mse float64
	for k := range n {
		target := 0.0
		if held.value[k] > 0 {
			target = 1
		}
		mse += (target - pred[k]) * (target - pred[k])
	}
	mse /= float64(max(n, 1))
	trainStart, trainItems := seenLists(train, len(f.Users))
	heldStart, heldItems := seenLists(held, len(f.Users))
	var users []int
	for u := range len(f.Users) {
		if heldStart[u+1] > heldStart[u] {
			users = append(users, u)
		}
	}
	if len(users) > 1000 {
		step := (len(users) + 999) / 1000
		var picked []int
		for i := 0; i < len(users); i += step {
			picked = append(picked, users[i])
		}
		users = picked
	}
	type part struct{ ap, ndcg, rank float64 }
	parts := make([]part, len(users))
	parallelRows(len(users), func(lo, hi int) {
		scores := make([]float64, len(f.Items))
		for ui := lo; ui < hi; ui++ {
			u := users[ui]
			trained := trainItems[trainStart[u]:trainStart[u+1]]
			relevant := heldItems[heldStart[u]:heldStart[u+1]]
			var cands []int
			for i := range f.Items {
				if _, found := slices.BinarySearch(trained, int32(i)); found {
					continue
				}
				scores[i] = f.Model.Predict(u, i)
				cands = append(cands, i)
			}
			sort.SliceStable(cands, func(a, b int) bool { return scores[cands[a]] > scores[cands[b]] })
			var hits, ap, dcg, idcg, rank float64
			for pos, i := range cands {
				if _, found := slices.BinarySearch(relevant, int32(i)); found {
					hits++
					ap += hits / float64(pos+1)
					dcg += 1 / math.Log2(float64(pos+2))
					if len(cands) > 1 {
						rank += float64(pos) / float64(len(cands)-1)
					}
				}
			}
			for r := range int(hits) {
				idcg += 1 / math.Log2(float64(r+2))
			}
			if hits > 0 {
				parts[ui] = part{ap / hits, dcg / idcg, rank / hits}
			}
		}
	})
	var ap, ndcg, rank float64
	for _, p := range parts {
		ap, ndcg, rank = ap+p.ap, ndcg+p.ndcg, rank+p.rank
	}
	k := float64(max(len(users), 1))
	return &Evaluation{Rows: n, Metrics: []Metric{{"mean_average_precision", ap / k}, {"mean_squared_error", mse},
		{"normalized_discounted_cumulative_gain", ndcg / k}, {"average_rank", rank / k}}}
}

// --- row methods ---------------------------------------------------------

func (f *Factors) rowFeatures(method string) ([]Feature, bool) {
	user := Feature{Name: f.UserCol, Type: TypeString.String()}
	switch method {
	case "PREDICT":
		return []Feature{user, {Name: f.ItemCol, Type: TypeString.String()}}, true
	case "RECOMMEND":
		return []Feature{user}, true
	}
	return nil, false
}

func (f *Factors) rowOptions(method string, cfg *config) (*RowOptions, error) {
	opt := &RowOptions{}
	if method != "RECOMMEND" {
		return opt, nil
	}
	var err error
	if opt.TopK, err = cfg.integer("top_k", 10, 1, 10_000); err != nil {
		return nil, err
	}
	if opt.ExcludeSeen, err = cfg.boolean("exclude_seen", false); err != nil {
		return nil, err
	}
	if opt.ExcludeSeen && len(f.SeenStart) == 0 {
		return nil, inputErr("the model was trained on more than %d interactions and kept none: exclude_seen needs them", maxSeen)
	}
	return opt, nil
}

// scoreName is the predicted column's name, as BigQuery's.
func (f *Factors) scoreName() string {
	if f.Implicit {
		return sqlName("predicted_", f.RatingCol, "_confidence")
	}
	return sqlName("predicted_", f.RatingCol)
}

func (f *Factors) rowTable(method string, _ *RowOptions) RowTable {
	score := RowColumn{f.scoreName(), "$.value", "DOUBLE"}
	if method == "RECOMMEND" {
		return RowTable{Unnest: "$.recommendations", Columns: []RowColumn{{sqlName(f.ItemCol), "$.item", "VARCHAR"}, score}}
	}
	return RowTable{Columns: []RowColumn{score}}
}

func (f *Factors) index(m map[string]int, r *ml.RowValues, j int) int {
	if r.Missing[j] {
		return -1
	}
	if i, ok := m[r.Str[j]]; ok {
		return i
	}
	return -1
}

func (f *Factors) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte {
	u := f.index(f.userIndex, r, 0)
	if method == "PREDICT" {
		i := f.index(f.itemIndex, r, 1)
		buf = append(buf, `{"value":`...)
		if f.Implicit && (u < 0 || i < 0) {
			buf = append(buf, "null"...)
		} else {
			buf = appendNum(buf, f.Model.Predict(u, i))
		}
		return append(buf, '}')
	}
	buf = append(buf, `{"recommendations":[`...)
	for k, s := range f.topItems(u, opt) {
		if k > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"item":`...)
		buf = append(buf, jsonString(f.Items[s.item])...)
		buf = append(buf, `,"value":`...)
		buf = appendNum(buf, s.score)
		buf = append(buf, '}')
	}
	return append(buf, "]}"...)
}

type scored struct {
	item  int
	score float64
}

// minHeap keeps the best scores seen, the worst on top.
type minHeap []scored

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(a, b int) bool {
	if h[a].score != h[b].score {
		return h[a].score < h[b].score
	}
	return h[a].item > h[b].item
}
func (h minHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *minHeap) Push(x any)   { *h = append(*h, x.(scored)) }
func (h *minHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// topItems returns user u's best opt.TopK items, best first: for a user
// training never saw, an explicit model's by item bias, and none for an
// implicit one.
func (f *Factors) topItems(u int, opt *RowOptions) []scored {
	if u < 0 && f.Implicit {
		return nil
	}
	seen := f.seen(u)
	h := make(minHeap, 0, opt.TopK+1)
	next := 0
	for i := range f.Items {
		if opt.ExcludeSeen {
			for next < len(seen) && int(seen[next]) < i {
				next++
			}
			if next < len(seen) && int(seen[next]) == i {
				continue
			}
		}
		s := scored{i, f.Model.Predict(u, i)}
		if len(h) < opt.TopK {
			heap.Push(&h, s)
		} else if s.score > h[0].score || s.score == h[0].score && s.item < h[0].item {
			h[0] = s
			heap.Fix(&h, 0)
		}
	}
	out := make([]scored, len(h))
	for k := len(h) - 1; k >= 0; k-- {
		out[k] = heap.Pop(&h).(scored)
	}
	return out
}

// --- table methods ---------------------------------------------------------

func (f *Factors) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	switch method {
	case "EVALUATE":
		if f.Eval == nil {
			return nil, errNoEval()
		}
		res.Rows = append(res.Rows, evaluationRow(f.Eval, cols))
	case "TRAINING_INFO":
		for _, it := range f.Training {
			res.Rows = append(res.Rows, []any{int64(it.Iteration), it.Loss, nil, nil})
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

// evaluateOn measures the model on t's ratings, as on held-out ones.
func (f *Factors) evaluateOn(t *Table) (*Evaluation, error) {
	uc, ic, rc := t.Col(f.UserCol), t.Col(f.ItemCol), t.Col(f.RatingCol)
	if uc == nil || ic == nil || rc == nil || rc.Type != TypeNumber {
		return nil, inputErr("INPUT_DATA needs the columns %s, %s and %s (a number)", f.UserCol, f.ItemCol, f.RatingCol)
	}
	held := interactions{}
	for r := range t.N {
		if uc.Null[r] || ic.Null[r] || rc.Null[r] {
			continue
		}
		u, uok := f.userIndex[labelText(uc, r)]
		i, iok := f.itemIndex[labelText(ic, r)]
		if !uok || !iok {
			continue
		}
		held.user, held.item, held.value = append(held.user, int32(u)), append(held.item, int32(i)), append(held.value, rc.Num[r])
	}
	if len(held.user) == 0 {
		return nil, inputErr("INPUT_DATA has no ratings of users and items the model knows")
	}
	// Items a user rated in training are not candidates; those in INPUT_DATA
	// are the ones to find.
	given := make(map[[2]int32]bool, len(held.user))
	for k := range held.user {
		given[[2]int32{held.user[k], held.item[k]}] = true
	}
	train := interactions{}
	for u := range f.Users {
		for _, i := range f.seen(u) {
			if !given[[2]int32{int32(u), i}] {
				train.user, train.item, train.value = append(train.user, int32(u)), append(train.item, i), append(train.value, 1)
			}
		}
	}
	return f.evaluate(held, train), nil
}

// TrainingInputs returns, for RECOMMEND without INPUT_DATA, the users the
// model was trained on.
func (f *Factors) trainingInputs(method string) *Result {
	if method != "RECOMMEND" {
		return nil
	}
	res := &Result{Columns: []ResultColumn{{Name: f.UserCol, Kind: KindText}}, Rows: make([][]any, len(f.Users))}
	for i, u := range f.Users {
		res.Rows[i] = []any{u}
	}
	return res
}
