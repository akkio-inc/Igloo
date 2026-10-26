package ml

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
)

// Objective is what a model's trees minimize.
type Objective string

const (
	// Regression minimizes squared error.
	Regression Objective = "regression"
	// Quantile minimizes the pinball loss of the Alpha quantile, for
	// prediction intervals.
	Quantile Objective = "quantile"
	// Binary minimizes the log loss of two classes, labeled 0 and 1.
	Binary Objective = "binary"
	// Multiclass minimizes the log loss of Classes classes, labeled 0 to
	// Classes-1, with a tree per class each round.
	Multiclass Objective = "multiclass"
)

// Params configure training. Zero values mean the defaults noted.
type Params struct {
	Objective Objective
	// Classes is the number of classes of a multiclass model.
	Classes int
	// Alpha is the quantile a Quantile model predicts (default 0.5).
	Alpha float64
	// Rounds bounds the boosting rounds (default 100).
	Rounds int
	// LearningRate shrinks each tree (default 0.1).
	LearningRate float64
	// NumLeaves bounds each tree's leaves (default 31), MaxDepth its depth
	// (default 12).
	NumLeaves, MaxDepth int
	// MinDataInLeaf is the fewest training rows a leaf may hold (default
	// 20, or less for small data), MinHessian the least sum of hessians.
	MinDataInLeaf int
	MinHessian    float64
	// Lambda is the L2 penalty on leaf values (default 1).
	Lambda float64
	// FeatureFraction and BaggingFraction sample features and rows for
	// each tree (default 1: all of them).
	FeatureFraction, BaggingFraction float64
	// MinCategoryCount is how often a category must occur to get a bin of
	// its own (default 5); rarer ones share one.
	MinCategoryCount int
	// EarlyStopping stops when the validation loss has not improved for
	// that many rounds, and keeps the best round's trees; 0 never stops
	// early. It needs validation rows.
	EarlyStopping int
	// Seed seeds the sampling of rows, features and bins: the same rows in
	// the same order and the same seed give the same model.
	Seed uint64
	// Threads bounds the goroutines training uses (default one per CPU).
	// The model does not depend on it.
	Threads int
}

func (p Params) withDefaults(n int) Params {
	if p.Rounds <= 0 {
		p.Rounds = 100
	}
	if p.LearningRate <= 0 {
		p.LearningRate = 0.1
	}
	if p.NumLeaves <= 1 {
		p.NumLeaves = 31
	}
	if p.MaxDepth <= 0 {
		p.MaxDepth = 12
	}
	if p.MinDataInLeaf <= 0 {
		p.MinDataInLeaf = min(20, max(1, n/20))
	}
	if p.MinHessian <= 0 {
		p.MinHessian = 1e-3
	}
	if p.Lambda <= 0 {
		p.Lambda = 1
	}
	if p.FeatureFraction <= 0 || p.FeatureFraction > 1 {
		p.FeatureFraction = 1
	}
	if p.BaggingFraction <= 0 || p.BaggingFraction > 1 {
		p.BaggingFraction = 1
	}
	if p.MinCategoryCount <= 0 {
		p.MinCategoryCount = 5
	}
	if p.Objective == Quantile && (p.Alpha <= 0 || p.Alpha >= 1) {
		p.Alpha = 0.5
	}
	return p
}

// GBM is a gradient-boosted tree model.
type GBM struct {
	Objective Objective `json:"objective"`
	// Classes is the number of classes of a multiclass model.
	Classes int     `json:"classes,omitempty"`
	Alpha   float64 `json:"alpha,omitempty"`
	// Features bin the model's features, in the order its trees number
	// them.
	Features []*Binner `json:"features"`
	// Init is the starting raw score, one per class for multiclass.
	Init  []float64 `json:"init"`
	Trees []Tree    `json:"trees"`
}

// TrainInfo is what training learned besides the model.
type TrainInfo struct {
	// Rounds is the number of rounds the model kept.
	Rounds int
	// ValidLoss is the validation loss after each round, if there were
	// validation rows.
	ValidLoss []float64
}

// outputs is the number of raw scores the model gives each row.
func (m *GBM) outputs() int {
	if m.Objective == Multiclass {
		return m.Classes
	}
	return 1
}

// Prepare readies a model for prediction from several goroutines.
func (m *GBM) Prepare() {
	for _, b := range m.Features {
		b.prepare()
	}
	for i := range m.Trees {
		m.Trees[i].prepare()
	}
}

// TrainGBM trains a model on x and labels y: numbers for Regression and
// Quantile, class numbers for Binary and Multiclass. valid and validY, if
// given, are held-out rows for early stopping. Rows should come in a
// canonical order (Frame.CanonicalOrder) for the model to be repeatable.
func TrainGBM(x *Frame, y []float64, valid *Frame, validY []float64, p Params) (*GBM, TrainInfo, error) {
	if err := x.Check(); err != nil {
		return nil, TrainInfo{}, err
	}
	if x.N == 0 {
		return nil, TrainInfo{}, errors.New("no rows to train on")
	}
	if len(y) != x.N {
		return nil, TrainInfo{}, fmt.Errorf("%d labels for %d rows", len(y), x.N)
	}
	if len(x.Cols) == 0 {
		return nil, TrainInfo{}, errors.New("no features to train on")
	}
	p = p.withDefaults(x.N)
	switch p.Objective {
	case Regression, Quantile, Binary:
	case Multiclass:
		if p.Classes < 2 || p.Classes > maxBins-1 {
			return nil, TrainInfo{}, fmt.Errorf("a multiclass model needs 2 to %d classes, not %d", maxBins-1, p.Classes)
		}
	default:
		return nil, TrainInfo{}, fmt.Errorf("unknown objective %q", p.Objective)
	}
	for _, v := range y {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, TrainInfo{}, errors.New("labels must be finite numbers")
		}
		if p.Objective == Binary && v != 0 && v != 1 || p.Objective == Multiclass && (v < 0 || v >= float64(p.Classes) || v != math.Trunc(v)) {
			return nil, TrainInfo{}, fmt.Errorf("label %v is not a class number", v)
		}
	}
	m := &GBM{Objective: p.Objective, Alpha: p.Alpha}
	if p.Objective == Multiclass {
		m.Classes = p.Classes
	}
	if p.Objective != Quantile {
		m.Alpha = 0
	}
	m.Features = make([]*Binner, len(x.Cols))
	parallelFor(len(x.Cols), p.Threads, func(j int) {
		m.Features[j] = newBinner(&x.Cols[j], p.MinCategoryCount, p.Seed^uint64(j))
	})
	data := binFrame(x, m.Features)
	var vdata *binned
	if valid != nil && valid.N > 0 {
		vb, err := m.binFrame(valid)
		if err != nil {
			return nil, TrainInfo{}, err
		}
		vdata = vb
	}
	m.Init = initScore(p, y)
	t := &trainer{p: p, m: m, data: data, y: y}
	return t.run(vdata, validY)
}

// initScore is the raw score training starts from: the mean, the
// quantile, or the log odds of the classes.
func initScore(p Params, y []float64) []float64 {
	switch p.Objective {
	case Regression:
		return []float64{mean(y)}
	case Quantile:
		return []float64{quantile(slices.Clone(y), p.Alpha)}
	case Binary:
		pos := 0.0
		for _, v := range y {
			pos += v
		}
		q := clamp(pos/float64(len(y)), 1e-6, 1-1e-6)
		return []float64{math.Log(q / (1 - q))}
	}
	counts := make([]float64, p.Classes)
	for _, v := range y {
		counts[int(v)]++
	}
	out := make([]float64, p.Classes)
	for k, c := range counts {
		out[k] = math.Log(clamp(c/float64(len(y)), 1e-6, 1))
	}
	return out
}

// trainer holds training's state.
type trainer struct {
	p    Params
	m    *GBM
	data *binned
	y    []float64
	// f holds the raw scores of every row, class-major.
	f []float64
	// g and h are gradients and hessians of the current class.
	g, h []float64
}

func (t *trainer) run(vdata *binned, vy []float64) (*GBM, TrainInfo, error) {
	n, K := t.data.n, t.m.outputs()
	t.f = make([]float64, n*K)
	for k := range K {
		for i := range n {
			t.f[k*n+i] = t.m.Init[k]
		}
	}
	var vf []float64
	if vdata != nil {
		vf = make([]float64, vdata.n*K)
		for k := range K {
			for i := range vdata.n {
				vf[k*vdata.n+i] = t.m.Init[k]
			}
		}
	}
	gr := newGrower(t.p, t.m.Features, t.data)
	t.g, t.h = make([]float64, n), make([]float64, n)
	var info TrainInfo
	best, bestRound := math.Inf(1), 0
	grads := make([][2][]float64, 0)
	if K > 1 {
		grads = make([][2][]float64, K)
		for k := range K {
			grads[k] = [2][]float64{make([]float64, n), make([]float64, n)}
		}
	}
	for round := range t.p.Rounds {
		if K > 1 {
			// Every class's gradients from this round's scores, before any
			// of its trees changes them.
			t.softmaxGradients(grads)
		}
		for k := range K {
			if K > 1 {
				t.g, t.h = grads[k][0], grads[k][1]
			} else {
				t.gradients()
			}
			tree := gr.grow(t.g, t.h, uint64(round*K+k))
			tree.Class = k
			if t.p.Objective == Quantile {
				t.renewQuantiles(gr, &tree)
			}
			t.apply(gr, &tree, k)
			if vdata != nil {
				parallelFor(numChunks(vdata.n), t.p.Threads, func(c int) {
					lo, hi := chunkRange(c, vdata.n)
					for i := lo; i < hi; i++ {
						vf[k*vdata.n+i] += tree.Nodes[tree.leafOfCol(vdata, i)].Value
					}
				})
			}
			t.m.Trees = append(t.m.Trees, tree)
		}
		if vdata != nil {
			loss := t.loss(vf, vy, vdata.n)
			info.ValidLoss = append(info.ValidLoss, loss)
			if loss < best-1e-12 {
				best, bestRound = loss, round+1
			} else if t.p.EarlyStopping > 0 && round+1-bestRound >= t.p.EarlyStopping {
				break
			}
		}
	}
	info.Rounds = len(t.m.Trees) / K
	if vdata != nil && t.p.EarlyStopping > 0 && bestRound > 0 {
		t.m.Trees = t.m.Trees[:bestRound*K]
		info.Rounds = bestRound
	}
	t.m.Prepare()
	return t.m, info, nil
}

// gradients computes the gradients of a single-output objective.
func (t *trainer) gradients() {
	n := t.data.n
	parallelFor(numChunks(n), t.p.Threads, func(c int) {
		lo, hi := chunkRange(c, n)
		for i := lo; i < hi; i++ {
			f, y := t.f[i], t.y[i]
			switch t.p.Objective {
			case Regression:
				t.g[i], t.h[i] = f-y, 1
			case Quantile:
				if y > f {
					t.g[i] = -t.p.Alpha
				} else {
					t.g[i] = 1 - t.p.Alpha
				}
				t.h[i] = 1
			case Binary:
				q := sigmoid(f)
				t.g[i], t.h[i] = q-y, math.Max(q*(1-q), 1e-16)
			}
		}
	})
}

// softmaxGradients computes every class's gradients of the multiclass log
// loss.
func (t *trainer) softmaxGradients(grads [][2][]float64) {
	n, K := t.data.n, t.m.Classes
	factor := float64(K) / float64(K-1)
	parallelFor(numChunks(n), t.p.Threads, func(c int) {
		lo, hi := chunkRange(c, n)
		q := make([]float64, K)
		for i := lo; i < hi; i++ {
			for k := range K {
				q[k] = t.f[k*n+i]
			}
			softmax(q)
			for k := range K {
				y := 0.0
				if int(t.y[i]) == k {
					y = 1
				}
				grads[k][0][i] = q[k] - y
				grads[k][1][i] = math.Max(factor*q[k]*(1-q[k]), 1e-16)
			}
		}
	})
}

// renewQuantiles sets a quantile tree's leaves to the Alpha quantile of
// their rows' residuals, as LightGBM does: gradient steps of a constant
// size would take many rounds to reach it.
func (t *trainer) renewQuantiles(gr *grower, tree *Tree) {
	for _, l := range gr.leaves {
		res := make([]float64, 0, l.end-l.start)
		for _, r := range gr.idx[l.start:l.end] {
			res = append(res, t.y[r]-t.f[r])
		}
		if len(res) > 0 {
			tree.Nodes[l.node].Value = quantile(res, t.p.Alpha) * t.p.LearningRate
		}
	}
}

// apply adds a new tree of class k to the training rows' scores.
func (t *trainer) apply(gr *grower, tree *Tree, k int) {
	n := t.data.n
	f := t.f[k*n : (k+1)*n]
	if gr.bagged {
		parallelFor(numChunks(n), t.p.Threads, func(c int) {
			lo, hi := chunkRange(c, n)
			for i := lo; i < hi; i++ {
				f[i] += tree.Nodes[tree.leafOfCol(t.data, i)].Value
			}
		})
		return
	}
	parallelFor(len(gr.leaves), t.p.Threads, func(i int) {
		l := gr.leaves[i]
		v := tree.Nodes[l.node].Value
		for _, r := range gr.idx[l.start:l.end] {
			f[r] += v
		}
	})
}

// loss is the objective's loss on rows of raw scores f.
func (t *trainer) loss(f, y []float64, n int) float64 {
	var sum float64
	K := t.m.outputs()
	q := make([]float64, K)
	for i := range n {
		switch t.p.Objective {
		case Regression:
			d := f[i] - y[i]
			sum += d * d
		case Quantile:
			sum += pinball(y[i], f[i], t.p.Alpha)
		case Binary:
			sum += logLoss1(y[i], sigmoid(f[i]))
		case Multiclass:
			for k := range K {
				q[k] = f[k*n+i]
			}
			softmax(q)
			sum -= math.Log(math.Max(q[int(y[i])], 1e-15))
		}
	}
	return sum / float64(max(n, 1))
}

// binFrame bins a frame's columns as the model's features, matching them
// by name; a feature the frame lacks is missing in every row.
func (m *GBM) binFrame(f *Frame) (*binned, error) {
	out := &binned{n: f.N, bins: make([][]uint8, len(m.Features))}
	for j, b := range m.Features {
		c := f.Col(b.Name)
		col := make([]uint8, f.N)
		switch {
		case c == nil:
		case c.Kind != b.Kind:
			return nil, fmt.Errorf("feature %s was %s in training, not %s", b.Name, b.Kind, c.Kind)
		case c.Kind == Numeric:
			for i, v := range c.Num {
				col[i] = b.NumBin(v)
			}
		default:
			lookup := make([]uint8, len(c.Levels))
			for i, l := range c.Levels {
				lookup[i] = b.CatBin(l)
			}
			for i, code := range c.Codes {
				if code >= 0 {
					col[i] = lookup[code]
				}
			}
		}
		out.bins[j] = col
	}
	return out, nil
}

// Raw returns the raw scores of a frame's rows: row i's score of class k
// is out[k*f.N+i].
func (m *GBM) Raw(f *Frame) ([]float64, error) {
	b, err := m.binFrame(f)
	if err != nil {
		return nil, err
	}
	return m.rawBinned(b), nil
}

func (m *GBM) rawBinned(b *binned) []float64 {
	K := m.outputs()
	out := make([]float64, b.n*K)
	parallelFor(numChunks(b.n), 0, func(c int) {
		lo, hi := chunkRange(c, b.n)
		row := make([]uint8, len(m.Features))
		for i := lo; i < hi; i++ {
			for j := range row {
				row[j] = b.bins[j][i]
			}
			for k := range K {
				out[k*b.n+i] = m.Init[k]
			}
			for ti := range m.Trees {
				tr := &m.Trees[ti]
				out[tr.Class*b.n+i] += tr.predict(row)
			}
		}
	})
	return out
}

// RawRow returns the raw scores of one row of bins into out.
func (m *GBM) RawRow(row []uint8, out []float64) {
	copy(out, m.Init)
	for ti := range m.Trees {
		tr := &m.Trees[ti]
		out[tr.Class] += tr.predict(row)
	}
}

// Predict returns predictions for a frame's rows: values for Regression
// and Quantile, the probability of class 1 for Binary, and class
// probabilities (row i's of class k at k*f.N+i) for Multiclass.
func (m *GBM) Predict(f *Frame) ([]float64, error) {
	raw, err := m.Raw(f)
	if err != nil {
		return nil, err
	}
	m.Transform(raw, f.N)
	return raw, nil
}

// Transform turns raw scores of n rows into predictions in place.
func (m *GBM) Transform(raw []float64, n int) {
	switch m.Objective {
	case Binary:
		for i, v := range raw {
			raw[i] = sigmoid(v)
		}
	case Multiclass:
		K := m.Classes
		q := make([]float64, K)
		for i := range n {
			for k := range K {
				q[k] = raw[k*n+i]
			}
			softmax(q)
			for k := range K {
				raw[k*n+i] = q[k]
			}
		}
	}
}

// Importance returns each feature's share of the gain of every split.
func (m *GBM) Importance() []float64 {
	out := make([]float64, len(m.Features))
	var total float64
	for _, t := range m.Trees {
		for _, n := range t.Nodes {
			if !n.IsLeaf() {
				out[n.Feature] += n.Gain
				total += n.Gain
			}
		}
	}
	if total > 0 {
		for i := range out {
			out[i] /= total
		}
	}
	return out
}

// --- growing trees ---------------------------------------------------------

// binStat is a bin's sums of gradients and hessians, and its row count.
type binStat struct {
	g, h float64
	n    int64
}

// split is the best way found to split a leaf.
type split struct {
	feature     int
	gain        float64
	threshold   uint8
	missingLeft bool
	cats        []uint8
	// left's sums.
	lg, lh float64
	ln     int64
}

// growLeaf is a leaf of the tree being grown: rows idx[start:end].
type growLeaf struct {
	start, end int
	g, h       float64
	depth      int
	node       int
	hist       []binStat
	best       *split
}

// grower grows trees on binned data.
type grower struct {
	p       Params
	binners []*Binner
	data    *binned
	nbins   []int
	cat     []bool
	// idx holds the rows of the tree's leaves, each leaf's contiguous.
	idx, scratch []int32
	// gbuf and hbuf hold a leaf's gradients in row order.
	gbuf, hbuf []float64
	bagged     bool
	// leaves are the finished tree's leaves.
	leaves []*growLeaf
	pool   [][]binStat
	// partial holds chunks' sums for build.
	partial []binStat
	use     []bool
	g, h    []float64
}

func newGrower(p Params, binners []*Binner, data *binned) *grower {
	gr := &grower{p: p, binners: binners, data: data, nbins: make([]int, len(binners)), cat: make([]bool, len(binners)),
		idx: make([]int32, data.n), scratch: make([]int32, data.n), gbuf: make([]float64, data.n), hbuf: make([]float64, data.n),
		bagged: p.BaggingFraction < 1, use: make([]bool, len(binners))}
	for j, b := range binners {
		gr.nbins[j] = b.NumBins()
		gr.cat[j] = b.Kind == Categorical
	}
	return gr
}

func (gr *grower) histSize() int { return len(gr.binners) * maxBins }

func (gr *grower) newHist() []binStat {
	if k := len(gr.pool); k > 0 {
		h := gr.pool[k-1]
		gr.pool = gr.pool[:k-1]
		return h
	}
	return make([]binStat, gr.histSize())
}

func (gr *grower) freeHist(h []binStat) {
	if h != nil {
		gr.pool = append(gr.pool, h)
	}
}

// grow grows one tree for gradients g and hessians h; salt varies the
// sampling between trees.
func (gr *grower) grow(g, h []float64, salt uint64) Tree {
	p := gr.p
	gr.g, gr.h = g, h
	n := gr.data.n
	m := 0
	for i := range n {
		if gr.bagged && unitHash(p.Seed, 0xba9+salt, i) >= p.BaggingFraction {
			continue
		}
		gr.idx[m] = int32(i)
		m++
	}
	if m == 0 {
		gr.idx[0], m = 0, 1
	}
	some := false
	for j := range gr.use {
		gr.use[j] = gr.nbins[j] > 2 && (p.FeatureFraction >= 1 || unitHash(p.Seed, 0xfea+salt, j) < p.FeatureFraction)
		some = some || gr.use[j]
	}
	if !some {
		for j := range gr.use {
			gr.use[j] = gr.nbins[j] > 2
		}
	}
	var sg, sh float64
	for c := range numChunks(m) {
		lo, hi := chunkRange(c, m)
		var cg, ch float64
		for _, r := range gr.idx[lo:hi] {
			cg += g[r]
			ch += h[r]
		}
		sg += cg
		sh += ch
	}
	root := &growLeaf{start: 0, end: m, g: sg, h: sh, node: 0, hist: gr.newHist()}
	gr.build(root.hist, 0, m)
	tree := Tree{Nodes: []Node{{Feature: -1, Cover: float64(m)}}}
	gr.findBest(root)
	leaves := []*growLeaf{root}
	for len(leaves) < p.NumLeaves {
		bi := -1
		for i, l := range leaves {
			if l.best != nil && (bi < 0 || l.best.gain > leaves[bi].best.gain) {
				bi = i
			}
		}
		if bi < 0 {
			break
		}
		l := leaves[bi]
		s := l.best
		left, right := gr.splitLeaf(&tree, l, s)
		leaves[bi] = left
		leaves = append(leaves, right)
	}
	for _, l := range leaves {
		tree.Nodes[l.node].Value = -l.g / (l.h + p.Lambda) * p.LearningRate
		gr.freeHist(l.hist)
		l.hist = nil
	}
	gr.leaves = leaves
	return tree
}

// splitLeaf splits leaf l by s into two new leaves.
func (gr *grower) splitLeaf(tree *Tree, l *growLeaf, s *split) (*growLeaf, *growLeaf) {
	node := Node{Feature: int32(s.feature), Threshold: s.threshold, MissingLeft: s.missingLeft, Cats: s.cats,
		Cover: float64(l.end - l.start), Gain: s.gain}
	node.prepare()
	nl := gr.partition(&node, l.start, l.end)
	nr := l.end - l.start - nl
	li, ri := len(tree.Nodes), len(tree.Nodes)+1
	node.LeftChild, node.RightChild = int32(li), int32(ri)
	tree.Nodes[l.node] = node
	tree.Nodes = append(tree.Nodes, Node{Feature: -1, Cover: float64(nl)}, Node{Feature: -1, Cover: float64(nr)})
	a := &growLeaf{start: l.start, end: l.start + nl, g: s.lg, h: s.lh, depth: l.depth + 1, node: li}
	b := &growLeaf{start: l.start + nl, end: l.end, g: l.g - s.lg, h: l.h - s.lh, depth: l.depth + 1, node: ri}
	small, large := a, b
	if nr < nl {
		small, large = b, a
	}
	small.hist = gr.newHist()
	gr.build(small.hist, small.start, small.end)
	large.hist = l.hist
	l.hist = nil
	for j := range gr.binners {
		if !gr.use[j] {
			continue
		}
		off := j * maxBins
		for k := range gr.nbins[j] {
			ps, ss := &large.hist[off+k], &small.hist[off+k]
			ps.g -= ss.g
			ps.h -= ss.h
			ps.n -= ss.n
		}
	}
	gr.findBest(a)
	gr.findBest(b)
	return a, b
}

// histChunk is how many rows one task of build sums, and histGroup how
// many features: a task reads its rows' gradients once for several
// features, while they are in cache.
const (
	histChunk = 1 << 16
	histGroup = 8
)

// partition orders rows idx[start:end] with those going left first, each
// side in the order it had, and returns how many go left. Chunks of rows
// are split at once, then put in place.
func (gr *grower) partition(node *Node, start, end int) int {
	col := gr.data.bins[node.Feature]
	rows := gr.idx[start:end]
	scratch := gr.scratch[start:end]
	m := len(rows)
	nc := numChunks(m)
	lefts := make([]int, nc)
	threads := gr.p.Threads
	if nc < 4 {
		threads = 1
	}
	parallelFor(nc, threads, func(c int) {
		lo, hi := chunkRange(c, m)
		nl, nr := 0, 0
		for k := lo; k < hi; k++ {
			if r := rows[k]; node.goesLeft(col[r]) {
				rows[lo+nl] = r
				nl++
			} else {
				scratch[lo+nr] = r
				nr++
			}
		}
		copy(rows[lo+nl:hi], scratch[lo:lo+nr])
		lefts[c] = nl
	})
	if nc == 1 {
		return lefts[0]
	}
	total := 0
	for _, n := range lefts {
		total += n
	}
	leftAt, rightAt := make([]int, nc), make([]int, nc)
	l, r := 0, total
	for c := range nc {
		lo, hi := chunkRange(c, m)
		leftAt[c], rightAt[c] = l, r
		l += lefts[c]
		r += hi - lo - lefts[c]
	}
	parallelFor(nc, threads, func(c int) {
		lo, hi := chunkRange(c, m)
		copy(scratch[leftAt[c]:], rows[lo:lo+lefts[c]])
		copy(scratch[rightAt[c]:], rows[lo+lefts[c]:hi])
	})
	copy(rows, scratch)
	return total
}

// build fills hist with the sums of rows idx[start:end]. Large leaves are
// summed a fixed chunk of rows at a time and the chunks' sums added in
// order, so the sums do not depend on the number of threads.
func (gr *grower) build(hist []binStat, start, end int) {
	rows := gr.idx[start:end]
	m := len(rows)
	F := len(gr.binners)
	gg, hh := gr.gbuf[:m], gr.hbuf[:m]
	parallelFor(numChunks(m), gr.p.Threads, func(c int) {
		lo, hi := chunkRange(c, m)
		for k := lo; k < hi; k++ {
			r := rows[k]
			gg[k], hh[k] = gr.g[r], gr.h[r]
		}
	})
	sum := func(hs []binStat, col []uint8, lo, hi int) {
		clear(hs)
		for k := lo; k < hi; k++ {
			b := &hs[col[rows[k]]]
			b.g += gg[k]
			b.h += hh[k]
			b.n++
		}
	}
	chunks := (m + histChunk - 1) / histChunk
	if chunks <= 1 {
		threads := gr.p.Threads
		if m*F < 1<<16 {
			threads = 1
		}
		parallelFor(F, threads, func(j int) {
			if gr.use[j] {
				sum(hist[j*maxBins:j*maxBins+gr.nbins[j]], gr.data.bins[j], 0, m)
			}
		})
		return
	}
	if need := chunks * F * maxBins; len(gr.partial) < need {
		gr.partial = make([]binStat, need)
	}
	groups := (F + histGroup - 1) / histGroup
	parallelFor(chunks*groups, gr.p.Threads, func(task int) {
		c, g := task/groups, task%groups
		lo, hi := c*histChunk, min((c+1)*histChunk, m)
		for j := g * histGroup; j < min((g+1)*histGroup, F); j++ {
			if gr.use[j] {
				off := (c*F + j) * maxBins
				sum(gr.partial[off:off+gr.nbins[j]], gr.data.bins[j], lo, hi)
			}
		}
	})
	parallelFor(F, gr.p.Threads, func(j int) {
		if !gr.use[j] {
			return
		}
		hs := hist[j*maxBins : j*maxBins+gr.nbins[j]]
		clear(hs)
		for c := range chunks {
			ps := gr.partial[(c*F+j)*maxBins:]
			for b := range hs {
				hs[b].g += ps[b].g
				hs[b].h += ps[b].h
				hs[b].n += ps[b].n
			}
		}
	})
}

// findBest finds the leaf's best split, or none.
func (gr *grower) findBest(l *growLeaf) {
	l.best = nil
	p := gr.p
	count := int64(l.end - l.start)
	if l.depth >= p.MaxDepth || count < 2*int64(p.MinDataInLeaf) {
		return
	}
	cands := make([]*split, len(gr.binners))
	threads := p.Threads
	if count < 4096 {
		threads = 1
	}
	parallelFor(len(gr.binners), threads, func(j int) {
		if !gr.use[j] {
			return
		}
		hs := l.hist[j*maxBins : j*maxBins+gr.nbins[j]]
		if gr.cat[j] {
			cands[j] = gr.bestCategorical(j, hs, l, count)
		} else {
			cands[j] = gr.bestNumeric(j, hs, l, count)
		}
	})
	for _, c := range cands {
		if c != nil && c.gain > 1e-12 && (l.best == nil || c.gain > l.best.gain) {
			l.best = c
		}
	}
}

func (gr *grower) score(g, h float64) float64 { return g * g / (h + gr.p.Lambda) }

func (gr *grower) ok(n int64, h float64) bool {
	return n >= int64(gr.p.MinDataInLeaf) && h >= gr.p.MinHessian
}

// bestNumeric finds the best threshold of numeric feature j.
func (gr *grower) bestNumeric(j int, hs []binStat, l *growLeaf, count int64) *split {
	parent := gr.score(l.g, l.h)
	miss := hs[missingBin]
	var best *split
	try := func(t int, missingLeft bool, lg, lh float64, ln int64) {
		rg, rh, rn := l.g-lg, l.h-lh, count-ln
		if !gr.ok(ln, lh) || !gr.ok(rn, rh) {
			return
		}
		gain := gr.score(lg, lh) + gr.score(rg, rh) - parent
		if best == nil || gain > best.gain {
			best = &split{feature: j, gain: gain, threshold: uint8(t), missingLeft: missingLeft, lg: lg, lh: lh, ln: ln}
		}
	}
	var cg, ch float64
	var cn int64
	last := len(hs) - 1
	for t := 1; t <= last; t++ {
		cg, ch, cn = cg+hs[t].g, ch+hs[t].h, cn+hs[t].n
		if t == last {
			// Everything but the missing values goes left.
			if miss.n > 0 {
				try(t, false, cg, ch, cn)
			}
			break
		}
		if hs[t].n == 0 {
			continue
		}
		try(t, false, cg, ch, cn)
		if miss.n > 0 {
			try(t, true, cg+miss.g, ch+miss.h, cn+miss.n)
		}
	}
	return best
}

// catSmooth and catL2 regularize categorical splits, as LightGBM's
// cat_smooth and cat_l2 do.
const (
	catSmooth = 10
	catL2     = 10
)

// bestCategorical finds the best set of feature j's bins to send left:
// the bins sorted by their mean gradient, split where that gains most.
func (gr *grower) bestCategorical(j int, hs []binStat, l *growLeaf, count int64) *split {
	var bins []int
	for k := range hs {
		if hs[k].n > 0 {
			bins = append(bins, k)
		}
	}
	if len(bins) < 2 {
		return nil
	}
	sort.SliceStable(bins, func(a, b int) bool {
		x, y := hs[bins[a]], hs[bins[b]]
		return x.g/(x.h+catSmooth) < y.g/(y.h+catSmooth)
	})
	lambda := gr.p.Lambda + catL2
	sc := func(g, h float64) float64 { return g * g / (h + lambda) }
	parent := sc(l.g, l.h)
	var best *split
	bestAt := 0
	var cg, ch float64
	var cn int64
	for i := 0; i+1 < len(bins); i++ {
		s := hs[bins[i]]
		cg, ch, cn = cg+s.g, ch+s.h, cn+s.n
		rg, rh, rn := l.g-cg, l.h-ch, count-cn
		if !gr.ok(cn, ch) || !gr.ok(rn, rh) {
			continue
		}
		gain := sc(cg, ch) + sc(rg, rh) - parent
		if best == nil || gain > best.gain {
			best = &split{feature: j, gain: gain, lg: cg, lh: ch, ln: cn}
			bestAt = i
		}
	}
	if best == nil {
		return nil
	}
	cats := make([]uint8, 0, bestAt+1)
	for _, b := range bins[:bestAt+1] {
		cats = append(cats, uint8(b))
	}
	slices.Sort(cats)
	best.cats = cats
	return best
}

// --- small numeric helpers ------------------------------------------------

func sigmoid(x float64) float64 {
	if x >= 0 {
		return 1 / (1 + math.Exp(-x))
	}
	e := math.Exp(x)
	return e / (1 + e)
}

// softmax turns raw scores into probabilities in place.
func softmax(q []float64) {
	hi := math.Inf(-1)
	for _, v := range q {
		hi = math.Max(hi, v)
	}
	var sum float64
	for k, v := range q {
		q[k] = math.Exp(v - hi)
		sum += q[k]
	}
	for k := range q {
		q[k] /= sum
	}
}

func clamp(x, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, x)) }

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// quantile returns the q quantile of v, interpolating linearly; it sorts
// v.
func quantile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	sort.Float64s(v)
	pos := q * float64(len(v)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(v)-1)
	return v[lo] + (v[hi]-v[lo])*(pos-float64(lo))
}

func pinball(y, f, alpha float64) float64 {
	if y >= f {
		return alpha * (y - f)
	}
	return (1 - alpha) * (f - y)
}

func logLoss1(y, q float64) float64 {
	q = clamp(q, 1e-15, 1-1e-15)
	return -(y*math.Log(q) + (1-y)*math.Log(1-q))
}
