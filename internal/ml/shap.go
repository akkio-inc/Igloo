package ml

// SHAP values by Lundberg et al.'s TreeSHAP ("Consistent Individualized
// Feature Attribution for Tree Ensembles", 2018, algorithm 2), as XGBoost
// and LightGBM compute them: each feature's contribution to one row's raw
// score, with expectations over the training rows that reached each node.
// A row's contributions and the model's bias add up to its raw score.

type pathElem struct {
	feature   int
	zero, one float64
	pweight   float64
}

func extendPath(path []pathElem, depth int, zero, one float64, feature int) {
	w := 0.0
	if depth == 0 {
		w = 1
	}
	path[depth] = pathElem{feature: feature, zero: zero, one: one, pweight: w}
	for i := depth - 1; i >= 0; i-- {
		path[i+1].pweight += one * path[i].pweight * float64(i+1) / float64(depth+1)
		path[i].pweight = zero * path[i].pweight * float64(depth-i) / float64(depth+1)
	}
}

func unwindPath(path []pathElem, depth, index int) {
	one, zero := path[index].one, path[index].zero
	next := path[depth].pweight
	for i := depth - 1; i >= 0; i-- {
		if one != 0 {
			tmp := path[i].pweight
			path[i].pweight = next * float64(depth+1) / (float64(i+1) * one)
			next = tmp - path[i].pweight*zero*float64(depth-i)/float64(depth+1)
		} else {
			path[i].pweight = path[i].pweight * float64(depth+1) / (zero * float64(depth-i))
		}
	}
	for i := index; i < depth; i++ {
		path[i].feature, path[i].zero, path[i].one = path[i+1].feature, path[i+1].zero, path[i+1].one
	}
}

func unwoundPathSum(path []pathElem, depth, index int) float64 {
	one, zero := path[index].one, path[index].zero
	next := path[depth].pweight
	total := 0.0
	for i := depth - 1; i >= 0; i-- {
		switch {
		case one != 0:
			tmp := next * float64(depth+1) / (float64(i+1) * one)
			total += tmp
			next = path[i].pweight - tmp*zero*float64(depth-i)/float64(depth+1)
		case zero != 0:
			total += path[i].pweight / zero / (float64(depth-i) / float64(depth+1))
		}
	}
	return total
}

// shap adds the tree's contributions for a row of bins to phi.
func (t *Tree) shap(row []uint8, phi []float64) {
	if len(t.Nodes) == 0 || t.Nodes[0].IsLeaf() {
		return
	}
	// Each level of the recursion keeps its path in the buffer after its
	// parent's: depth d's takes d+1 elements.
	d := t.maxDepth
	if d == 0 {
		d = t.depth()
	}
	buf := make([]pathElem, (d+2)*(d+3)/2)
	t.shapNode(row, phi, 0, 0, nil, buf, 1, 1, -1)
}

func (t *Tree) shapNode(row []uint8, phi []float64, node, depth int, parent, buf []pathElem, pz, po float64, pf int) {
	path := buf[:depth+1]
	rest := buf[depth+1:]
	copy(path, parent[:depth])
	extendPath(path, depth, pz, po, pf)
	n := &t.Nodes[node]
	if n.IsLeaf() {
		for i := 1; i <= depth; i++ {
			w := unwoundPathSum(path, depth, i)
			e := path[i]
			phi[e.feature] += w * (e.one - e.zero) * n.Value
		}
		return
	}
	hot, cold := int(n.RightChild), int(n.LeftChild)
	if n.goesLeft(row[n.Feature]) {
		hot, cold = cold, hot
	}
	hz := t.Nodes[hot].Cover / n.Cover
	cz := t.Nodes[cold].Cover / n.Cover
	iz, io := 1.0, 1.0
	f := int(n.Feature)
	for k := 0; k <= depth; k++ {
		if path[k].feature == f {
			iz, io = path[k].zero, path[k].one
			unwindPath(path, depth, k)
			depth--
			break
		}
	}
	t.shapNode(row, phi, hot, depth+1, path, rest, hz*iz, io, f)
	t.shapNode(row, phi, cold, depth+1, path, rest, cz*iz, 0, f)
}

// nodeMean is the cover-weighted mean of the leaves under node.
func (t *Tree) nodeMean(node int) float64 {
	n := &t.Nodes[node]
	if n.IsLeaf() {
		return n.Value
	}
	l, r := &t.Nodes[n.LeftChild], &t.Nodes[n.RightChild]
	if l.Cover+r.Cover == 0 {
		return 0
	}
	return (l.Cover*t.nodeMean(int(n.LeftChild)) + r.Cover*t.nodeMean(int(n.RightChild))) / (l.Cover + r.Cover)
}

// Bias returns the model's expected raw score per output, the part of a
// row's score no feature accounts for.
func (m *GBM) Bias() []float64 {
	out := append([]float64(nil), m.Init...)
	for i := range m.Trees {
		t := &m.Trees[i]
		if len(t.Nodes) > 0 {
			out[t.Class] += t.nodeMean(0)
		}
	}
	return out
}

// Contributions returns a row's SHAP values: for each output k (one,
// or one per class), the contributions of the model's features at
// out[k*(F+1)+j] and, at out[k*(F+1)+F], the bias, which with them adds
// up to the row's raw score. bias is Bias(), passed in to compute once.
func (m *GBM) Contributions(row []uint8, bias []float64) []float64 {
	F := len(m.Features)
	out := make([]float64, m.outputs()*(F+1))
	for k, b := range bias {
		out[k*(F+1)+F] = b
	}
	for i := range m.Trees {
		t := &m.Trees[i]
		t.shap(row, out[t.Class*(F+1):t.Class*(F+1)+F])
	}
	return out
}

// RowBins returns a frame's row i as bins of the model's features.
func (m *GBM) RowBins(b *binned, i int) []uint8 {
	row := make([]uint8, len(m.Features))
	for j := range row {
		row[j] = b.bins[j][i]
	}
	return row
}

// MeanAbsContributions returns each feature's mean absolute SHAP value
// over a frame's rows (summed over classes), for global importance; at
// most limit rows are used, chosen evenly.
func (m *GBM) MeanAbsContributions(f *Frame, limit int) ([]float64, error) {
	b, err := m.binFrame(f)
	if err != nil {
		return nil, err
	}
	F := len(m.Features)
	out := make([]float64, F)
	if f.N == 0 {
		return out, nil
	}
	step := 1
	if limit > 0 && f.N > limit {
		step = (f.N + limit - 1) / limit
	}
	var rows []int
	for i := 0; i < f.N; i += step {
		rows = append(rows, i)
	}
	bias := m.Bias()
	per := make([][]float64, numChunks(len(rows)))
	parallelFor(len(per), 0, func(c int) {
		lo, hi := chunkRange(c, len(rows))
		sum := make([]float64, F)
		for _, i := range rows[lo:hi] {
			phi := m.Contributions(m.RowBins(b, i), bias)
			for k := range m.outputs() {
				for j := range F {
					v := phi[k*(F+1)+j]
					if v < 0 {
						v = -v
					}
					sum[j] += v
				}
			}
		}
		per[c] = sum
	})
	for _, sum := range per {
		for j := range F {
			out[j] += sum[j]
		}
	}
	for j := range out {
		out[j] /= float64(len(rows))
	}
	return out, nil
}
