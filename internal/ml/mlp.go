package ml

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
)

// MLP is a feed-forward neural network of dense layers, each followed by
// its activation: relu, tanh, identity, sigmoid or softmax.
type MLP struct {
	// Sizes are the layers' widths, the input's first.
	Sizes []int `json:"sizes"`
	// Activations are each layer's, after its dense step.
	Activations []string `json:"activations"`
	// W[l] is layer l's weights, Sizes[l+1] rows of Sizes[l] (row-major);
	// B[l] its biases.
	W [][]float64 `json:"w"`
	B [][]float64 `json:"b"`
}

// Losses an MLP trains for.
const (
	// LossMSE is the mean squared error of every output (identity outputs).
	LossMSE = "mse"
	// LossBinary is the log loss of one sigmoid output, labels 0 and 1.
	LossBinary = "binary"
	// LossSoftmax is the log loss of softmax outputs, labels the class's
	// number.
	LossSoftmax = "softmax"
)

// MLPParams configure TrainMLP. Zero values mean the defaults noted.
type MLPParams struct {
	// Hidden are the hidden layers' widths; HiddenActivations their
	// activations (default relu).
	Hidden            []int
	HiddenActivations []string
	// Loss is LossMSE, LossBinary or LossSoftmax; Outputs is the output
	// layer's width (LossMSE's targets per row, or the classes).
	Loss    string
	Outputs int
	// LearningRate is Adam's step (default 0.001).
	LearningRate float64
	// BatchSize is the rows of each step (default 32), Epochs the passes
	// over the rows at most (default 20).
	BatchSize, Epochs int
	// Dropout is the share of hidden units dropped in training.
	Dropout float64
	// L2 penalizes weights.
	L2 float64
	// EarlyStopping stops after that many epochs without a better
	// validation loss, keeping the best epoch's weights (default 5, given
	// validation rows; negative never stops early).
	EarlyStopping int
	// Seed seeds the weights, the order of rows and dropout: the same rows
	// and seed give the same network, whatever the threads.
	Seed      uint64
	Threads   int
	Interrupt func() error
}

// MLPInfo is what training learned besides the network.
type MLPInfo struct {
	// Loss and ValidLoss are each epoch's mean training and validation
	// losses; BestEpoch (from 1) is the one kept.
	Loss, ValidLoss []float64
	BestEpoch       int
}

// subRows is how many rows of a batch one task handles: a batch's
// gradient is the sum of its tasks', added in order.
const subRows = 32

// TrainMLP trains a network on x (n rows of d values, row-major) and y:
// n rows of Outputs values for LossMSE, n labels otherwise. vx and vy,
// if given, are validation rows for early stopping.
func TrainMLP(x []float64, n, d int, y []float64, vx []float64, vn int, vy []float64, p MLPParams) (*MLP, MLPInfo, error) {
	var info MLPInfo
	if n == 0 || d == 0 {
		return nil, info, errors.New("a network needs rows and features")
	}
	if len(x) != n*d {
		return nil, info, fmt.Errorf("%d values for %d rows of %d", len(x), n, d)
	}
	switch p.Loss {
	case LossMSE:
		if len(y) != n*p.Outputs {
			return nil, info, errors.New("targets do not match the rows")
		}
	case LossBinary:
		p.Outputs = 1
	case LossSoftmax:
		if p.Outputs < 2 {
			return nil, info, errors.New("softmax needs two classes or more")
		}
	default:
		return nil, info, fmt.Errorf("unknown loss %q", p.Loss)
	}
	if p.Loss != LossMSE && len(y) != n {
		return nil, info, errors.New("labels do not match the rows")
	}
	if p.LearningRate <= 0 {
		p.LearningRate = 0.001
	}
	if p.BatchSize <= 0 {
		p.BatchSize = 32
	}
	if p.Epochs <= 0 {
		p.Epochs = 20
	}
	if p.EarlyStopping == 0 {
		p.EarlyStopping = 5
	}
	if p.Dropout < 0 || p.Dropout >= 1 {
		return nil, info, errors.New("dropout must be at least 0 and less than 1")
	}
	m := &MLP{Sizes: append(append([]int{d}, p.Hidden...), p.Outputs)}
	for l := range p.Hidden {
		a := "relu"
		if l < len(p.HiddenActivations) && p.HiddenActivations[l] != "" {
			a = p.HiddenActivations[l]
		}
		m.Activations = append(m.Activations, a)
	}
	switch p.Loss {
	case LossMSE:
		m.Activations = append(m.Activations, "identity")
	case LossBinary:
		m.Activations = append(m.Activations, "sigmoid")
	default:
		m.Activations = append(m.Activations, "softmax")
	}
	t := newMLPTrainer(m, p)
	t.init()
	best := math.Inf(1)
	var bestTheta []float64
	sinceBest := 0
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	for epoch := range p.Epochs {
		if p.Interrupt != nil {
			if err := p.Interrupt(); err != nil {
				return nil, info, err
			}
		}
		rng := rand.New(rand.NewPCG(p.Seed, 0x5eed+uint64(epoch)))
		rng.Shuffle(n, func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
		var total float64
		for start := 0; start < n; start += p.BatchSize {
			rows := perm[start:min(start+p.BatchSize, n)]
			total += t.step(x, y, rows, uint64(epoch))
		}
		info.Loss = append(info.Loss, total/float64(n))
		if vn > 0 {
			t.unflatten()
			loss := m.Loss(vx, vn, vy, p.Loss, p.Threads)
			info.ValidLoss = append(info.ValidLoss, loss)
			if loss < best-1e-12 {
				best, info.BestEpoch, sinceBest = loss, epoch+1, 0
				bestTheta = append(bestTheta[:0], t.theta...)
			} else if sinceBest++; p.EarlyStopping > 0 && sinceBest >= p.EarlyStopping {
				break
			}
		}
	}
	if bestTheta != nil {
		copy(t.theta, bestTheta)
	} else {
		info.BestEpoch = len(info.Loss)
	}
	t.unflatten()
	return m, info, nil
}

// mlpTrainer holds training's state: the parameters, flat, and Adam's
// moments.
type mlpTrainer struct {
	m        *MLP
	p        MLPParams
	theta    []float64
	mom, vel []float64
	// wOff[l] and bOff[l] are layer l's weights' and biases' offsets in
	// theta.
	wOff, bOff []int
	steps      int
	// grads holds each task's gradient; scratch its activations.
	grads   [][]float64
	scratch []*mlpScratch
}

type mlpScratch struct {
	acts, deltas [][]float64
	masks        [][]float64
	// nz are the input's nonzero entries: one-hot inputs are mostly zeros,
	// which the first layer skips.
	nz []int
}

func newMLPTrainer(m *MLP, p MLPParams) *mlpTrainer {
	t := &mlpTrainer{m: m, p: p}
	size := 0
	for l := 0; l+1 < len(m.Sizes); l++ {
		t.wOff = append(t.wOff, size)
		size += m.Sizes[l] * m.Sizes[l+1]
		t.bOff = append(t.bOff, size)
		size += m.Sizes[l+1]
	}
	t.theta, t.mom, t.vel = make([]float64, size), make([]float64, size), make([]float64, size)
	tasks := (p.BatchSize + subRows - 1) / subRows
	for range tasks {
		t.grads = append(t.grads, make([]float64, size))
		t.scratch = append(t.scratch, m.newScratch())
	}
	return t
}

func (m *MLP) newScratch() *mlpScratch {
	s := &mlpScratch{}
	for _, w := range m.Sizes {
		s.acts = append(s.acts, make([]float64, w))
		s.deltas = append(s.deltas, make([]float64, w))
		s.masks = append(s.masks, make([]float64, w))
	}
	return s
}

// init draws the weights: uniform within He's bound for relu layers and
// Glorot's otherwise; biases start at 0.
func (t *mlpTrainer) init() {
	rng := rand.New(rand.NewPCG(t.p.Seed, 0x1417))
	m := t.m
	for l := 0; l+1 < len(m.Sizes); l++ {
		in, out := m.Sizes[l], m.Sizes[l+1]
		bound := math.Sqrt(6 / float64(in+out))
		if m.Activations[l] == "relu" && l+1 < len(m.Sizes)-1 {
			bound = math.Sqrt(6 / float64(in))
		}
		w := t.theta[t.wOff[l] : t.wOff[l]+in*out]
		for i := range w {
			w[i] = (2*rng.Float64() - 1) * bound
		}
	}
}

// unflatten copies the parameters into the network.
func (t *mlpTrainer) unflatten() {
	m := t.m
	if m.W == nil {
		m.W, m.B = make([][]float64, len(m.Sizes)-1), make([][]float64, len(m.Sizes)-1)
	}
	for l := 0; l+1 < len(m.Sizes); l++ {
		in, out := m.Sizes[l], m.Sizes[l+1]
		m.W[l] = append(m.W[l][:0], t.theta[t.wOff[l]:t.wOff[l]+in*out]...)
		m.B[l] = append(m.B[l][:0], t.theta[t.bOff[l]:t.bOff[l]+out]...)
	}
}

// step takes one Adam step on a batch's rows and returns their summed
// loss.
func (t *mlpTrainer) step(x, y []float64, rows []int, epoch uint64) float64 {
	tasks := (len(rows) + subRows - 1) / subRows
	losses := make([]float64, tasks)
	parallelFor(tasks, t.p.Threads, func(k int) {
		g := t.grads[k]
		clear(g)
		s := t.scratch[k]
		var loss float64
		for _, r := range rows[k*subRows : min((k+1)*subRows, len(rows))] {
			loss += t.backprop(x, y, r, s, g, epoch)
		}
		losses[k] = loss
	})
	var loss float64
	g := t.grads[0]
	for k := 1; k < tasks; k++ {
		for i, v := range t.grads[k] {
			g[i] += v
		}
	}
	for _, l := range losses {
		loss += l
	}
	t.steps++
	const beta1, beta2, eps = 0.9, 0.999, 1e-8
	lr := t.p.LearningRate * math.Sqrt(1-math.Pow(beta2, float64(t.steps))) / (1 - math.Pow(beta1, float64(t.steps)))
	scale := 1 / float64(len(rows))
	for i := range t.theta {
		gi := g[i] * scale
		if t.p.L2 > 0 && t.isWeight(i) {
			gi += t.p.L2 * t.theta[i]
		}
		t.mom[i] = beta1*t.mom[i] + (1-beta1)*gi
		t.vel[i] = beta2*t.vel[i] + (1-beta2)*gi*gi
		t.theta[i] -= lr * t.mom[i] / (math.Sqrt(t.vel[i]) + eps)
	}
	return loss
}

// isWeight reports whether parameter i is a weight rather than a bias.
func (t *mlpTrainer) isWeight(i int) bool {
	for l := range t.wOff {
		if i >= t.wOff[l] && i < t.bOff[l] {
			return true
		}
	}
	return false
}

// backprop adds row r's gradient to g and returns its loss.
func (t *mlpTrainer) backprop(x, y []float64, r int, s *mlpScratch, g []float64, epoch uint64) float64 {
	m := t.m
	L := len(m.Sizes) - 1
	d := m.Sizes[0]
	copy(s.acts[0], x[r*d:(r+1)*d])
	s.nz = nonzero(s.acts[0], s.nz[:0])
	for l := range L {
		in, out := m.Sizes[l], m.Sizes[l+1]
		w := t.theta[t.wOff[l] : t.wOff[l]+in*out]
		b := t.theta[t.bOff[l] : t.bOff[l]+out]
		if l == 0 {
			denseSparse(w, b, s.acts[0], s.nz, s.acts[1])
		} else {
			dense(w, b, s.acts[l], s.acts[l+1])
		}
		activate(m.Activations[l], s.acts[l+1])
		if l+1 < L && t.p.Dropout > 0 {
			keep := 1 - t.p.Dropout
			for o := range out {
				if unitHash(t.p.Seed^epoch*0x9e37, uint64(l)+0xd809, r*4099+o) < keep {
					s.masks[l+1][o] = 1 / keep
				} else {
					s.masks[l+1][o] = 0
				}
				s.acts[l+1][o] *= s.masks[l+1][o]
			}
		}
	}
	// The output layer's delta, the loss's gradient by its dense step.
	out := s.acts[L]
	delta := s.deltas[L]
	var loss float64
	switch t.p.Loss {
	case LossMSE:
		O := float64(len(out))
		for o := range out {
			diff := out[o] - y[r*len(out)+o]
			loss += diff * diff / O
			delta[o] = 2 * diff / O
		}
	case LossBinary:
		loss = logLoss1(y[r], out[0])
		delta[0] = out[0] - y[r]
	default:
		k := int(y[r])
		loss = -math.Log(math.Max(out[k], 1e-15))
		for o := range out {
			delta[o] = out[o]
		}
		delta[k]--
	}
	for l := L - 1; l >= 0; l-- {
		in, outW := m.Sizes[l], m.Sizes[l+1]
		w := t.theta[t.wOff[l] : t.wOff[l]+in*outW]
		gw := g[t.wOff[l] : t.wOff[l]+in*outW]
		gb := g[t.bOff[l] : t.bOff[l]+outW]
		a := s.acts[l]
		dl := s.deltas[l+1]
		for o := range outW {
			do := dl[o]
			gb[o] += do
			if do == 0 {
				continue
			}
			row := gw[o*in : (o+1)*in]
			if l == 0 {
				for _, i := range s.nz {
					row[i] += do * a[i]
				}
				continue
			}
			for i, ai := range a {
				row[i] += do * ai
			}
		}
		if l == 0 {
			break
		}
		prev := s.deltas[l]
		clear(prev)
		for o := range outW {
			do := dl[o]
			if do == 0 {
				continue
			}
			row := w[o*in : (o+1)*in]
			for i := range prev {
				prev[i] += row[i] * do
			}
		}
		// Through layer l-1's activation and dropout.
		act := m.Activations[l-1]
		for i := range prev {
			switch act {
			case "relu":
				if a[i] <= 0 {
					prev[i] = 0
				}
			case "sigmoid", "tanh":
				q := a[i]
				if t.p.Dropout > 0 && s.masks[l][i] > 0 {
					q /= s.masks[l][i]
				}
				if act == "tanh" {
					prev[i] *= 1 - q*q
				} else {
					prev[i] *= q * (1 - q)
				}
			}
			if t.p.Dropout > 0 {
				prev[i] *= s.masks[l][i]
			}
		}
	}
	return loss
}

// nonzero appends the indexes of v's nonzero entries to nz.
func nonzero(v []float64, nz []int) []int {
	for i, x := range v {
		if x != 0 {
			nz = append(nz, i)
		}
	}
	return nz
}

// denseSparse is dense for an input whose nonzero entries are at nz: the
// zeros, which add nothing, are skipped, and the rest added in order.
func denseSparse(w, b, in []float64, nz []int, out []float64) {
	n := len(in)
	for o := range out {
		s := b[o]
		row := w[o*n : (o+1)*n]
		for _, i := range nz {
			s += row[i] * in[i]
		}
		out[o] = s
	}
}

// dense computes out = w·in + b.
func dense(w, b, in, out []float64) {
	n := len(in)
	for o := range out {
		s := b[o]
		row := w[o*n : (o+1)*n]
		for i, v := range in {
			s += row[i] * v
		}
		out[o] = s
	}
}

func activate(kind string, v []float64) {
	switch kind {
	case "relu":
		for i, x := range v {
			if x < 0 {
				v[i] = 0
			}
		}
	case "sigmoid":
		for i, x := range v {
			v[i] = sigmoid(x)
		}
	case "tanh":
		for i, x := range v {
			v[i] = math.Tanh(x)
		}
	case "softmax":
		softmax(v)
	}
}

// MLPScratch holds one goroutine's activations for Forward.
type MLPScratch struct {
	acts [][]float64
	nz   []int
}

// NewScratch returns buffers for Forward.
func (m *MLP) NewScratch() *MLPScratch {
	s := &MLPScratch{}
	for _, w := range m.Sizes {
		s.acts = append(s.acts, make([]float64, w))
	}
	return s
}

// Forward runs a row through the network and returns layer upto's
// outputs (len(Sizes)-1 for the network's), which s holds until its next
// use.
func (m *MLP) Forward(x []float64, upto int, s *MLPScratch) []float64 {
	copy(s.acts[0], x)
	for l := range upto {
		if l == 0 {
			s.nz = nonzero(s.acts[0], s.nz[:0])
			denseSparse(m.W[0], m.B[0], s.acts[0], s.nz, s.acts[1])
		} else {
			dense(m.W[l], m.B[l], s.acts[l], s.acts[l+1])
		}
		activate(m.Activations[l], s.acts[l+1])
	}
	return s.acts[upto]
}

// Layers is the number of dense layers.
func (m *MLP) Layers() int { return len(m.Sizes) - 1 }

// Loss is the network's mean loss on rows x and targets y.
func (m *MLP) Loss(x []float64, n int, y []float64, loss string, threads int) float64 {
	d := m.Sizes[0]
	parts := make([]float64, numChunks(n))
	parallelFor(len(parts), threads, func(c int) {
		lo, hi := chunkRange(c, n)
		s := m.NewScratch()
		var sum float64
		for i := lo; i < hi; i++ {
			out := m.Forward(x[i*d:(i+1)*d], m.Layers(), s)
			switch loss {
			case LossMSE:
				for o, v := range out {
					diff := v - y[i*len(out)+o]
					sum += diff * diff / float64(len(out))
				}
			case LossBinary:
				sum += logLoss1(y[i], out[0])
			default:
				sum -= math.Log(math.Max(out[int(y[i])], 1e-15))
			}
		}
		parts[c] = sum
	})
	var total float64
	for _, v := range parts {
		total += v
	}
	return total / float64(max(n, 1))
}
