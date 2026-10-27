package models

import (
	"math"

	"github.com/akkio-inc/igloo/internal/ml"
)

// DNN_CLASSIFIER and DNN_REGRESSOR: a feed-forward network on the features
// standardized and one-hot encoded, trained by Adam with early stopping on
// a tenth of the training rows.

// netOptions are a network's options, BigQuery's names: hidden_units,
// activation_fn, dropout, batch_size, learn_rate, max_iterations (epochs),
// early_stop, l2_reg and optimizer.
type netOptions struct {
	hidden          []int
	activation      string
	dropout, lr, l2 float64
	batch, epochs   int
	stop            bool
}

func readNetOptions(cfg *config, defHidden []int) (netOptions, error) {
	var o netOptions
	var err error
	if o.hidden, err = cfg.ints("hidden_units", defHidden, 1, 4096); err != nil {
		return o, err
	}
	if o.activation, err = cfg.str("activation_fn", "relu", "relu", "tanh", "sigmoid", "linear"); err != nil {
		return o, err
	}
	if o.activation == "linear" {
		o.activation = "identity"
	}
	if o.dropout, err = cfg.num("dropout", 0, 0, 0.9); err != nil {
		return o, err
	}
	if o.batch, err = cfg.integer("batch_size", 0, 1, 1<<20); err != nil {
		return o, err
	}
	if o.lr, err = cfg.num("learn_rate", 0.001, 1e-7, 1); err != nil {
		return o, err
	}
	if o.epochs, err = cfg.integer("max_iterations", 0, 1, 10_000); err != nil {
		return o, err
	}
	if o.stop, err = cfg.boolean("early_stop", true); err != nil {
		return o, err
	}
	if _, err := cfg.num("min_rel_progress", 0, 0, 1); err != nil {
		return o, err
	}
	if o.l2, err = cfg.num("l2_reg", 0, 0, 1e6); err != nil {
		return o, err
	}
	if l1, err := cfg.num("l1_reg", 0, 0, 1e6); err != nil || l1 != 0 {
		return o, inputErr("l1_reg is not supported: Igloo's networks take l2_reg")
	}
	if err := cfg.accept("optimizer", "adam"); err != nil {
		return o, err
	}
	if _, err := cfg.boolean("enable_global_explain", true); err != nil {
		return o, err
	}
	return o, nil
}

// params fills in a network's defaults for n training rows: batches of
// about a thirty-second of the rows (32 to 1,024), and enough epochs for
// some 3,000 steps (5 to 500).
func (o netOptions) params(n int, seed uint64, interrupt func() error) ml.MLPParams {
	batch := o.batch
	if batch == 0 {
		batch = min(1024, max(32, n/32))
	}
	epochs := o.epochs
	if epochs == 0 {
		steps := max(1, (n+batch-1)/batch)
		epochs = min(500, max(5, (3000+steps-1)/steps))
	}
	p := ml.MLPParams{Hidden: o.hidden, LearningRate: o.lr, BatchSize: batch, Epochs: epochs, Dropout: o.dropout, L2: o.l2,
		Seed: seed, Interrupt: interrupt}
	for range o.hidden {
		p.HiddenActivations = append(p.HiddenActivations, o.activation)
	}
	if !o.stop {
		p.EarlyStopping = -1
	}
	return p
}

// validationSplit holds back a tenth of n rows for early stopping, when
// there are enough.
func validationSplit(n int, y []float64, classify bool, seed uint64) (keep, held []int) {
	if n < 50 {
		return identity(n), nil
	}
	if classify {
		return stratified(y, 0.1, seed, 0x7a1)
	}
	for i := range n {
		if rank(seed, 0x7a1, i)%10 == 0 {
			held = append(held, i)
		} else {
			keep = append(keep, i)
		}
	}
	return keep, held
}

// fitNet trains a DNN_CLASSIFIER's or DNN_REGRESSOR's network.
func (s *Supervised) fitNet(f *ml.Frame, y []float64, cfg *config, seed uint64, interrupt func() error) error {
	o, err := readNetOptions(cfg, []int{64, 32})
	if err != nil {
		return err
	}
	s.Encoder = ml.FitEncoder(f, true, 5, 100)
	if s.Encoder.Width == 0 {
		return inputErr("the features hold nothing a network can read")
	}
	X := s.Encoder.Encode(f)
	w := s.Encoder.Width
	target := y
	if !s.classify() {
		s.YMean = mean(y)
		s.YScale = math.Sqrt(variance(y))
		if s.YScale == 0 {
			s.YScale = 1
		}
		target = make([]float64, len(y))
		for i, v := range y {
			target[i] = (v - s.YMean) / s.YScale
		}
	}
	p := o.params(f.N, seed, interrupt)
	switch {
	case !s.classify():
		p.Loss, p.Outputs = ml.LossMSE, 1
	case len(s.Classes) == 2:
		p.Loss = ml.LossBinary
	default:
		p.Loss, p.Outputs = ml.LossSoftmax, len(s.Classes)
	}
	var keep, held []int
	if o.stop {
		keep, held = validationSplit(f.N, y, s.classify(), seed)
	} else {
		keep = identity(f.N)
	}
	tx, ty := takeRows(X, w, target, keep)
	vx, vy := takeRows(X, w, target, held)
	net, info, err := ml.TrainMLP(tx, len(keep), w, ty, vx, len(held), vy, p)
	if err != nil {
		return inputErr("%v", err)
	}
	s.Net = net
	s.Hyper = map[string]any{"hidden_units": o.hidden, "activation_fn": o.activation, "dropout": o.dropout,
		"batch_size": p.BatchSize, "learn_rate": o.lr, "max_iterations": info.BestEpoch, "l2_reg": o.l2}
	s.Training = netTraining(info, o.lr)
	return nil
}

// netTraining is TRAINING_INFO's lines of a network's epochs, up to the
// one kept.
func netTraining(info ml.MLPInfo, lr float64) []Iteration {
	var out []Iteration
	for e, l := range info.Loss {
		if info.BestEpoch > 0 && e >= info.BestEpoch {
			break
		}
		it := Iteration{Iteration: e + 1, Loss: finiteOr(l, 0), LearningRate: lr}
		if e < len(info.ValidLoss) {
			it.EvalLoss = finiteOr(info.ValidLoss[e], 0)
		}
		out = append(out, it)
	}
	return out
}

// takeRows copies rows of x (w values each) and their targets.
func takeRows(x []float64, w int, y []float64, rows []int) ([]float64, []float64) {
	if len(rows) == 0 {
		return nil, nil
	}
	ox, oy := make([]float64, 0, len(rows)*w), make([]float64, 0, len(rows))
	for _, r := range rows {
		ox = append(ox, x[r*w:(r+1)*w]...)
		oy = append(oy, y[r])
	}
	return ox, oy
}
