package tsfm

import (
	"fmt"
	"math"
	"slices"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// TimesFM 2.5 (google-research/timesfm, timesfm_2p5/ and torch/): a
// decoder-only transformer over patches of 32 values and their mask, each
// normalized by the running mean and deviation of the series up to it.
// Each patch's state maps to the next 128 steps' mean and deciles, and
// the last patch's also to 1,024 steps of deciles (the continuous quantile
// head). Horizons past 128 steps feed the median back as four new patches,
// with the keys and values of the patches before cached. Igloo forecasts
// with the flags the TimesFM README recommends: inputs normalized, flip
// invariance (the forecast of -x, negated, averaged in), the continuous
// quantile head up to 1,024 steps, quantile crossing fixed, and
// non-negative series forecast non-negative.

type timesfmConfig struct {
	ContextLimit    int       `json:"context_length"`
	HeadDim         int       `json:"head_dim"`
	Hidden          int       `json:"hidden_size"`
	OutputPatch     int       `json:"horizon_length"`
	Intermediate    int       `json:"intermediate_size"`
	NumHeads        int       `json:"num_attention_heads"`
	NumLayers       int       `json:"num_hidden_layers"`
	Patch           int       `json:"patch_length"`
	QuantileHorizon int       `json:"quantile_horizon_length"`
	Quantiles       []float64 `json:"quantiles"`
	Eps             float32   `json:"rms_norm_eps"`
}

type timesfmLayer struct {
	preAttn, postAttn, preFF, postFF []float32
	qkv, out, ff0, ff1               linear
	queryLN, keyLN                   []float32
	scale                            []float32 // per-dimension query scale
}

type timesfm struct {
	name      string
	cfg       timesfmConfig
	tensors   *modelpack.Tensors
	tokenizer residualBlock
	layers    []timesfmLayer
	point     residualBlock
	quantile  residualBlock
	timescale []float32
}

// timesfmFlags are TimesFM's ForecastConfig flags.
type timesfmFlags struct {
	normalize, flip, continuousHead, positive, fixCrossing bool
}

var recommended = timesfmFlags{normalize: true, flip: true, continuousHead: true, positive: true, fixCrossing: true}

func loadTimesFM(m *modelpack.Model) (Model, error) {
	var cfg timesfmConfig
	if err := readConfig(m, &cfg); err != nil {
		return nil, err
	}
	if cfg.Hidden <= 0 || cfg.NumHeads <= 0 || cfg.Patch <= 0 || cfg.OutputPatch%cfg.Patch != 0 || len(cfg.Quantiles) != 9 ||
		cfg.QuantileHorizon < cfg.OutputPatch {
		return nil, fmt.Errorf("model %s: config.json is not a TimesFM 2.5 configuration Igloo runs", m.Name)
	}
	if cfg.Eps == 0 {
		cfg.Eps = 1e-6
	}
	if cfg.ContextLimit == 0 {
		cfg.ContextLimit = 16384
	}
	if cfg.HeadDim == 0 {
		cfg.HeadDim = cfg.Hidden / cfg.NumHeads
	}
	t, err := modelpack.OpenTensors(m.Path("model.safetensors"))
	if err != nil {
		return nil, err
	}
	w := &weights{t: t}
	md, hd, q := cfg.Hidden, cfg.HeadDim, len(cfg.Quantiles)+1
	tokHidden := md
	if s := w.shape("tokenizer.hidden_layer.weight"); len(s) == 2 {
		tokHidden = s[0]
	}
	x := &timesfm{name: m.Name, cfg: cfg, tensors: t}
	x.tokenizer = w.residualBlock("tokenizer", 2*cfg.Patch, tokHidden, md, true, silu)
	for i := range cfg.NumLayers {
		lw := w.sub(fmt.Sprintf("stacked_xf.%d.", i))
		l := timesfmLayer{
			preAttn: lw.vec("pre_attn_ln.scale", md), postAttn: lw.vec("post_attn_ln.scale", md),
			preFF: lw.vec("pre_ff_ln.scale", md), postFF: lw.vec("post_ff_ln.scale", md),
			qkv: lw.linear("attn.qkv_proj", md, 3*md, false), out: lw.linear("attn.out", md, md, false),
			ff0: lw.linear("ff0", md, cfg.Intermediate, false), ff1: lw.linear("ff1", cfg.Intermediate, md, false),
			queryLN: lw.vec("attn.query_ln.scale", hd), keyLN: lw.vec("attn.key_ln.scale", hd),
		}
		pds := lw.vec("attn.per_dim_scale.per_dim_scale", hd)
		l.scale = make([]float32, hd)
		r := float32(1.442695041 / math.Sqrt(float64(hd)))
		for j, v := range pds {
			l.scale[j] = r * float32(math.Log1p(math.Exp(float64(v))))
		}
		x.layers = append(x.layers, l)
	}
	x.point = w.residualBlock("output_projection_point", md, md, cfg.OutputPatch*q, false, silu)
	x.quantile = w.residualBlock("output_projection_quantiles", md, md, cfg.QuantileHorizon*q, false, silu)
	if w.err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("model %s: %w", m.Name, w.err)
	}
	half := hd / 2
	x.timescale = make([]float32, half)
	for i := range half {
		frac := float32(2*i) / float32(hd)
		x.timescale[i] = float32(math.Pow(10000, float64(frac)))
	}
	return x, nil
}

func (x *timesfm) Name() string { return x.name }
func (x *timesfm) Levels() []float64 {
	return slices.Clone(x.cfg.Quantiles)
}
func (x *timesfm) MaxContext() int { return x.cfg.ContextLimit - x.cfg.OutputPatch }
func (x *timesfm) MaxHorizon() int { return 10000 }

// defaultContext is BigQuery's choice of context window for a series of
// n points: the smallest power of two from 64 that holds it, up to
// 8,192, or 15,360.
func defaultContext(n int) int {
	c := 64
	for c < n && c < 8192 {
		c *= 2
	}
	if n > 8192 {
		c = 15360
	}
	return c
}

// Forecast forecasts as TimesFM_2p5.forecast does, compiled with the
// recommended flags, a context of opts.Context (or BigQuery's default) and
// the horizon rounded up to whole output patches.
func (x *timesfm) Forecast(series []float32, horizon int, opts Options) (Forecast, error) {
	if horizon <= 0 || horizon > x.MaxHorizon() {
		return Forecast{}, fmt.Errorf("%w: %s forecasts 1 to %d steps", ErrInput, x.name, x.MaxHorizon())
	}
	mc := opts.Context
	if mc <= 0 {
		mc = defaultContext(len(series))
	}
	mc = (mc + x.cfg.Patch - 1) / x.cfg.Patch * x.cfg.Patch
	mh := (horizon + x.cfg.OutputPatch - 1) / x.cfg.OutputPatch * x.cfg.OutputPatch
	if mc+mh > x.cfg.ContextLimit {
		mc = (x.cfg.ContextLimit - mh) / x.cfg.Patch * x.cfg.Patch
		if mc < x.cfg.Patch {
			return Forecast{}, fmt.Errorf("%w: %s forecasts at most %d steps", ErrInput, x.name, x.cfg.ContextLimit-x.cfg.Patch)
		}
	}
	flags := recommended
	if mh > x.cfg.QuantileHorizon {
		flags.continuousHead = false
	}
	return x.forecast(series, horizon, mc, mh, flags), nil
}

// prepareSeries is TimesFM's: leading NaNs stripped, the rest linearly
// interpolated, the last mc points kept, left-padded with masked zeros.
func prepareSeries(series []float32, mc int) ([]float32, []bool) {
	start := 0
	for start < len(series) && isNaN32(series[start]) {
		start++
	}
	if start == len(series) {
		start = 0
	}
	v := make([]float64, len(series)-start)
	var known []int
	for i, s := range series[start:] {
		v[i] = float64(s)
		if !isNaN32(s) {
			known = append(known, i)
		}
	}
	if len(known) == 0 {
		clear(v)
	} else if len(known) < len(v) {
		k := 0
		for i := range v {
			if !math.IsNaN(v[i]) {
				continue
			}
			for k+1 < len(known) && known[k+1] < i {
				k++
			}
			switch {
			case i < known[0]:
				v[i] = v[known[0]]
			case i > known[len(known)-1]:
				v[i] = v[known[len(known)-1]]
			default:
				a, b := known[k], known[k+1]
				v[i] = v[a] + (v[b]-v[a])*float64(i-a)/float64(b-a)
			}
		}
	}
	values := make([]float32, mc)
	mask := make([]bool, mc)
	if len(v) >= mc {
		for i := range values {
			values[i] = float32(v[len(v)-mc+i])
		}
		return values, mask
	}
	pad := mc - len(v)
	for i := range pad {
		mask[i] = true
	}
	for i, f := range v {
		values[pad+i] = float32(f)
	}
	return values, mask
}

func (x *timesfm) forecast(series []float32, horizon, mc, mh int, flags timesfmFlags) Forecast {
	inputs, mask := prepareSeries(series, mc)
	positive := true
	for _, v := range inputs {
		if v < 0 {
			positive = false
		}
	}
	var mu, sigma float32
	if flags.normalize {
		var sum float32
		for _, v := range inputs {
			sum += v
		}
		mu = sum / float32(len(inputs))
		var ss float32
		for _, v := range inputs {
			ss += (v - mu) * (v - mu)
		}
		sigma = float32(math.Sqrt(float64(ss / float32(len(inputs)-1))))
		div := sigma
		if div < 1e-6 {
			div = 1
		}
		for i, v := range inputs {
			inputs[i] = (v - mu) / div
		}
	}
	nq := len(x.cfg.Quantiles) + 1
	full, spread := x.decode(mh, inputs, mask)
	if flags.flip {
		neg := make([]float32, len(inputs))
		for i, v := range inputs {
			neg[i] = -v
		}
		ffull, fspread := x.decode(mh, neg, mask)
		flip := func(a, b []float32) {
			for t := 0; t < len(a)/nq; t++ {
				ra, rb := a[t*nq:(t+1)*nq], b[t*nq:(t+1)*nq]
				ra[0] = (ra[0] - rb[0]) / 2
				for c := 1; c < nq; c++ {
					ra[c] = (ra[c] - rb[nq-c]) / 2
				}
			}
		}
		flip(full, ffull)
		flip(spread, fspread)
	}
	if flags.continuousHead {
		med := 5
		for t := 0; t < mh; t++ {
			row, s := full[t*nq:(t+1)*nq], spread[t*nq:(t+1)*nq]
			for _, c := range []int{1, 2, 3, 4, 6, 7, 8, 9} {
				row[c] = s[c] - s[med] + row[med]
			}
		}
	}
	full = full[:horizon*nq]
	if flags.fixCrossing {
		for t := 0; t < horizon; t++ {
			row := full[t*nq : (t+1)*nq]
			for _, c := range []int{4, 3, 2, 1} {
				if !(row[c] < row[c+1]) {
					row[c] = row[c+1]
				}
			}
			for _, c := range []int{6, 7, 8, 9} {
				if !(row[c] > row[c-1]) {
					row[c] = row[c-1]
				}
			}
		}
	}
	if flags.normalize {
		for i, v := range full {
			full[i] = v*sigma + mu
		}
	}
	if positive && flags.positive {
		for i, v := range full {
			if v < 0 {
				full[i] = 0
			}
		}
	}
	f := Forecast{Levels: x.Levels(), Quantiles: make([][]float64, nq-1), Point: make([]float64, horizon)}
	for c := 1; c < nq; c++ {
		f.Quantiles[c-1] = make([]float64, horizon)
		for t := 0; t < horizon; t++ {
			f.Quantiles[c-1][t] = float64(full[t*nq+c])
		}
	}
	for t := range horizon {
		f.Point[t] = float64(full[t*nq+5])
	}
	return f
}

// runningStats is TimesFM's update_running_stats.
type runningStats struct{ n, mu, sigma float32 }

func (s runningStats) update(x []float32, mask []bool) runningStats {
	var incN, sum float32
	for i, v := range x {
		if mask == nil || !mask[i] {
			incN++
			sum += v
		}
	}
	var incMu, incVar float32
	if incN > 0 {
		incMu = sum / incN
		var ss float32
		for i, v := range x {
			if mask == nil || !mask[i] {
				ss += (v - incMu) * (v - incMu)
			}
		}
		incVar = ss / incN
	}
	incSigma := float32(math.Sqrt(float64(incVar)))
	newN := s.n + incN
	if newN == 0 {
		return runningStats{}
	}
	newMu := (s.n*s.mu + incMu*incN) / newN
	newVar := (s.n*s.sigma*s.sigma + incN*incSigma*incSigma + s.n*(s.mu-newMu)*(s.mu-newMu) + incN*(incMu-newMu)*(incMu-newMu)) / newN
	return runningStats{n: newN, mu: newMu, sigma: float32(math.Sqrt(math.Max(0, float64(newVar))))}
}

func revinForward(v, mu, sigma float32) float32 {
	if sigma < 1e-6 {
		sigma = 1
	}
	return (v - mu) / sigma
}

// kvCache holds each layer's keys and values of the patches so far.
type kvCache struct {
	k, v   [][]float32 // per layer: rows of heads×headDim
	n      int         // patches cached
	masked int         // patches masked, at the front
}

// decode is TimesFM_2p5_200M_torch_module.decode: the forecast of mh
// steps (mean and deciles of each), and the last patch's continuous
// quantile spread.
func (x *timesfm) decode(mh int, inputs []float32, mask []bool) ([]float32, []float32) {
	cfg := x.cfg
	p, o := cfg.Patch, cfg.OutputPatch
	nq := len(cfg.Quantiles) + 1
	steps := (mh - 1) / o
	np := len(inputs) / p
	stats := runningStats{}
	mus, sigmas := make([]float32, np), make([]float32, np)
	feats := make([]float32, np*2*p)
	patchMask := make([]bool, np)
	for i := 0; i < np; i++ {
		stats = stats.update(inputs[i*p:(i+1)*p], mask[i*p:(i+1)*p])
		mus[i], sigmas[i] = stats.mu, stats.sigma
		row := feats[i*2*p : (i+1)*2*p]
		for j := 0; j < p; j++ {
			if mask[i*p+j] {
				row[p+j] = 1
				continue
			}
			row[j] = revinForward(inputs[i*p+j], stats.mu, stats.sigma)
		}
		patchMask[i] = mask[(i+1)*p-1]
	}
	cache := &kvCache{k: make([][]float32, len(x.layers)), v: make([][]float32, len(x.layers))}
	h := x.run(feats, np, patchMask, cache)
	// The last patch's point forecast and spread, renormalized.
	last := h[(np-1)*cfg.Hidden : np*cfg.Hidden]
	pt := x.point.apply(last, 1)
	sp := x.quantile.apply(last, 1)
	mu, sigma := mus[np-1], sigmas[np-1]
	for i := range pt {
		pt[i] = pt[i]*sigma + mu
	}
	for i := range sp {
		sp[i] = sp[i]*sigma + mu
	}
	full := make([]float32, 0, (steps+1)*o*nq)
	full = append(full, pt...)
	prev := make([]float32, o)
	for t := range o {
		prev[t] = pt[t*nq+5]
	}
	m := o / p
	for range steps {
		feats := make([]float32, m*2*p)
		pmus, psigmas := make([]float32, m), make([]float32, m)
		for i := 0; i < m; i++ {
			stats = stats.update(prev[i*p:(i+1)*p], nil)
			pmus[i], psigmas[i] = stats.mu, stats.sigma
			for j := 0; j < p; j++ {
				feats[i*2*p+j] = revinForward(prev[i*p+j], stats.mu, stats.sigma)
			}
		}
		h := x.run(feats, m, make([]bool, m), cache)
		out := x.point.apply(h[(m-1)*cfg.Hidden:m*cfg.Hidden], 1)
		for i := range out {
			out[i] = out[i]*psigmas[m-1] + pmus[m-1]
		}
		full = append(full, out...)
		for t := range o {
			prev[t] = out[t*nq+5]
		}
	}
	return full, sp
}

// run runs the transformer over n new patches' features, after the cached
// ones, and returns their states.
func (x *timesfm) run(feats []float32, n int, patchMask []bool, cache *kvCache) []float32 {
	cfg := x.cfg
	md, heads, hd := cfg.Hidden, cfg.NumHeads, cfg.HeadDim
	h := x.tokenizer.apply(feats, n)
	newMasked := 0
	for _, m := range patchMask {
		if m {
			newMasked++
		}
	}
	masked := cache.masked + newMasked
	start := cache.n
	total := start + n
	xs := make([]float32, n*md)
	scores := make([]float32, total)
	for li, l := range x.layers {
		rmsNorm(xs, h, md, l.preAttn, cfg.Eps)
		qkv := l.qkv.apply(xs, n) // n × 3md: q, k, v
		q := make([]float32, n*md)
		k := make([]float32, n*md)
		v := make([]float32, n*md)
		for r := 0; r < n; r++ {
			copy(q[r*md:(r+1)*md], qkv[r*3*md:r*3*md+md])
			copy(k[r*md:(r+1)*md], qkv[r*3*md+md:r*3*md+2*md])
			copy(v[r*md:(r+1)*md], qkv[r*3*md+2*md:(r+1)*3*md])
		}
		for r := 0; r < n; r++ {
			pos := float32(r + start - masked)
			for hh := 0; hh < heads; hh++ {
				x.rotate(q[r*md+hh*hd:r*md+(hh+1)*hd], pos)
				x.rotate(k[r*md+hh*hd:r*md+(hh+1)*hd], pos)
				qrow := q[r*md+hh*hd : r*md+(hh+1)*hd]
				rmsNorm(qrow, qrow, hd, l.queryLN, cfg.Eps)
				for j := range qrow {
					qrow[j] *= l.scale[j]
				}
				krow := k[r*md+hh*hd : r*md+(hh+1)*hd]
				rmsNorm(krow, krow, hd, l.keyLN, cfg.Eps)
			}
		}
		cache.k[li] = append(cache.k[li], k...)
		cache.v[li] = append(cache.v[li], v...)
		ks, vs := cache.k[li], cache.v[li]
		att := make([]float32, n*md)
		for r := 0; r < n; r++ {
			qi := start + r
			lo, hi := masked, qi // keys [masked, qi]
			if hi < lo {
				// A padding patch attends to nothing: zero.
				continue
			}
			for hh := 0; hh < heads; hh++ {
				attention(att[r*md+hh*hd:], md, q[r*md+hh*hd:], md, ks[lo*md+hh*hd:], vs[lo*md+hh*hd:], md, 1, hi-lo+1, hd, nil, scores)
			}
		}
		a := l.out.apply(att, n)
		rmsNorm(a, a, md, l.postAttn, cfg.Eps)
		addTo(a, h)
		rmsNorm(xs, a, md, l.preFF, cfg.Eps)
		f := l.ff0.apply(xs, n)
		silu(f)
		g := l.ff1.apply(f, n)
		rmsNorm(g, g, md, l.postFF, cfg.Eps)
		addTo(g, a)
		h = g
	}
	cache.n = total
	cache.masked = masked
	return h
}

// rotate applies TimesFM's rotary embedding to one head's vector.
func (x *timesfm) rotate(v []float32, pos float32) {
	half := len(v) / 2
	for i := 0; i < half; i++ {
		ang := pos / x.timescale[i]
		sin, cos := float32(math.Sin(float64(ang))), float32(math.Cos(float64(ang)))
		a, b := v[i], v[i+half]
		v[i] = a*cos - b*sin
		v[i+half] = b*cos + a*sin
	}
}
