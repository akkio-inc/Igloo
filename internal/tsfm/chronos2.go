package tsfm

import (
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// Chronos-2 (amazon-science/chronos-forecasting, chronos2/): an encoder
// alone. The context, scaled and then arcsinh-squashed, in patches of 16
// values, their mask and their time (position over the context length),
// each embedded by a residual block; a [REG] token; and a token for each
// output patch, holding only its time. Each layer attends along time, with
// rotary positions, then across the series of a group (one series here: a
// series is its own group), then applies its feed-forward network. The
// output patches' states map to the quantiles of their 16 steps. Horizons
// past max_output_patches×16 steps append nine quantiles' paths to the
// context and forecast again, weighting the paths' quantiles, as the
// official pipeline does.

type chronos2Config struct {
	DModel    int     `json:"d_model"`
	DKV       int     `json:"d_kv"`
	DFF       int     `json:"d_ff"`
	NumLayers int     `json:"num_layers"`
	NumHeads  int     `json:"num_heads"`
	Eps       float32 `json:"layer_norm_epsilon"`
	Act       string  `json:"dense_act_fn"`
	RopeTheta float64 `json:"rope_theta"`
	Chronos   struct {
		ContextLength     int       `json:"context_length"`
		PatchSize         int       `json:"input_patch_size"`
		PatchStride       int       `json:"input_patch_stride"`
		OutputPatchSize   int       `json:"output_patch_size"`
		MaxOutputPatches  int       `json:"max_output_patches"`
		Quantiles         []float64 `json:"quantiles"`
		UseRegToken       bool      `json:"use_reg_token"`
		UseArcsinh        bool      `json:"use_arcsinh"`
		TimeEncodingScale int       `json:"time_encoding_scale"`
	} `json:"chronos_config"`
}

type chronos2Block struct {
	ln1, ln2, ln3 []float32
	time, group   t5Attention
	wi, wo        linear
}

type chronos2 struct {
	name     string
	cfg      chronos2Config
	tensors  *modelpack.Tensors
	shared   []float32
	inEmbed  residualBlock
	blocks   []chronos2Block
	finalLN  []float32
	out      residualBlock
	act      func([]float32)
	invFreq  []float64
	unrolled []float64
}

func loadChronos2(m *modelpack.Model) (Model, error) {
	var cfg chronos2Config
	if err := readConfig(m, &cfg); err != nil {
		return nil, err
	}
	c := &cfg.Chronos
	if cfg.DModel <= 0 || cfg.NumHeads <= 0 || cfg.DKV <= 0 || c.PatchSize <= 0 || c.PatchStride != c.PatchSize ||
		c.OutputPatchSize != c.PatchSize || len(c.Quantiles) == 0 || !slices.IsSorted(c.Quantiles) {
		return nil, fmt.Errorf("model %s: config.json is not a Chronos-2 configuration Igloo runs", m.Name)
	}
	if cfg.Eps == 0 {
		cfg.Eps = 1e-6
	}
	if cfg.RopeTheta == 0 {
		cfg.RopeTheta = 10000
	}
	if c.MaxOutputPatches == 0 {
		c.MaxOutputPatches = 1
	}
	if c.TimeEncodingScale == 0 {
		c.TimeEncodingScale = c.ContextLength
	}
	act, err := activation(cfg.Act)
	if err != nil {
		return nil, fmt.Errorf("model %s: %w", m.Name, err)
	}
	t, err := modelpack.OpenTensors(m.Path("model.safetensors"))
	if err != nil {
		return nil, err
	}
	w := &weights{t: t}
	d, inner, ff := cfg.DModel, cfg.NumHeads*cfg.DKV, cfg.DFF
	vocab := 1
	if c.UseRegToken {
		vocab = 2
	}
	x := &chronos2{name: m.Name, cfg: cfg, tensors: t, act: act}
	x.shared = w.mat("shared.weight", vocab, d)
	x.inEmbed = w.residualBlock("input_patch_embedding", 3*c.PatchSize, ff, d, true, act)
	x.out = w.residualBlock("output_patch_embedding", d, ff, len(c.Quantiles)*c.OutputPatchSize, true, act)
	attn := func(w *weights, name string) t5Attention {
		return t5Attention{q: w.linear(name+".q", d, inner, false), k: w.linear(name+".k", d, inner, false),
			v: w.linear(name+".v", d, inner, false), o: w.linear(name+".o", inner, d, false)}
	}
	for i := range cfg.NumLayers {
		bw := w.sub(fmt.Sprintf("encoder.block.%d.layer.", i))
		x.blocks = append(x.blocks, chronos2Block{
			ln1: bw.vec("0.layer_norm.weight", d), time: attn(bw, "0.self_attention"),
			ln2: bw.vec("1.layer_norm.weight", d), group: attn(bw, "1.self_attention"),
			ln3: bw.vec("2.layer_norm.weight", d), wi: bw.linear("2.mlp.wi", d, ff, false), wo: bw.linear("2.mlp.wo", ff, d, false),
		})
	}
	x.finalLN = w.vec("encoder.final_layer_norm.weight", d)
	if w.err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("model %s: %w", m.Name, w.err)
	}
	for i := 0; i < cfg.DKV; i += 2 {
		x.invFreq = append(x.invFreq, 1/math.Pow(cfg.RopeTheta, float64(float32(i)/float32(cfg.DKV))))
	}
	for _, q := range []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9} {
		if slices.Contains(c.Quantiles, q) {
			x.unrolled = append(x.unrolled, q)
		}
	}
	return x, nil
}

func (x *chronos2) Name() string      { return x.name }
func (x *chronos2) Levels() []float64 { return slices.Clone(x.cfg.Chronos.Quantiles) }
func (x *chronos2) MaxContext() int   { return x.cfg.Chronos.ContextLength }
func (x *chronos2) MaxHorizon() int   { return 10000 }

// Forecast forecasts as Chronos2Pipeline.predict does for one univariate
// series.
func (x *chronos2) Forecast(series []float32, horizon int, opts Options) (Forecast, error) {
	if horizon <= 0 || horizon > x.MaxHorizon() {
		return Forecast{}, fmt.Errorf("%w: %s forecasts 1 to %d steps", ErrInput, x.name, x.MaxHorizon())
	}
	c := x.cfg.Chronos
	ctxLen := c.ContextLength
	if opts.Context > 0 && opts.Context < ctxLen {
		ctxLen = opts.Context
	}
	ctx := slices.Clone(lastPoints(series, ctxLen))
	nq, ps := len(c.Quantiles), c.OutputPatchSize
	patches := func(remaining int) int { return min((remaining+ps-1)/ps, c.MaxOutputPatches) }

	np := patches(horizon)
	first := x.forward(ctx, np)
	steps := np * ps
	preds := [][]float32{first}
	widths := []int{steps}
	remaining := horizon - steps
	if remaining > 0 {
		paths := make([][]float32, len(x.unrolled))
		for p := range paths {
			paths[p] = slices.Clone(ctx)
		}
		weights := outer(probMass(x.unrolled), probMass(c.Quantiles))
		levels := levels32(c.Quantiles)
		prev, prevSteps := first, steps
		for remaining > 0 {
			np := patches(remaining)
			steps := np * ps
			// Each unrolled level's path, from the last prediction.
			samples := make([][]float32, 0, len(paths)*nq)
			for p, level := range x.unrolled {
				path := make([]float32, prevSteps)
				for t := 0; t < prevSteps; t++ {
					path[t] = interpolateQuantile(levels, func(q int) float32 { return prev[q*prevSteps+t] }, float32(level))
				}
				paths[p] = append(paths[p], path...)
				if len(paths[p]) > c.ContextLength {
					paths[p] = paths[p][len(paths[p])-c.ContextLength:]
				}
				out := x.forward(paths[p], np)
				for q := 0; q < nq; q++ {
					samples = append(samples, out[q*steps:(q+1)*steps])
				}
			}
			next := make([]float32, nq*steps)
			vals := make([]float32, len(samples))
			for t := 0; t < steps; t++ {
				for s, row := range samples {
					vals[s] = row[t]
				}
				qs := weightedQuantiles(levels, weights, vals)
				for q := range nq {
					next[q*steps+t] = qs[q]
				}
			}
			preds = append(preds, next)
			widths = append(widths, steps)
			prev, prevSteps = next, steps
			remaining -= steps
		}
	}
	f := Forecast{Levels: x.Levels(), Quantiles: make([][]float64, nq), Point: make([]float64, horizon)}
	for q := range nq {
		f.Quantiles[q] = make([]float64, 0, horizon)
		for i, p := range preds {
			w := widths[i]
			for _, v := range p[q*w : (q+1)*w] {
				if len(f.Quantiles[q]) < horizon {
					f.Quantiles[q] = append(f.Quantiles[q], float64(v))
				}
			}
		}
	}
	median := slices.Index(c.Quantiles, 0.5)
	for t := range horizon {
		if median >= 0 {
			f.Point[t] = f.Quantiles[median][t]
		} else {
			f.Point[t] = f.QuantileAt(0.5, t)
		}
	}
	return f, nil
}

// probMass is Chronos2Pipeline._get_prob_mass_per_quantile_level, in
// float32 as PyTorch computes it.
func probMass(levels []float64) []float32 {
	out := make([]float32, len(levels))
	var sum float32
	for i := range levels {
		lo, hi := float32(0), float32(1)
		if i > 0 {
			lo = float32(levels[i-1])
		}
		if i+1 < len(levels) {
			hi = float32(levels[i+1])
		}
		out[i] = (hi - lo) / 2
		sum += out[i]
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}

func outer(a, b []float32) []float32 {
	out := make([]float32, 0, len(a)*len(b))
	for _, x := range a {
		for _, y := range b {
			out = append(out, x*y)
		}
	}
	return out
}

// interpolateQuantile is chronos.utils.interpolate_quantiles for one
// query level, in float32: linear between the levels, the outermost
// values beyond.
func interpolateQuantile(levels []float32, value func(int) float32, q float32) float32 {
	// The levels padded with 0 and 1, holding the outermost values.
	n := len(levels)
	lv := make([]float32, 0, n+2)
	at := make([]int, 0, n+2)
	if levels[0] > 0 {
		lv, at = append(lv, 0), append(at, 0)
	}
	for i, l := range levels {
		lv, at = append(lv, l), append(at, i)
	}
	if levels[n-1] < 1 {
		lv, at = append(lv, 1), append(at, n-1)
	}
	upper := sort.Search(len(lv), func(i int) bool { return lv[i] > q })
	upper = min(upper, len(lv)-1)
	lower := max(upper-1, 0)
	lo, hi := lv[lower], lv[upper]
	var w float32
	if hi != lo {
		w = (q - lo) / (hi - lo)
	}
	a, b := value(at[lower]), value(at[upper])
	return a + w*(b-a)
}

func levels32(levels []float64) []float32 {
	out := make([]float32, len(levels))
	for i, l := range levels {
		out[i] = float32(l)
	}
	return out
}

// weightedQuantiles is chronos.utils.weighted_quantile of one step's
// samples: weights normalized in float32, cumulated in float64 as
// PyTorch's CPU cumsum does, and interpolated in float32.
func weightedQuantiles(levels []float32, weights []float32, samples []float32) []float32 {
	idx := make([]int, len(samples))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return samples[idx[a]] < samples[idx[b]] })
	var total float32
	for _, w := range weights {
		total += w
	}
	cum := make([]float32, len(samples))
	sorted := make([]float32, len(samples))
	var acc float64
	for i, j := range idx {
		acc += float64(weights[j] / total)
		cum[i] = float32(math.Min(1, math.Max(0, acc)))
		sorted[i] = samples[j]
	}
	out := make([]float32, len(levels))
	for i, q := range levels {
		out[i] = interpolateQuantile(cum, func(k int) float32 { return sorted[k] }, q)
	}
	return out
}

// forward runs the model on a context for np output patches, and returns
// the quantiles of their steps, quantile by quantile.
func (x *chronos2) forward(context []float32, np int) []float32 {
	cfg := x.cfg
	c := cfg.Chronos
	ps := c.PatchSize
	d := cfg.DModel
	loc, scale := instanceNorm(context)
	pad := (ps - len(context)%ps) % ps
	nc := (len(context) + pad) / ps
	scaleT := float32(c.TimeEncodingScale)
	feats := make([]float32, (nc+np)*3*ps)
	observed := make([]bool, 0, nc+1+np)
	finalLen := nc * ps
	for p := 0; p < nc; p++ {
		row := feats[p*3*ps : (p+1)*3*ps]
		seen := false
		for i := 0; i < ps; i++ {
			row[i] = float32(-finalLen+p*ps+i) / scaleT
			j := p*ps + i - pad
			if j < 0 || isNaN32(context[j]) {
				continue
			}
			v := (context[j] - loc) / scale
			if c.UseArcsinh {
				v = float32(math.Asinh(float64(v)))
			}
			row[ps+i] = v
			row[2*ps+i] = 1
			seen = true
		}
		observed = append(observed, seen)
	}
	for p := 0; p < np; p++ {
		row := feats[(nc+p)*3*ps : (nc+p+1)*3*ps]
		for i := 0; i < ps; i++ {
			row[i] = float32(p*ps+i) / scaleT
		}
	}
	emb := x.inEmbed.apply(feats, nc+np)
	n := nc + np
	h := make([]float32, 0, (n+1)*d)
	h = append(h, emb[:nc*d]...)
	if c.UseRegToken {
		h = append(h, x.shared[d:2*d]...)
		observed = append(observed, true)
		n++
	}
	h = append(h, emb[nc*d:]...)
	for range np {
		observed = append(observed, true)
	}
	h = x.encode(h, n, observed)
	states := h[(n-np)*d:]
	out := x.out.apply(states, np) // np × (nq·ps)
	nq := len(c.Quantiles)
	res := make([]float32, nq*np*ps)
	for p := 0; p < np; p++ {
		for q := 0; q < nq; q++ {
			for i := 0; i < ps; i++ {
				v := out[p*nq*ps+q*ps+i]
				if c.UseArcsinh {
					v = float32(math.Sinh(float64(v)))
				}
				res[q*np*ps+p*ps+i] = v*scale + loc
			}
		}
	}
	return res
}

// rope rotates each head's halves of x's rows by their positions.
func (x *chronos2) rope(v []float32, n int) {
	heads, dkv := x.cfg.NumHeads, x.cfg.DKV
	half := dkv / 2
	for pos := 0; pos < n; pos++ {
		for i, f := range x.invFreq {
			ang := float32(float32(pos) * float32(f))
			cos, sin := float32(math.Cos(float64(ang))), float32(math.Sin(float64(ang)))
			for hd := 0; hd < heads; hd++ {
				row := v[pos*heads*dkv+hd*dkv:]
				a, b := row[i], row[i+half]
				row[i] = a*cos - b*sin
				row[i+half] = b*cos + a*sin
			}
		}
	}
}

func (x *chronos2) encode(h []float32, n int, observed []bool) []float32 {
	cfg := x.cfg
	d, heads, dkv := cfg.DModel, cfg.NumHeads, cfg.DKV
	xs := make([]float32, n*d)
	scores := make([]float32, n)
	mask := func(_, j int) float32 {
		if observed[j] {
			return 0
		}
		return minFloat32
	}
	for _, blk := range x.blocks {
		// Time attention, with rotary positions.
		rmsNorm(xs, h, d, blk.ln1, cfg.Eps)
		q, k, v := blk.time.q.apply(xs, n), blk.time.k.apply(xs, n), blk.time.v.apply(xs, n)
		x.rope(q, n)
		x.rope(k, n)
		att := make([]float32, n*heads*dkv)
		for hd := 0; hd < heads; hd++ {
			attention(att[hd*dkv:], heads*dkv, q[hd*dkv:], heads*dkv, k[hd*dkv:], v[hd*dkv:], heads*dkv, n, n, dkv, mask, scores)
		}
		addTo(h, blk.time.o.apply(att, n))
		// Group attention across a group of one series: each token
		// attends to itself alone.
		rmsNorm(xs, h, d, blk.ln2, cfg.Eps)
		addTo(h, blk.group.o.apply(blk.group.v.apply(xs, n), n))
		// Feed-forward.
		rmsNorm(xs, h, d, blk.ln3, cfg.Eps)
		f := blk.wi.apply(xs, n)
		x.act(f)
		addTo(h, blk.wo.apply(f, n))
	}
	out := make([]float32, n*d)
	rmsNorm(out, h, d, x.finalLN, cfg.Eps)
	return out
}
