package tsfm

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// Chronos-Bolt (amazon-science/chronos-forecasting, chronos_bolt.py): the
// context, scaled to zero mean and unit variance, in patches of 16 values
// and their mask, each embedded by a residual block; a T5 encoder over the
// patches and a [REG] token; one step of a T5 decoder from its start
// token, attending to the encoder; and a residual block that maps the
// decoder's state to the quantiles of the next prediction_length (64)
// steps. Longer horizons append each quantile's path to the context and
// forecast again, reducing the paths' quantiles to the model's levels, as
// the official pipeline does.

type chronosBoltConfig struct {
	DModel         int     `json:"d_model"`
	DKV            int     `json:"d_kv"`
	DFF            int     `json:"d_ff"`
	NumLayers      int     `json:"num_layers"`
	NumDecLayers   int     `json:"num_decoder_layers"`
	NumHeads       int     `json:"num_heads"`
	RelBuckets     int     `json:"relative_attention_num_buckets"`
	RelMaxDistance int     `json:"relative_attention_max_distance"`
	Eps            float32 `json:"layer_norm_epsilon"`
	Act            string  `json:"dense_act_fn"`
	FeedForward    string  `json:"feed_forward_proj"`
	DecoderStart   int     `json:"decoder_start_token_id"`
	Chronos        struct {
		ContextLength    int       `json:"context_length"`
		PredictionLength int       `json:"prediction_length"`
		PatchSize        int       `json:"input_patch_size"`
		PatchStride      int       `json:"input_patch_stride"`
		Quantiles        []float64 `json:"quantiles"`
		UseRegToken      bool      `json:"use_reg_token"`
	} `json:"chronos_config"`
}

// t5Block is a layer of T5: self-attention, cross-attention in the
// decoder, and the feed-forward network, each after an RMS norm and
// added to its input.
type t5Block struct {
	ln1, ln2, ln3 []float32
	self, cross   t5Attention
	wi, wo        linear
}

type t5Attention struct {
	q, k, v, o linear
}

type chronosBolt struct {
	name    string
	cfg     chronosBoltConfig
	tensors *modelpack.Tensors
	shared  []float32 // vocab × d
	inEmbed residualBlock
	enc     []t5Block
	encLN   []float32
	relBias []float32 // buckets × heads, the encoder's
	dec     []t5Block
	decLN   []float32
	out     residualBlock
	act     func([]float32)
}

func readConfig(m *modelpack.Model, v any) error {
	b, err := m.ReadFile("config.json")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("model %s's config.json: %w", m.Name, err)
	}
	return nil
}

func activation(name string) (func([]float32), error) {
	switch name {
	case "relu":
		return relu, nil
	case "silu", "swish":
		return silu, nil
	}
	return nil, fmt.Errorf("activation %q is not supported", name)
}

func loadChronosBolt(m *modelpack.Model) (Model, error) {
	var cfg chronosBoltConfig
	if err := readConfig(m, &cfg); err != nil {
		return nil, err
	}
	c := cfg.Chronos
	if cfg.DModel <= 0 || cfg.NumHeads <= 0 || cfg.DKV <= 0 || c.PatchSize <= 0 || c.PatchStride != c.PatchSize ||
		c.PredictionLength <= 0 || len(c.Quantiles) == 0 || !slices.IsSorted(c.Quantiles) {
		return nil, fmt.Errorf("model %s: config.json is not a Chronos-Bolt configuration Igloo runs", m.Name)
	}
	if cfg.FeedForward != "" && cfg.FeedForward != "relu" && cfg.FeedForward != cfg.Act {
		return nil, fmt.Errorf("model %s: gated feed-forward layers are not supported", m.Name)
	}
	if cfg.Eps == 0 {
		cfg.Eps = 1e-6
	}
	if cfg.RelBuckets == 0 {
		cfg.RelBuckets = 32
	}
	if cfg.RelMaxDistance == 0 {
		cfg.RelMaxDistance = 128
	}
	if cfg.NumDecLayers == 0 {
		cfg.NumDecLayers = cfg.NumLayers
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
	b := &chronosBolt{name: m.Name, cfg: cfg, tensors: t, act: act}
	b.shared = w.mat("shared.weight", vocab, d)
	b.inEmbed = w.residualBlock("input_patch_embedding", 2*c.PatchSize, ff, d, true, act)
	b.out = w.residualBlock("output_patch_embedding", d, ff, len(c.Quantiles)*c.PredictionLength, true, act)
	attn := func(w *weights, name string) t5Attention {
		return t5Attention{q: w.linear(name+".q", d, inner, false), k: w.linear(name+".k", d, inner, false),
			v: w.linear(name+".v", d, inner, false), o: w.linear(name+".o", inner, d, false)}
	}
	for i := range cfg.NumLayers {
		bw := w.sub(fmt.Sprintf("encoder.block.%d.layer.", i))
		b.enc = append(b.enc, t5Block{
			ln1: bw.vec("0.layer_norm.weight", d), self: attn(bw, "0.SelfAttention"),
			ln2: bw.vec("1.layer_norm.weight", d), wi: bw.linear("1.DenseReluDense.wi", d, ff, false),
			wo: bw.linear("1.DenseReluDense.wo", ff, d, false),
		})
	}
	b.relBias = w.mat("encoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight", cfg.RelBuckets, cfg.NumHeads)
	b.encLN = w.vec("encoder.final_layer_norm.weight", d)
	for i := range cfg.NumDecLayers {
		bw := w.sub(fmt.Sprintf("decoder.block.%d.layer.", i))
		b.dec = append(b.dec, t5Block{
			ln1: bw.vec("0.layer_norm.weight", d), self: attn(bw, "0.SelfAttention"),
			ln2: bw.vec("1.layer_norm.weight", d), cross: attn(bw, "1.EncDecAttention"),
			ln3: bw.vec("2.layer_norm.weight", d), wi: bw.linear("2.DenseReluDense.wi", d, ff, false),
			wo: bw.linear("2.DenseReluDense.wo", ff, d, false),
		})
	}
	b.decLN = w.vec("decoder.final_layer_norm.weight", d)
	if w.err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("model %s: %w", m.Name, w.err)
	}
	return b, nil
}

func (b *chronosBolt) Name() string       { return b.name }
func (b *chronosBolt) Levels() []float64  { return slices.Clone(b.cfg.Chronos.Quantiles) }
func (b *chronosBolt) MaxContext() int    { return b.cfg.Chronos.ContextLength }
func (b *chronosBolt) MaxHorizon() int    { return 10000 }
func (b *chronosBolt) predictionLen() int { return b.cfg.Chronos.PredictionLength }
func (b *chronosBolt) numQuantiles() int  { return len(b.cfg.Chronos.Quantiles) }

// Forecast forecasts as ChronosBoltPipeline.predict does.
func (b *chronosBolt) Forecast(series []float32, horizon int, opts Options) (Forecast, error) {
	if horizon <= 0 || horizon > b.MaxHorizon() {
		return Forecast{}, fmt.Errorf("%w: %s forecasts 1 to %d steps", ErrInput, b.name, b.MaxHorizon())
	}
	ctxLen := b.cfg.Chronos.ContextLength
	if opts.Context > 0 && opts.Context < ctxLen {
		ctxLen = opts.Context
	}
	ctx := slices.Clone(lastPoints(series, ctxLen))
	nq, pl := b.numQuantiles(), b.predictionLen()
	first := b.forward(ctx) // nq × pl
	preds := [][]float32{first}
	remaining := horizon - pl
	if remaining > 0 {
		// Each quantile's path, appended to the context, forecasts the
		// next steps; their nq×nq paths reduce to nq quantiles.
		paths := make([][]float32, nq)
		for q := range paths {
			paths[q] = slices.Clone(ctx)
		}
		prev := first
		for remaining > 0 {
			samples := make([][]float32, 0, nq*nq)
			for q := range paths {
				paths[q] = append(paths[q], prev[q*pl:(q+1)*pl]...)
				if len(paths[q]) > b.cfg.Chronos.ContextLength {
					paths[q] = paths[q][len(paths[q])-b.cfg.Chronos.ContextLength:]
				}
				out := b.forward(paths[q])
				for r := 0; r < nq; r++ {
					samples = append(samples, out[r*pl:(r+1)*pl])
				}
			}
			next := make([]float32, nq*pl)
			col := make([]float64, len(samples))
			for t := 0; t < pl; t++ {
				for s, row := range samples {
					col[s] = float64(row[t])
				}
				slices.Sort(col)
				for q, level := range b.cfg.Chronos.Quantiles {
					next[q*pl+t] = float32(linearQuantile(col, level))
				}
			}
			preds = append(preds, next)
			prev = next
			remaining -= pl
		}
	}
	f := Forecast{Levels: b.Levels(), Quantiles: make([][]float64, nq), Point: make([]float64, horizon)}
	for q := range nq {
		f.Quantiles[q] = make([]float64, 0, horizon)
		for _, p := range preds {
			for _, v := range p[q*pl : (q+1)*pl] {
				if len(f.Quantiles[q]) < horizon {
					f.Quantiles[q] = append(f.Quantiles[q], float64(v))
				}
			}
		}
	}
	median := slices.Index(b.cfg.Chronos.Quantiles, 0.5)
	for t := range horizon {
		if median >= 0 {
			f.Point[t] = f.Quantiles[median][t]
		} else {
			f.Point[t] = f.QuantileAt(0.5, t)
		}
	}
	return f, nil
}

// linearQuantile is torch.quantile's default (linear) interpolation of
// sorted values.
func linearQuantile(sorted []float64, p float64) float64 {
	pos := p * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(sorted)-1)
	w := pos - float64(lo)
	return sorted[lo] + w*(sorted[hi]-sorted[lo])
}

// instanceNorm scales x to zero mean and unit variance over its values
// that are numbers, as Chronos' InstanceNorm: loc, scale.
func instanceNorm(x []float32) (float32, float32) {
	var sum float64
	n := 0
	for _, v := range x {
		if !isNaN32(v) {
			sum += float64(v)
			n++
		}
	}
	if n == 0 {
		return 0, 1
	}
	loc := float32(sum / float64(n))
	var ss float64
	for _, v := range x {
		if !isNaN32(v) {
			d := float64(v - loc)
			ss += d * d
		}
	}
	scale := float32(math.Sqrt(ss / float64(n)))
	if scale == 0 {
		scale = 1e-5
	}
	return loc, scale
}

// forward runs the model on a context, and returns its quantiles' next
// prediction_length steps, quantile by quantile.
func (b *chronosBolt) forward(context []float32) []float32 {
	cfg := b.cfg
	ps := cfg.Chronos.PatchSize
	d := cfg.DModel
	loc, scale := instanceNorm(context)
	// Left-pad to whole patches; padding is missing.
	pad := (ps - len(context)%ps) % ps
	n := (len(context) + pad) / ps
	feats := make([]float32, n*2*ps)
	observed := make([]bool, n, n+1)
	for p := 0; p < n; p++ {
		row := feats[p*2*ps : (p+1)*2*ps]
		for i := 0; i < ps; i++ {
			j := p*ps + i - pad
			if j < 0 || isNaN32(context[j]) {
				continue
			}
			row[i] = (context[j] - loc) / scale
			row[ps+i] = 1
			observed[p] = true
		}
	}
	h := b.inEmbed.apply(feats, n)
	if cfg.Chronos.UseRegToken {
		h = append(h, b.shared[d:2*d]...)
		observed = append(observed, true)
		n++
	}
	enc := b.encode(h, n, observed)
	dec := b.decode(enc, n, observed)
	out := b.out.apply(dec, 1)
	for i, v := range out {
		out[i] = v*scale + loc
	}
	return out
}

// t5Bucket is T5's relative position bucket.
func t5Bucket(rel int, bidirectional bool, numBuckets, maxDistance int) int {
	bucket := 0
	if bidirectional {
		numBuckets /= 2
		if rel > 0 {
			bucket += numBuckets
		}
		if rel < 0 {
			rel = -rel
		}
	} else {
		rel = -min(rel, 0)
	}
	maxExact := numBuckets / 2
	if rel < maxExact {
		return bucket + rel
	}
	large := maxExact + int(math.Log(float64(rel)/float64(maxExact))/math.Log(float64(maxDistance)/float64(maxExact))*
		float64(numBuckets-maxExact))
	return bucket + min(large, numBuckets-1)
}

// encode runs the T5 encoder over n tokens.
func (b *chronosBolt) encode(h []float32, n int, observed []bool) []float32 {
	cfg := b.cfg
	d, heads, dkv := cfg.DModel, cfg.NumHeads, cfg.DKV
	// The position bias of each head and pair of tokens, with the mask.
	bias := make([]float32, heads*n*n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			bucket := t5Bucket(j-i, true, cfg.RelBuckets, cfg.RelMaxDistance)
			for hd := 0; hd < heads; hd++ {
				v := b.relBias[bucket*heads+hd]
				if !observed[j] {
					v += minFloat32
				}
				bias[hd*n*n+i*n+j] = v
			}
		}
	}
	x := make([]float32, n*d)
	scores := make([]float32, n)
	for _, blk := range b.enc {
		rmsNorm(x, h, d, blk.ln1, cfg.Eps)
		q, k, v := blk.self.q.apply(x, n), blk.self.k.apply(x, n), blk.self.v.apply(x, n)
		att := make([]float32, n*heads*dkv)
		for hd := 0; hd < heads; hd++ {
			hb := bias[hd*n*n:]
			attention(att[hd*dkv:], heads*dkv, q[hd*dkv:], heads*dkv, k[hd*dkv:], v[hd*dkv:], heads*dkv, n, n, dkv,
				func(i, j int) float32 { return hb[i*n+j] }, scores)
		}
		addTo(h, blk.self.o.apply(att, n))
		rmsNorm(x, h, d, blk.ln2, cfg.Eps)
		f := blk.wi.apply(x, n)
		b.act(f)
		addTo(h, blk.wo.apply(f, n))
	}
	out := make([]float32, n*d)
	rmsNorm(out, h, d, b.encLN, cfg.Eps)
	return out
}

// decode runs one step of the T5 decoder from its start token.
func (b *chronosBolt) decode(enc []float32, n int, observed []bool) []float32 {
	cfg := b.cfg
	d, heads, dkv := cfg.DModel, cfg.NumHeads, cfg.DKV
	h := slices.Clone(b.shared[cfg.DecoderStart*d : (cfg.DecoderStart+1)*d])
	x := make([]float32, d)
	scores := make([]float32, n)
	for _, blk := range b.dec {
		// Self-attention over the one token is its value.
		rmsNorm(x, h, d, blk.ln1, cfg.Eps)
		addTo(h, blk.self.o.apply(blk.self.v.apply(x, 1), 1))
		// Cross-attention to the encoder's tokens.
		rmsNorm(x, h, d, blk.ln2, cfg.Eps)
		q := blk.cross.q.apply(x, 1)
		k, v := blk.cross.k.apply(enc, n), blk.cross.v.apply(enc, n)
		att := make([]float32, heads*dkv)
		mask := func(_, j int) float32 {
			if observed[j] {
				return 0
			}
			return minFloat32
		}
		for hd := 0; hd < heads; hd++ {
			attention(att[hd*dkv:], heads*dkv, q[hd*dkv:], heads*dkv, k[hd*dkv:], v[hd*dkv:], heads*dkv, 1, n, dkv, mask, scores)
		}
		addTo(h, blk.cross.o.apply(att, 1))
		rmsNorm(x, h, d, blk.ln3, cfg.Eps)
		f := blk.wi.apply(x, 1)
		b.act(f)
		addTo(h, blk.wo.apply(f, 1))
	}
	out := make([]float32, d)
	rmsNorm(out, h, d, b.decLN, cfg.Eps)
	return out
}

func addTo(dst, src []float32) {
	for i := range dst {
		dst[i] += src[i]
	}
}
