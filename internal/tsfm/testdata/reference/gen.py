"""Reference outputs for Igloo's Go inference of time-series foundation models.

Made with the official implementations: chronos-forecasting 2.3.2, timesfm at
commit e51928e, torch 2.8.0 (CPU), transformers 4.57.6 (see Dockerfile):

  docker build -t igloo-tsfm-ref .
  docker run --rm -v "$PWD/out":/out -v "$PWD":/work -w /work igloo-tsfm-ref python gen.py tiny
  docker run --rm -v /path/to/pack:/models:ro -v "$PWD/out":/out -v "$PWD":/work -w /work igloo-tsfm-ref python gen.py real

Writes, under /out:
  tiny/<arch>/config.json, model.safetensors, expected.json: tiny random models of each
    architecture, built with the official classes, and their official pipelines' forecasts.
  real/<model>.json: forecasts of the installed real models (from /models) on synthetic series.
"""

import dataclasses
import json
import math
import os
import sys

import numpy as np
import torch

torch.set_num_threads(4)
OUT = "/out"
MODELS = "/models"


def series_set(n_long=100):
    t = np.arange(n_long, dtype=np.float64)
    seasonal = 10 + 3 * np.sin(2 * np.pi * t / 7) + 0.05 * t + 0.4 * np.cos(t * 1.7)
    with_nan = seasonal.copy()
    with_nan[n_long // 2] = np.nan
    with_nan[n_long // 2 + 3] = np.nan
    return {
        "seasonal50": seasonal[:50].tolist(),
        "long_with_gaps": [None if math.isnan(v) else v for v in with_nan.tolist()],
        "constant": [5.0] * 40,
        "short": seasonal[:5].tolist(),
        "negative": (-seasonal[:37] + 4).tolist(),
    }


def to_array(values):
    return np.array([np.nan if v is None else v for v in values], dtype=np.float32)


def randomize(model, scale, seed):
    g = torch.Generator().manual_seed(seed)
    with torch.no_grad():
        for _, p in sorted(model.named_parameters(), key=lambda kv: kv[0]):
            p.add_(torch.randn(p.shape, generator=g) * scale)


def dump(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
        f.write("\n")


def r(x):
    return [float("%.7g" % v) for v in np.asarray(x, dtype=np.float64).ravel()]


# --- Chronos-Bolt -------------------------------------------------------------


def chronos_bolt_tiny():
    from transformers import T5Config
    from chronos.chronos_bolt import ChronosBoltModelForForecasting, ChronosBoltPipeline

    cfg = T5Config(d_model=16, d_kv=8, d_ff=32, num_layers=2, num_decoder_layers=2, num_heads=2,
                   relative_attention_num_buckets=32, relative_attention_max_distance=128, dropout_rate=0.0,
                   feed_forward_proj="relu", decoder_start_token_id=0, pad_token_id=0, eos_token_id=1,
                   initializer_factor=1.0, vocab_size=2, is_encoder_decoder=True, layer_norm_epsilon=1e-6)
    cfg.chronos_config = dict(context_length=64, prediction_length=8, input_patch_size=16, input_patch_stride=16,
                              quantiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], use_reg_token=True)
    cfg.architectures = ["ChronosBoltModelForForecasting"]
    cfg.chronos_pipeline_class = "ChronosBoltPipeline"
    torch.manual_seed(1)
    model = ChronosBoltModelForForecasting(cfg).eval()
    randomize(model, 0.2, 11)
    d = os.path.join(OUT, "tiny", "chronos-bolt")
    model.save_pretrained(d, safe_serialization=True)
    pipe = ChronosBoltPipeline(model=model)
    cases = []
    for name, values in series_set().items():
        for h in (8, 20):
            pred = pipe.predict(torch.tensor(to_array(values)), prediction_length=h)  # (1, 9, h)
            cases.append({"series": name, "values": values, "horizon": h, "quantiles": r(pred[0])})
    dump(os.path.join(d, "expected.json"), {"quantile_levels": cfg.chronos_config["quantiles"], "cases": cases})


def chronos_bolt_real(name, cases_out):
    from chronos import BaseChronosPipeline

    pipe = BaseChronosPipeline.from_pretrained(os.path.join(MODELS, name), device_map="cpu", torch_dtype=torch.float32)
    cases = []
    s = series_set(300)
    for sname, h in (("seasonal50", 24), ("long_with_gaps", 24), ("long_with_gaps", 100)):
        values = s[sname]
        pred = pipe.predict(torch.tensor(to_array(values)), prediction_length=h)
        cases.append({"series": sname, "values": values, "horizon": h, "quantiles": r(pred[0])})
    cases_out[name] = {"quantile_levels": pipe.quantiles, "cases": cases}


# --- Chronos-2 ----------------------------------------------------------------


def chronos2_tiny():
    from chronos.chronos2.config import Chronos2CoreConfig
    from chronos.chronos2.model import Chronos2Model
    from chronos.chronos2.pipeline import Chronos2Pipeline

    q = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
    cfg = Chronos2CoreConfig(d_model=16, d_kv=8, d_ff=32, num_layers=2, num_heads=2, dropout_rate=0.0,
                             initializer_factor=1.0, rope_theta=10000.0, attn_implementation="eager")
    cfg.chronos_config = dict(context_length=64, input_patch_size=16, input_patch_stride=16, output_patch_size=16,
                              max_output_patches=2, quantiles=q, use_reg_token=True, use_arcsinh=True,
                              time_encoding_scale=64)
    cfg.architectures = ["Chronos2Model"]
    cfg.chronos_pipeline_class = "Chronos2Pipeline"
    torch.manual_seed(2)
    model = Chronos2Model(cfg).eval()
    randomize(model, 0.2, 12)
    d = os.path.join(OUT, "tiny", "chronos-2")
    model.save_pretrained(d, safe_serialization=True)
    pipe = Chronos2Pipeline(model=model)
    cases = []
    for name, values in series_set().items():
        for h in (10, 40):
            pred = pipe.predict([to_array(values)], prediction_length=h)[0]  # (1, q, h)
            cases.append({"series": name, "values": values, "horizon": h, "quantiles": r(pred[0])})
    dump(os.path.join(d, "expected.json"), {"quantile_levels": q, "cases": cases})


def chronos2_real(name, cases_out):
    from chronos import BaseChronosPipeline

    pipe = BaseChronosPipeline.from_pretrained(os.path.join(MODELS, name), device_map="cpu", torch_dtype=torch.float32)
    cases = []
    s = series_set(300)
    for sname, h in (("seasonal50", 24), ("long_with_gaps", 24), ("long_with_gaps", 1100)):
        if name == "chronos-2-small" and h > 24:
            continue
        values = s[sname]
        pred = pipe.predict([to_array(values)], prediction_length=h)[0]
        cases.append({"series": sname, "values": values, "horizon": h, "quantiles": r(pred[0])})
    cases_out[name] = {"quantile_levels": pipe.quantiles, "cases": cases}


# --- TimesFM 2.5 --------------------------------------------------------------

FLAGS = {
    "recommended": dict(normalize_inputs=True, use_continuous_quantile_head=True, force_flip_invariance=True,
                        infer_is_positive=True, fix_quantile_crossing=True),
    "raw": dict(normalize_inputs=False, use_continuous_quantile_head=False, force_flip_invariance=False,
                infer_is_positive=False, fix_quantile_crossing=False),
}


def timesfm_tiny():
    from safetensors.torch import save_file
    from timesfm import configs
    from timesfm.timesfm_2p5 import timesfm_2p5_base, timesfm_2p5_torch

    tiny = dataclasses.replace(
        timesfm_2p5_base.TimesFM_2p5_200M_Definition(),
        input_patch_len=8, output_patch_len=16, output_quantile_len=32,
        tokenizer=configs.ResidualBlockConfig(input_dims=16, hidden_dims=32, output_dims=32, use_bias=True, activation="swish"),
        stacked_transformers=configs.StackedTransformersConfig(num_layers=2, transformer=configs.TransformerConfig(
            model_dims=32, hidden_dims=32, num_heads=2, attention_norm="rms", feedforward_norm="rms", qk_norm="rms",
            use_bias=False, use_rotary_position_embeddings=True, ff_activation="swish", fuse_qkv=True)),
        output_projection_point=configs.ResidualBlockConfig(input_dims=32, hidden_dims=32, output_dims=160, use_bias=False,
                                                            activation="swish"),
        output_projection_quantiles=configs.ResidualBlockConfig(input_dims=32, hidden_dims=32, output_dims=320,
                                                                use_bias=False, activation="swish"),
    )
    timesfm_2p5_torch.TimesFM_2p5_200M_torch_module.config = tiny
    torch.manual_seed(3)
    m = timesfm_2p5_torch.TimesFM_2p5_200M_torch(torch_compile=False)
    m.model.eval()
    randomize(m.model, 0.3, 13)
    d = os.path.join(OUT, "tiny", "timesfm-2.5")
    os.makedirs(d, exist_ok=True)
    save_file({k: v.contiguous() for k, v in m.model.state_dict().items()}, os.path.join(d, "model.safetensors"))
    dump(os.path.join(d, "config.json"), {
        "architectures": ["TimesFmModelForPrediction"], "context_length": 16384, "head_dim": 16, "hidden_size": 32,
        "horizon_length": 16, "intermediate_size": 32, "model_type": "timesfm", "num_attention_heads": 2,
        "num_hidden_layers": 2, "patch_length": 8, "quantile_horizon_length": 32,
        "quantiles": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], "rms_norm_eps": 1e-06, "torch_compile": False})
    cases = []
    for flags, max_horizon in (("recommended", 32), ("raw", 48)):
        m.compile(configs.ForecastConfig(max_context=64, max_horizon=max_horizon, per_core_batch_size=1, **FLAGS[flags]))
        for name, values in series_set().items():
            for h in (5, max_horizon):
                point, quantiles = m.forecast(horizon=h, inputs=[to_array(values).astype(np.float64)])
                cases.append({"series": name, "values": values, "horizon": h, "flags": flags, "max_context": 64,
                              "max_horizon": max_horizon, "point": r(point[0]), "quantiles": r(quantiles[0])})
    dump(os.path.join(d, "expected.json"), {"cases": cases})


def timesfm_real(cases_out):
    from timesfm import configs
    from timesfm.timesfm_2p5 import timesfm_2p5_base, timesfm_2p5_torch

    timesfm_2p5_torch.TimesFM_2p5_200M_torch_module.config = timesfm_2p5_base.TimesFM_2p5_200M_Definition()
    m = timesfm_2p5_torch.TimesFM_2p5_200M_torch(torch_compile=False)
    m.load_checkpoint(os.path.join(MODELS, "timesfm-2.5", "model.safetensors"), torch_compile=False)
    cases = []
    s = series_set(300)
    for sname, h, max_context, max_horizon in (("seasonal50", 24, 64, 128), ("long_with_gaps", 24, 512, 128),
                                               ("long_with_gaps", 300, 512, 384)):
        m.compile(configs.ForecastConfig(max_context=max_context, max_horizon=max_horizon, per_core_batch_size=1,
                                         **FLAGS["recommended"]))
        point, quantiles = m.forecast(horizon=h, inputs=[to_array(s[sname]).astype(np.float64)])
        cases.append({"series": sname, "values": s[sname], "horizon": h, "flags": "recommended", "max_context": max_context,
                      "max_horizon": max_horizon, "point": r(point[0]), "quantiles": r(quantiles[0])})
    cases_out["timesfm-2.5"] = {"cases": cases}


if __name__ == "__main__":
    what = sys.argv[1:] or ["tiny", "real"]
    if "tiny" in what:
        chronos_bolt_tiny()
        chronos2_tiny()
        timesfm_tiny()
    if "real" in what:
        out = {}
        for n in ("chronos-bolt-tiny", "chronos-bolt-small"):
            chronos_bolt_real(n, out)
        for n in ("chronos-2", "chronos-2-small"):
            chronos2_real(n, out)
        timesfm_real(out)
        for name, v in out.items():
            dump(os.path.join(OUT, "real", name + ".json"), v)
    print("done")
