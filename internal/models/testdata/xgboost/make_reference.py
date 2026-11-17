"""Writes the data and the reference values IGLOO.ML.XGBOOST's tests check
Igloo against: what XGBoost's Python package trains and predicts on the
same rows with the same parameters.

    python make_reference.py

It needs numpy, pandas and XGBoost's Python package at the release
third_party/xgboost/build.sh builds (results differ between releases), and
writes binary.csv, survival.csv and reference.json next to itself. Run it
again after bumping XGBoost; the tests then check the new release against
its own Python package. The Python package in XGBoost's source tree works
too, with the shared library built from the same tree in xgboost/lib.
"""

import hashlib
import json
import pathlib
import platform
import sys

import numpy as np
import pandas as pd
import xgboost as xgb

here = pathlib.Path(__file__).resolve().parent
rng = np.random.default_rng(20261115)


def floats(a):
    """XGBoost's float32 results as JSON numbers, each the shortest that
    reads back as the same float32."""
    return [float(str(v)) for v in np.asarray(a, dtype=np.float32).ravel()]


def model_sha256(bst):
    """The SHA-256 of the model as XGBoost saves it (UBJSON), which Igloo's
    must equal byte for byte."""
    return hashlib.sha256(bytes(bst.save_raw("ubj"))).hexdigest()


def history(evals_result):
    return {
        data: {m: [float(v) for v in vals] for m, vals in metrics.items()}
        for data, metrics in evals_result.items()
    }


# Binary classification, a weighted regression and early stopping, on one
# table: four numbers (some missing), a boolean, and the rows to validate on.
n = 2000
x = rng.normal(size=(n, 4))
x[rng.random(size=(n, 4)) < 0.05] = np.nan
flag = rng.random(n) < 0.4
logit = (
    1.5 * np.nan_to_num(x[:, 0])
    - np.nan_to_num(x[:, 1]) ** 2
    + 0.8 * flag
    + 0.5 * np.nan_to_num(x[:, 2]) * np.nan_to_num(x[:, 3])
)
y = (rng.random(n) < 1 / (1 + np.exp(-logit))).astype(int)
value = (
    3 * np.nan_to_num(x[:, 0])
    + 2 * flag
    - np.abs(np.nan_to_num(x[:, 3]))
    + rng.normal(scale=0.5, size=n)
)
weight = np.round(rng.uniform(0.5, 3.0, size=n), 3)
is_validation = rng.random(n) < 0.25
binary = pd.DataFrame(
    {
        "id": np.arange(1, n + 1),
        "x1": x[:, 0],
        "x2": x[:, 1],
        "x3": x[:, 2],
        "x4": x[:, 3],
        "flag": flag,
        "w": weight,
        "value": value,
        "y": y,
        "is_validation": is_validation,
    }
)
binary.to_csv(here / "binary.csv", index=False)

features = ["x1", "x2", "x3", "x4", "flag"]
X = binary[features].astype(float).to_numpy()
train, valid = ~is_validation, is_validation
dtrain = xgb.DMatrix(X[train], label=y[train], feature_names=features)
dvalid = xgb.DMatrix(X[valid], label=y[valid], feature_names=features)
dall = xgb.DMatrix(X, feature_names=features)

# Where the reference was made: there, Igloo's models are these byte for
# byte; elsewhere compilers round some floats otherwise.
ref = {"xgboost": xgb.__version__, "platform": sys.platform + "/" + platform.machine()}

# No subsample or colsample_*: XGBoost draws them with the C++ standard
# library's distributions, whose draws differ between libc++ (macOS) and
# libstdc++ (Linux), so they train other trees on each.
params = {
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "auc"],
    "max_depth": 4,
    "eta": 0.1,
    "lambda": 1.0,
    "min_child_weight": 1,
    "seed": 7,
    "tree_method": "hist",
}
evals_result = {}
bst = xgb.train(
    params,
    dtrain,
    num_boost_round=50,
    evals=[(dtrain, "train"), (dvalid, "validation")],
    evals_result=evals_result,
    verbose_eval=False,
)
contribs = bst.predict(dall, pred_contribs=True)
ref["binary"] = {
    "params": params,
    "num_boost_round": 50,
    "model_sha256": model_sha256(bst),
    "history": history(evals_result),
    "validation_auc": evals_result["validation"]["auc"][-1],
    "validation_logloss": evals_result["validation"]["logloss"][-1],
    "predictions": floats(bst.predict(dall)),
    "margins": floats(bst.predict(dall, output_margin=True)),
    # The first 500 rows' SHAP values and bias.
    "contributions": floats(contribs[:500]),
}

# The same rows, with early stopping on the validation rows: the last
# metric, log loss, decides.
params_es = {
    "objective": "binary:logistic",
    "eval_metric": ["auc", "logloss"],
    "max_depth": 6,
    "eta": 0.3,
    "seed": 7,
}
evals_result = {}
bst = xgb.train(
    params_es,
    dtrain,
    num_boost_round=500,
    evals=[(dtrain, "train"), (dvalid, "validation")],
    early_stopping_rounds=10,
    evals_result=evals_result,
    verbose_eval=False,
)
best = bst.best_iteration
ref["early_stopping"] = {
    "params": params_es,
    "num_boost_round": 500,
    "early_stopping_rounds": 10,
    "model_sha256": model_sha256(bst),
    "best_iteration": best,
    "best_score": bst.best_score,
    "rounds": bst.num_boosted_rounds(),
    "history": history(evals_result),
    "predictions": floats(bst.predict(dall, iteration_range=(0, best + 1))),
}

# A weighted regression on every row.
params_reg = {
    "objective": "reg:squarederror",
    "max_depth": 3,
    "learning_rate": 0.2,
    "seed": 3,
}
dreg = xgb.DMatrix(X, label=value, weight=weight, feature_names=features)
evals_result = {}
bst = xgb.train(
    params_reg,
    dreg,
    num_boost_round=30,
    evals=[(dreg, "train")],
    evals_result=evals_result,
    verbose_eval=False,
)
ref["weighted_regression"] = {
    "params": params_reg,
    "num_boost_round": 30,
    "model_sha256": model_sha256(bst),
    "history": history(evals_result),
    "predictions": floats(bst.predict(dall)),
}

# Accelerated failure time: times of which a third are right-censored (an
# upper bound of +Infinity).
m = 1500
z = rng.normal(size=(m, 3))
z[rng.random(size=(m, 3)) < 0.05] = np.nan
log_time = (
    1.0
    + 0.7 * np.nan_to_num(z[:, 0])
    - 0.4 * np.nan_to_num(z[:, 1])
    + rng.normal(scale=0.5, size=m)
)
time = np.round(np.exp(log_time), 4)
censored = rng.random(m) < 0.33
lower = np.where(censored, np.round(time * rng.uniform(0.3, 1.0, size=m), 4), time)
upper = np.where(censored, np.inf, time)
survival = pd.DataFrame(
    {
        "z1": z[:, 0],
        "z2": z[:, 1],
        "z3": z[:, 2],
        "time_lower": lower,
        "time_upper": upper,
    }
)
survival.to_csv(here / "survival.csv", index=False)

Z = survival[["z1", "z2", "z3"]].to_numpy()
daft = xgb.DMatrix(Z, feature_names=["z1", "z2", "z3"])
daft.set_float_info("label_lower_bound", lower)
daft.set_float_info("label_upper_bound", upper)
params_aft = {
    "objective": "survival:aft",
    "eval_metric": "aft-nloglik",
    "aft_loss_distribution": "normal",
    "aft_loss_distribution_scale": 1.0,
    "tree_method": "hist",
    "learning_rate": 0.05,
    "max_depth": 3,
    "lambda": 0.01,
    "alpha": 0.02,
}
evals_result = {}
bst = xgb.train(
    params_aft,
    daft,
    num_boost_round=100,
    evals=[(daft, "train")],
    evals_result=evals_result,
    verbose_eval=False,
)
ref["aft"] = {
    "params": params_aft,
    "num_boost_round": 100,
    "model_sha256": model_sha256(bst),
    "history": history(evals_result),
    "final_aft_nloglik": evals_result["train"]["aft-nloglik"][-1],
    "predictions": floats(bst.predict(daft)),
}

(here / "reference.json").write_text(json.dumps(ref, separators=(",", ":")) + "\n")
print("xgboost", xgb.__version__, "- wrote binary.csv, survival.csv, reference.json")
