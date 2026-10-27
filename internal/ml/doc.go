// Package ml is Igloo's predictive engine: gradient-boosted trees for
// classification, regression and quantiles, and random forests on the same
// trees; a linear and logistic regression whose coefficients explain it;
// SHAP contributions for both; and, for the models that take vectors
// (Encoder), feed-forward neural networks (MLP), k-means, principal
// components and a matrix factorization by alternating least squares
// (ALS). Snowflake's ML classes (FORECAST, ANOMALY_DETECTION,
// CLASSIFICATION, TOP_INSIGHTS) and Igloo's own (IGLOO.ML.*; see
// docs/ml-functions.md) are built on it.
//
// The engine is Go, not LightGBM through cgo or a DuckDB extension. It
// builds wherever Igloo's Go does, on amd64 and arm64 alike, with no C++
// toolchain or OpenMP in the image or in CI, and it is Igloo's own
// Apache-2.0 code. It follows LightGBM's algorithm where that matters for
// speed and accuracy: features binned into at most 255 bins (plus one for
// missing values), leaf-wise growth, histograms built for the smaller child
// and subtracted for the larger, categorical splits on categories sorted by
// their gradients, a learned direction for missing values, and quantile
// leaves set to the quantile of their residuals. On one machine it trains
// on a million rows of 50 features in seconds (see BenchmarkTrain1M).
//
// Models are deterministic: the same rows, in the same order, with the same
// seed give the same trees, whatever the number of threads, because every
// sum is taken in a fixed order (a histogram's bins one feature at a time,
// rows in fixed chunks, a network's batch in fixed parts). Callers put rows
// in a canonical order first (Frame.CanonicalOrder) so that a query
// returning rows in another order changes nothing, and a model's blob
// (Encode) then has the same SHA-256.
package ml
