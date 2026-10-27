//go:build !arm64 && !amd64

package tsfm

// dot4x4 is the 4×4 kernel of matrix products.
var dot4x4 = dot4x4Go
