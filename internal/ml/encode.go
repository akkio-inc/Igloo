package ml

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
)

// maxBlob bounds a model blob, compressed and not.
const maxBlob = 1 << 30

// Encode serializes v as a model blob, gzipped JSON, and returns it with
// its SHA-256 in hex. The encoding is deterministic: equal values give
// equal bytes and so the same hash. Floats must be finite.
func Encode(v any) ([]byte, string, error) {
	if err := checkFinite(reflect.ValueOf(v)); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, "", err
	}
	if _, err := zw.Write(raw); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:]), nil
}

// Hash returns the SHA-256 of a blob in hex.
func Hash(blob []byte) string {
	sum := sha256.Sum256(blob)
	return hex.EncodeToString(sum[:])
}

// Decode reads a blob Encode wrote into v, checking it against its hash.
func Decode(blob []byte, sha string, v any) error {
	if sha != "" && Hash(blob) != sha {
		return fmt.Errorf("model blob does not match its SHA-256 %s", sha)
	}
	zr, err := gzip.NewReader(bytes.NewReader(blob))
	if err != nil {
		return fmt.Errorf("read model blob: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, maxBlob))
	if err != nil {
		return fmt.Errorf("read model blob: %w", err)
	}
	return json.Unmarshal(raw, v)
}

// checkFinite refuses NaN and infinite floats, which JSON cannot hold.
func checkFinite(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		if f := v.Float(); math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("model holds a non-finite number")
		}
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			return checkFinite(v.Elem())
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				if err := checkFinite(v.Field(i)); err != nil {
					return fmt.Errorf("%s: %w", v.Type().Field(i).Name, err)
				}
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if err := checkFinite(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		it := v.MapRange()
		for it.Next() {
			if err := checkFinite(it.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}
