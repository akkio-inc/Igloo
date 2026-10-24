package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/akkio-inc/igloo/internal/config"
)

// The rb extension (extensions/rb) counts distinct integer IDs exactly in
// roaring bitmaps. It is Igloo's own, so DuckDB cannot check a signature on
// it: the image carries the file Igloo's build wrote, and the binary the
// file's SHA-256 from that same build. Igloo checks the file against it,
// loads it before DuckDB's lockdown, and turns unsigned loading off again;
// once external access is off, nothing can load another file.

// rbFile is where the image keeps the extension, under the extension
// directory: outside DuckDB's own versioned tree, so INSTALL and autoload
// never see it.
const rbFile = "igloo/rb.duckdb_extension"

// rbSHA256 is the SHA-256 of the image's rb extension, set by the image
// build (-ldflags -X) from the build that wrote the file. Without it Igloo
// loads the extension only from IGLOO_TEST_RB_EXTENSION, which builds made
// with the hash (every image) ignore.
var rbSHA256 string

// rbTestEnv names an rb extension for builds without rbSHA256, as Go
// tests are: it is loaded without a hash check.
const rbTestEnv = "IGLOO_TEST_RB_EXTENSION"

// rbExtension is an rb extension file to load, and the hash it must have
// ("" when it is a test build's unchecked file).
type rbExtension struct {
	path   string
	sha256 string
}

// rbToLoad returns the rb extension this process loads, or nil.
func rbToLoad(cfg config.Config) *rbExtension {
	if rbSHA256 != "" {
		if cfg.DuckDB.ExtensionDir == "" {
			return nil
		}
		return &rbExtension{path: filepath.Join(cfg.DuckDB.ExtensionDir, rbFile), sha256: rbSHA256}
	}
	if p := os.Getenv(rbTestEnv); p != "" {
		return &rbExtension{path: p}
	}
	return nil
}

// rbDSN adds to a DuckDB path the setting that lets it load an unsigned
// extension, when it will load rb.
func rbDSN(path string, ext *rbExtension) string {
	if ext == nil {
		return path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "allow_unsigned_extensions=true"
}

// extensionFooter is what a DuckDB extension's last 512 bytes say of it.
type extensionFooter struct {
	platform, duckdbVersion, abi string
}

// readFooter reads the metadata DuckDB appends to an extension: eight
// 32-byte fields, last first, then 256 bytes of signature.
func readFooter(data []byte) (extensionFooter, error) {
	if len(data) < 512 {
		return extensionFooter{}, errors.New("not a DuckDB extension: shorter than its 512-byte footer")
	}
	meta := data[len(data)-512 : len(data)-256]
	field := func(i int) string { // i counts from the first field, which is stored last
		b := meta[(7-i)*32 : (8-i)*32]
		return string(bytes.TrimRight(b, "\x00"))
	}
	if field(0) != "4" {
		return extensionFooter{}, errors.New("not a DuckDB extension: no metadata footer")
	}
	return extensionFooter{platform: field(1), duckdbVersion: field(2), abi: field(4)}, nil
}

// checkRB reads ext and checks it: its hash, when it has one to match, and
// that it was built for this DuckDB and platform.
func checkRB(ext *rbExtension, duckdbVersion, platform string) error {
	data, err := os.ReadFile(ext.path)
	if err != nil {
		return err
	}
	if ext.sha256 != "" {
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != ext.sha256 {
			return fmt.Errorf("%s has SHA-256 %s, not the %s this igloo was built with", ext.path, got, ext.sha256)
		}
	}
	f, err := readFooter(data)
	if err != nil {
		return fmt.Errorf("%s: %w", ext.path, err)
	}
	if f.duckdbVersion != duckdbVersion || f.platform != platform {
		return fmt.Errorf("%s was built for DuckDB %s on %s, not this igloo's DuckDB %s on %s",
			ext.path, f.duckdbVersion, f.platform, duckdbVersion, platform)
	}
	return nil
}

// loadRB loads the rb extension, if this process has one (see rbToLoad),
// before lockdown, and reports whether it did. A file that fails its checks
// stops Igloo from starting.
func loadRB(cfg config.Config, log *slog.Logger, exec func(string) error, query func(string) (string, error)) (bool, error) {
	ext := rbToLoad(cfg)
	if ext == nil {
		return false, nil
	}
	version, err := query("SELECT library_version FROM pragma_version()")
	if err != nil {
		return false, fmt.Errorf("read DuckDB's version: %w", err)
	}
	platform, err := query("SELECT platform FROM pragma_platform()")
	if err != nil {
		return false, fmt.Errorf("read DuckDB's platform: %w", err)
	}
	if err := checkRB(ext, version, platform); err != nil {
		return false, fmt.Errorf("rb extension: %w", err)
	}
	if ext.sha256 == "" && log != nil {
		log.Warn("loading the rb extension unchecked (a build without its hash)", "path", ext.path)
	}
	if err := exec("LOAD " + literal(ext.path)); err != nil {
		return false, fmt.Errorf("load the rb extension: %w", err)
	}
	if err := exec("SET allow_unsigned_extensions = false"); err != nil {
		return false, fmt.Errorf("turn unsigned extensions off: %w", err)
	}
	return true, nil
}

// BitmapsLoaded reports whether DuckDB has the rb extension's bitmap
// functions (rb_agg, rb_count_distinct and the rest).
func (e *Engine) BitmapsLoaded() bool {
	return e.bitmaps
}

// CheckExtensions opens DuckDB as the server does, with the extensions in
// dir, and checks that the rb extension loads and counts. The image build
// and CI run it against the very binary and files they ship.
func CheckExtensions(ctx context.Context, dir string) (string, error) {
	if rbSHA256 == "" {
		return "", errors.New("this igloo was built without the rb extension's hash")
	}
	e, err := Open(ctx, config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return "", err
	}
	defer e.Close() //nolint:errcheck
	if !e.BitmapsLoaded() {
		return "", fmt.Errorf("the rb extension did not load from %s", dir)
	}
	conn, err := e.Connect(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck
	got, err := queryString(ctx, conn, "SELECT rb_count_distinct(x::UINTEGER)::VARCHAR FROM range(100000) t(x)")
	if err != nil {
		return "", fmt.Errorf("count with the rb extension: %w", err)
	}
	if got != "100000" {
		return "", fmt.Errorf("the rb extension counted %s distinct values of 100000", got)
	}
	return fmt.Sprintf("rb extension %s loads and counts", rbSHA256), nil
}
