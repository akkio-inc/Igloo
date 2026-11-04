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

// Igloo builds two of DuckDB's extensions itself, with the image, rather
// than download them: rb (extensions/rb), which counts distinct integer IDs
// exactly in roaring bitmaps, and iceberg (extensions/iceberg), DuckDB's
// Iceberg extension with Igloo's patches. DuckDB cannot check a signature
// on either: the image carries the files Igloo's build wrote, and the
// binary their SHA-256s from that same build. Igloo checks each file
// against its hash, loads it before DuckDB's lockdown, and turns unsigned
// loading off again; once external access is off, nothing can load another
// file. The iceberg extension is loaded before anything asks DuckDB for
// iceberg by name, which then finds it loaded.

// ownExtension is one of Igloo's own extensions.
type ownExtension struct {
	// name is the extension's name, as LOAD and duckdb_extensions() have it.
	name string
	// file is where the image keeps it, under the extension directory:
	// outside DuckDB's own versioned tree, so INSTALL and autoload never
	// see it.
	file string
	// sha256 is the SHA-256 of the image's file, set by the image build
	// (-ldflags -X) from the build that wrote the file. Without it Igloo
	// loads the extension only from testEnv, which builds made with the
	// hash (every image) ignore.
	sha256 *string
	// testEnv names a file for builds without the hash, as Go tests are: it
	// is loaded without a hash check.
	testEnv string
	// pack says it loads extensions of DuckDB's from the extension
	// directory as it loads, as iceberg loads avro, so it loads only in a
	// process that has one.
	pack bool
}

var (
	rbSHA256      string
	icebergSHA256 string
)

const (
	rbFile      = "igloo/rb.duckdb_extension"
	rbTestEnv   = "IGLOO_TEST_RB_EXTENSION"
	icebergFile = "igloo/iceberg.duckdb_extension"
)

var ownExtensions = []ownExtension{
	{name: "rb", file: rbFile, sha256: &rbSHA256, testEnv: rbTestEnv},
	{name: "iceberg", file: icebergFile, sha256: &icebergSHA256, testEnv: "IGLOO_TEST_ICEBERG_EXTENSION", pack: true},
}

// extensionFile is an extension file to load, and the hash it must have
// ("" when it is a test build's unchecked file).
type extensionFile struct {
	name   string
	path   string
	sha256 string
}

// ownToLoad returns the extensions of Igloo's own this process loads.
func ownToLoad(cfg config.Config) []extensionFile {
	var files []extensionFile
	for _, x := range ownExtensions {
		if *x.sha256 != "" {
			if cfg.DuckDB.ExtensionDir != "" {
				files = append(files, extensionFile{name: x.name, path: filepath.Join(cfg.DuckDB.ExtensionDir, x.file), sha256: *x.sha256})
			}
		} else if p := os.Getenv(x.testEnv); p != "" && (!x.pack || cfg.DuckDB.ExtensionDir != "") {
			files = append(files, extensionFile{name: x.name, path: p})
		}
	}
	return files
}

// unsignedDSN adds to a DuckDB path the setting that lets it load an
// unsigned extension, when it will load one of files.
func unsignedDSN(path string, files []extensionFile) string {
	if len(files) == 0 {
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

// checkFile reads f and checks it: its hash, when it has one to match, and
// that it was built for this DuckDB and platform.
func checkFile(f extensionFile, duckdbVersion, platform string) error {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return err
	}
	if f.sha256 != "" {
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != f.sha256 {
			return fmt.Errorf("%s has SHA-256 %s, not the %s this igloo was built with", f.path, got, f.sha256)
		}
	}
	footer, err := readFooter(data)
	if err != nil {
		return fmt.Errorf("%s: %w", f.path, err)
	}
	if footer.duckdbVersion != duckdbVersion || footer.platform != platform {
		return fmt.Errorf("%s was built for DuckDB %s on %s, not this igloo's DuckDB %s on %s",
			f.path, footer.duckdbVersion, footer.platform, duckdbVersion, platform)
	}
	return nil
}

// loadOwn loads Igloo's own extensions this process has (see ownToLoad)
// before lockdown, and reports which it loaded, by name. A file that fails
// its checks stops Igloo from starting.
func loadOwn(cfg config.Config, log *slog.Logger, exec func(string) error, query func(string) (string, error)) (map[string]bool, error) {
	files := ownToLoad(cfg)
	if len(files) == 0 {
		return nil, nil
	}
	version, err := query("SELECT library_version FROM pragma_version()")
	if err != nil {
		return nil, fmt.Errorf("read DuckDB's version: %w", err)
	}
	platform, err := query("SELECT platform FROM pragma_platform()")
	if err != nil {
		return nil, fmt.Errorf("read DuckDB's platform: %w", err)
	}
	loaded := map[string]bool{}
	for _, f := range files {
		if err := checkFile(f, version, platform); err != nil {
			return nil, fmt.Errorf("%s extension: %w", f.name, err)
		}
		if f.sha256 == "" && log != nil {
			log.Warn("loading an extension unchecked (a build without its hash)", "extension", f.name, "path", f.path)
		}
		if err := exec("LOAD " + literal(f.path)); err != nil {
			return nil, fmt.Errorf("load the %s extension: %w", f.name, err)
		}
		loaded[f.name] = true
	}
	if err := exec("SET allow_unsigned_extensions = false"); err != nil {
		return nil, fmt.Errorf("turn unsigned extensions off: %w", err)
	}
	return loaded, nil
}

// BitmapsLoaded reports whether DuckDB has the rb extension's bitmap
// functions (rb_agg, rb_count_distinct and the rest).
func (e *Engine) BitmapsLoaded() bool {
	return e.bitmaps
}

// CheckExtensions opens DuckDB as the server does, with the extensions in
// dir, and checks that Igloo's own extensions load: that rb counts, and
// that the iceberg extension loads. The image build
// and CI run it against the very binary and files they ship.
func CheckExtensions(ctx context.Context, dir string) (string, error) {
	for _, x := range ownExtensions {
		if *x.sha256 == "" {
			return "", fmt.Errorf("this igloo was built without the %s extension's hash", x.name)
		}
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
	if !e.icebergOwn {
		return "", fmt.Errorf("the iceberg extension did not load from %s", dir)
	}
	return fmt.Sprintf("rb extension %s loads and counts; iceberg extension %s loads", rbSHA256, icebergSHA256), nil
}
