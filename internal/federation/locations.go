package federation

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// Where the tables of Iceberg REST catalogs may keep their files. Their
// locations go into DuckDB's sandbox (allowed_directories), which then lets
// SQL read under them, so each is checked first: an object store's URL, with
// no way to climb out of it, below a bucket, and apart from the warehouse
// and the allowed locations, which Igloo reads with its own credentials.

// storageSchemes are the URL schemes of object stores DuckDB reads, by the
// store they reach.
var storageSchemes = map[string]string{
	"s3": "s3", "s3a": "s3", "s3n": "s3", "r2": "r2", "gs": "gs", "gcs": "gs",
	"az": "az", "azure": "az", "abfs": "az", "abfss": "az",
}

// locationRules decides which locations a catalog's tables may be in.
type locationRules struct {
	// reserved are Igloo's own locations, in canonical form.
	reserved []string
	// local admits directories of the server, for tests.
	local bool
}

func newLocationRules(reserved []string, local bool) locationRules {
	l := locationRules{local: local}
	for _, r := range reserved {
		if r != "" {
			l.reserved = append(l.reserved, canonicalLocation(r))
		}
	}
	return l
}

// canonicalLocation is the form of a location two spellings of it share,
// for telling whether two locations overlap: lower case, with a trailing
// slash, s3a and s3n as s3, gcs as gs, and Azure's URLs as az://container/
// whatever the account, which is the cautious reading.
func canonicalLocation(loc string) string {
	l := strings.ToLower(strings.TrimSuffix(loc, "/")) + "/"
	scheme, rest, ok := strings.Cut(l, "://")
	if !ok {
		return "file://" + l
	}
	if family, ok := storageSchemes[scheme]; ok {
		scheme = family
	}
	if scheme == "az" {
		auth, path, _ := strings.Cut(rest, "/")
		if container, _, ok := strings.Cut(auth, "@"); ok {
			auth = container
		}
		rest = auth + "/" + path
	}
	return scheme + "://" + rest
}

// overlaps reports whether two canonical locations share any file.
func overlaps(a, b string) bool { return strings.HasPrefix(a, b) || strings.HasPrefix(b, a) }

// check validates a location a table's files may be in and returns it
// ending in a slash, as DuckDB's sandbox takes it. bucketOnly admits a
// bare bucket, for the prefixes storage credentials are scoped to.
func (l locationRules) check(loc string, bucketOnly bool) (string, error) {
	if loc == "" {
		return "", errors.New("no location")
	}
	if strings.ContainsAny(loc, "?#%\\") || strings.ContainsFunc(loc, unicode.IsControl) {
		return "", fmt.Errorf("%q is not a plain storage location", loc)
	}
	scheme, rest, ok := strings.Cut(loc, "://")
	local := false
	switch {
	case !ok && l.local && strings.HasPrefix(loc, "/"):
		rest, local = loc, true
	case ok && strings.EqualFold(scheme, "file") && l.local:
		local = true
	case !ok:
		return "", fmt.Errorf("%q is not an object store's location", loc)
	case storageSchemes[strings.ToLower(scheme)] == "":
		return "", fmt.Errorf("%q is not in an object store Igloo reads tables from (s3, gs, az, abfss, r2)", loc)
	}
	path := strings.TrimPrefix(rest, "/")
	if !local {
		var bucket string
		bucket, path, _ = strings.Cut(rest, "/")
		if bucket == "" {
			return "", fmt.Errorf("%q names no bucket", loc)
		}
		// DuckDB makes the bucket part of the host it connects to, and an
		// abfss URL names the host itself: no other characters, and no
		// host but Azure's, so a location cannot send reads elsewhere.
		family := storageSchemes[strings.ToLower(scheme)]
		host := ""
		if family == "az" {
			if container, h, ok := strings.Cut(bucket, "@"); ok {
				bucket, host = container, strings.ToLower(h)
				if !strings.HasSuffix(host, ".dfs.core.windows.net") && !strings.HasSuffix(host, ".blob.core.windows.net") {
					return "", fmt.Errorf("%q is not on Azure's storage", loc)
				}
				if !validHost(host) {
					return "", fmt.Errorf("%q has no valid host", loc)
				}
			}
		}
		if strings.ContainsFunc(bucket, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_')
		}) {
			return "", fmt.Errorf("%q has no valid bucket name", loc)
		}
	}
	segs := strings.Split(strings.TrimSuffix(path, "/"), "/")
	for _, s := range segs {
		if s == "." || s == ".." || s == "" && len(segs) > 1 {
			return "", fmt.Errorf("%q has '.', '..' or empty path segments", loc)
		}
	}
	if !bucketOnly && segs[0] == "" && !s3TablesBucket(rest) {
		// A table at a bucket's root would open the whole bucket.
		return "", fmt.Errorf("%q is a whole bucket, not a table's location", loc)
	}
	norm := strings.TrimSuffix(loc, "/") + "/"
	c := canonicalLocation(norm)
	for _, r := range l.reserved {
		if overlaps(c, r) {
			return "", fmt.Errorf("%s overlaps Igloo's warehouse or allowed locations (%s), which Igloo reads with its own credentials", norm, r)
		}
	}
	return norm, nil
}

// s3TablesBucket reports whether a location's bucket is one S3 Tables made
// for a single table, which it names ending in --table-s3: the table's
// location is the whole bucket.
func s3TablesBucket(rest string) bool {
	bucket, _, _ := strings.Cut(rest, "/")
	return strings.HasSuffix(bucket, "--table-s3")
}

// checkPrefix validates the prefix storage credentials are handed out for.
func (l locationRules) checkPrefix(prefix string) error {
	_, err := l.check(prefix, true)
	return err
}

// rootProperties are the table properties that place a table's files
// outside its location: DuckDB reads them and scopes the table's storage
// credentials to write.data.path.
var rootProperties = []string{"write.data.path", "write.metadata.path", "write.object-storage.path", "write.folder-storage.path"}

// tableRoots returns the locations a table's files may be in: its location,
// and those of rootProperties outside it.
func (l locationRules) tableRoots(location string, props map[string]string) ([]string, error) {
	root, err := l.check(location, false)
	if err != nil {
		return nil, err
	}
	roots := []string{root}
	for _, p := range rootProperties {
		v := props[p]
		if v == "" {
			continue
		}
		r, err := l.check(v, false)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if !under(r, roots) {
			roots = append(roots, r)
		}
	}
	slices.Sort(roots[1:])
	return roots, nil
}
