package erasure

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

const (
	// maxIdentifiers bounds the identifiers of one request.
	maxIdentifiers = 1000
	// maxIdentifierLength bounds one identifier, in bytes.
	maxIdentifierLength = 1024
	// boundLength is how many characters of a string Iceberg keeps in
	// manifest bounds by default (write.metadata.metrics.default is
	// truncate(16)).
	boundLength = 16
)

// digests are the algorithms whose hex digests of identifiers a column may
// hold, named after a colon in its tag value: pii = 'email:sha256'. Weak
// ones are here because ad platforms match on them, not for security.
var digests = map[string]func([]byte) []byte{
	"sha256": func(b []byte) []byte { s := sha256.Sum256(b); return s[:] },
	"sha1":   func(b []byte) []byte { s := sha1.Sum(b); return s[:] },
	"md5":    func(b []byte) []byte { s := md5.Sum(b); return s[:] },
}

// normalize is how identifiers and column values compare: lower-cased,
// without surrounding spaces. DuckDB's lower(trim(...)) does the same.
func normalize(s string) string { return strings.ToLower(strings.Trim(s, " ")) }

// salted is the hash a request keeps of s: SHA-256 of the salt then s, in
// hex, as DuckDB's sha256(salt || s) computes it.
func salted(salt, s string) string {
	sum := sha256.Sum256([]byte(salt + s))
	return hex.EncodeToString(sum[:])
}

// newSubject computes the hashes of the identifiers ids of tag.value.
// Hashed columns may hold digests of an identifier as it was given or as it
// compares, so both count.
func newSubject(salt, tag, value string, ids []string) Subject {
	plain, prefixes := map[string]bool{}, map[string]bool{}
	byDigest := map[string]map[string]bool{}
	add := func(set map[string]bool, s string) {
		set[salted(salt, s)] = true
		for _, t := range truncations(s) {
			prefixes[salted(salt, t)] = true
		}
	}
	for _, id := range ids {
		n := normalize(id)
		add(plain, n)
		for name, digest := range digests {
			if byDigest[name] == nil {
				byDigest[name] = map[string]bool{}
			}
			for _, form := range []string{id, n} {
				add(byDigest[name], hex.EncodeToString(digest([]byte(form))))
			}
		}
	}
	sub := Subject{Tag: tag, Value: value, Count: len(ids), Plain: sorted(plain), Prefixes: sorted(prefixes),
		Digests: map[string][]string{}}
	for name, set := range byDigest {
		sub.Digests[name] = sorted(set)
	}
	return sub
}

func sorted(set map[string]bool) []string { return slices.Sorted(maps.Keys(set)) }

// hashes are the salted hashes a column tagged with s's value and digest
// holds.
func (s Subject) hashes(digest string) []string {
	if digest == "" {
		return s.Plain
	}
	return s.Digests[digest]
}

// truncations are what Iceberg writes as a column's lower and upper bounds
// when s, longer than boundLength characters, is its smallest or largest
// value: s cut to boundLength characters, and that with its last character
// incremented.
func truncations(s string) []string {
	if utf8.RuneCountInString(s) <= boundLength {
		return nil
	}
	r := []rune(s)[:boundLength]
	lower := string(r)
	r[boundLength-1]++
	if !utf8.ValidRune(r[boundLength-1]) {
		return []string{lower}
	}
	return []string{lower, string(r)}
}

// matcher answers whether values are a request's identifiers.
type matcher struct {
	salt string
	// exact are the hashes of identifiers and their digests; prefixes of
	// their truncations in bounds.
	exact, prefixes map[string]bool
}

func newMatcher(r *Request) *matcher {
	m := &matcher{salt: r.Salt, exact: map[string]bool{}, prefixes: map[string]bool{}}
	for _, s := range r.Subjects {
		for _, h := range s.Plain {
			m.exact[h] = true
		}
		for _, hs := range s.Digests {
			for _, h := range hs {
				m.exact[h] = true
			}
		}
		for _, h := range s.Prefixes {
			m.prefixes[h] = true
		}
	}
	return m
}

func (m *matcher) matches(s string) bool { return m.exact[salted(m.salt, normalize(s))] }

// matchesBound reports whether a manifest bound may hold an identifier: it
// is one, or what one is truncated to.
func (m *matcher) matchesBound(s string) bool {
	h := salted(m.salt, normalize(s))
	return m.exact[h] || m.prefixes[h]
}

// delimiters split history text into the words that identifiers are
// compared with, in syntax that Go and PostgreSQL both read.
const delimiters = "[\\s\"'`,;%(){}\\[\\]<>|:=&?#$\\\\]+"

var word = regexp.MustCompile("[^\\s\"'`,;%(){}\\[\\]<>|:=&?#$\\\\]+")

// Prefilter implements audit.Redactor.
func (m *matcher) Prefilter() (string, string, [][]byte) {
	hashes := make([][]byte, 0, len(m.exact))
	for _, h := range sorted(m.exact) {
		b, _ := hex.DecodeString(h)
		hashes = append(hashes, b)
	}
	return delimiters, m.salt, hashes
}

// RedactSQL implements audit.Redactor: a string literal that is an
// identifier, or has one among its words, becomes '***'; elsewhere, such
// as in numbers, quoted names and comments, the word itself becomes ***.
func (m *matcher) RedactSQL(text string) (string, bool) {
	var b strings.Builder
	changed := false
	for _, seg := range sqllex.Segments(text) {
		if seg.Kind == sqllex.String {
			if body := literalBody(seg.Text); m.matches(body) || m.hasWord(body) {
				b.WriteString("'***'")
				changed = true
				continue
			}
			b.WriteString(seg.Text)
			continue
		}
		out, ok := m.RedactText(seg.Text)
		changed = changed || ok
		b.WriteString(out)
	}
	return b.String(), changed
}

// RedactText implements audit.Redactor: each word that is an identifier
// becomes ***.
func (m *matcher) RedactText(text string) (string, bool) {
	changed := false
	out := word.ReplaceAllStringFunc(text, func(w string) string {
		if m.matches(w) {
			changed = true
			return "***"
		}
		return w
	})
	return out, changed
}

func (m *matcher) hasWord(s string) bool {
	return slices.ContainsFunc(word.FindAllString(s, -1), m.matches)
}

// literalBody is the text of a string literal: '...', with quotes in it
// doubled, E'...', whose escapes are left as written, or $tag$...$tag$.
func literalBody(lit string) string {
	if strings.HasPrefix(lit, "$") {
		if end := strings.Index(lit[1:], "$"); end >= 0 {
			tag := lit[:end+2]
			return strings.TrimSuffix(strings.TrimPrefix(lit, tag), tag)
		}
		return lit
	}
	lit = strings.TrimPrefix(lit, "'")
	lit = strings.TrimSuffix(lit, "'")
	return strings.ReplaceAll(lit, "''", "'")
}
