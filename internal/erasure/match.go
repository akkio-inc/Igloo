package erasure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	// maxIdentifiers bounds the identifiers of one request.
	maxIdentifiers = 1000
	// maxIdentifierLength bounds one identifier, in bytes.
	maxIdentifierLength = 1024
	// boundLength is how many characters of a string, or bytes of a
	// binary value, Iceberg keeps in manifest bounds by default
	// (write.metadata.metrics.default is truncate(16)).
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

// nullSuffix ends a tag value whose columns a request sets to NULL in
// matching rows, rather than deleting the rows: pii = 'email:null'.
const nullSuffix = ":null"

// identifier is one identifier a request erases: as it was given, and as
// DuckDB's lower(trim(...)) leaves it, which is how column values compare.
type identifier struct {
	Given  string `json:"g"`
	Normal string `json:"n"`
}

// sentinels are values that stand for no identifier at all, which systems
// write by the million: an opted-out device's all-zero advertising ID, and
// placeholders. A request for one would erase everyone it stands for.
var sentinels = []string{"null", "nil", "none", "unknown", "undefined", "n/a", "na", "nan", "-", "--", "?",
	"default", "anonymous", "not set", "notset", "unset", "empty", "blank", "test", "true", "false",
	"noemail@noemail.com", "no@email.com", "none@none.com", "test@test.com", "email@email.com", "example@example.com"}

// sentinel reports whether a normalized identifier is such a placeholder,
// whatever brackets, quotes or spaces are around it ({null}, <null>,
// (not set), ' null '), or a number that stands for none: one made of
// zeros (in any script's digits, with separators, a 0x prefix or an
// exponent: 0, 0x0, 0e0, {00000000-0000-...}), -1, or the all-F ID.
func sentinel(n string) bool {
	n = strings.TrimFunc(n, func(r rune) bool { return unicode.IsSpace(r) || strings.ContainsRune(`{}[]()<>"'`+"`", r) })
	n = strings.TrimFunc(n, unicode.IsSpace)
	if slices.Contains(sentinels, n) || n == "" {
		return true
	}
	var digits strings.Builder
	for _, r := range n {
		switch {
		case unicode.IsDigit(r):
			if v, ok := digitValue(r); ok {
				r = '0' + v
			}
			digits.WriteRune(r)
		case strings.ContainsRune("-:._ {}+", r) || unicode.IsSpace(r):
		default:
			digits.WriteRune(r)
		}
	}
	d := strings.TrimPrefix(digits.String(), "0x")
	switch {
	case strings.Trim(d, "0") == "", strings.Contains(d, "0") && strings.Trim(strings.Replace(d, "e", "", 1), "0") == "":
		return true
	case n == "-1", n == "-01":
		return true
	case len(d) >= 8 && strings.Trim(d, "f") == "":
		return true
	}
	return false
}

// digitValue is a decimal digit's value, in the scripts whose digits are
// most used.
func digitValue(r rune) (rune, bool) {
	for _, zero := range []rune{'0', '٠', '۰', '߀', '०', '০', '੦', '૦', '୦', '௦', '౦', '೦', '൦', '෦', '๐', '໐', '༠', '၀', '႐', '០', '᠐', '０'} {
		if r >= zero && r <= zero+9 {
			return r - zero, true
		}
	}
	return 0, false
}

// sentinelDigest reports whether a normalized identifier is the hex digest
// of a placeholder: hashed tables hold the SHA-256 of the empty string for
// every row without an email, and digests of "null" and the like as often.
func sentinelDigest(n string) bool {
	return placeholderDigests[n]
}

// placeholderDigests are the hex digests, by every algorithm in digests,
// of the empty string and the placeholders, as they are and upper-cased.
var placeholderDigests = func() map[string]bool {
	out := map[string]bool{}
	for _, p := range append([]string{"", " ", "0", "-1"}, sentinels...) {
		for _, form := range []string{p, strings.ToUpper(p)} {
			for _, digest := range digests {
				out[hex.EncodeToString(digest([]byte(form)))] = true
			}
		}
	}
	return out
}()

// sealer seals and unseals requests' identifiers, and derives their
// hashes' salts, under the erasure key.
type sealer struct{ seal, salt []byte }

func newSealer(key []byte) *sealer {
	derive := func(purpose string) []byte {
		m := hmac.New(sha256.New, key)
		m.Write([]byte("igloo erasure " + purpose))
		return m.Sum(nil)
	}
	return &sealer{seal: derive("seal"), salt: derive("salt")}
}

// hashSalt is the salt of r's hashes: without the key, Salt says nothing.
func (k *sealer) hashSalt(salt string) string {
	m := hmac.New(sha256.New, k.salt)
	m.Write([]byte(salt))
	return hex.EncodeToString(m.Sum(nil))
}

// sealIDs encrypts identifiers, by subject, for request id.
func (k *sealer) sealIDs(id string, ids [][]identifier) (string, error) {
	plain, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}
	gcm, err := k.gcm()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, plain, []byte(id))), nil
}

// errKey means a request's identifiers cannot be unsealed: the erasure key
// is not the one they were sealed under.
var errKey = errors.New("this replica cannot unseal the request's identifiers: its erasure key, IGLOO_ERASURE_KEY, " +
	"is not the key they were sealed under; set that key on every replica, and the request goes on")

func (k *sealer) unsealIDs(id, sealed string) ([][]identifier, error) {
	b, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return nil, errKey
	}
	gcm, err := k.gcm()
	if err != nil {
		return nil, err
	}
	if len(b) < gcm.NonceSize() {
		return nil, errKey
	}
	plain, err := gcm.Open(nil, b[:gcm.NonceSize()], b[gcm.NonceSize():], []byte(id))
	if err != nil {
		return nil, errKey
	}
	var ids [][]identifier
	return ids, json.Unmarshal(plain, &ids)
}

func (k *sealer) gcm() (cipher.AEAD, error) {
	block, err := aes.NewCipher(k.seal)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// salted is a hash a request computes of s: SHA-256 of the salt then s, in
// hex, as DuckDB's sha256(salt || s) computes it.
func salted(salt, s string) string {
	sum := sha256.Sum256([]byte(salt + s))
	return hex.EncodeToString(sum[:])
}

// hash computes sub's hashes of its identifiers with salt. Hashed columns
// may hold digests of an identifier as it was given or as it compares, so
// both count.
func (sub *Subject) hash(salt string) {
	plain, prefixes := map[string]bool{}, map[string]bool{}
	byDigest := map[string]map[string]bool{}
	add := func(set map[string]bool, s string) {
		set[salted(salt, s)] = true
		for _, t := range truncations(s) {
			prefixes[salted(salt, t)] = true
		}
	}
	for _, id := range sub.ids {
		for _, form := range id.forms() {
			add(plain, form)
		}
		for name, digest := range digests {
			if byDigest[name] == nil {
				byDigest[name] = map[string]bool{}
			}
			for _, form := range []string{id.Given, id.Normal} {
				add(byDigest[name], hex.EncodeToString(digest([]byte(form))))
			}
		}
	}
	sub.plain, sub.prefixes = sorted(plain), sorted(prefixes)
	sub.digests = map[string][]string{}
	for name, set := range byDigest {
		sub.digests[name] = sorted(set)
	}
}

func sorted(set map[string]bool) []string { return slices.Sorted(maps.Keys(set)) }

// forms are what column values an identifier compares as: as it is, and,
// for a UUID, as one in a binary column, whose values compare as hex,
// holds it (6f9619ff8b86d011b42d00c04fc964ff), and as a UUID column does,
// with dashes, whichever form it was given in.
func (id identifier) forms() []string {
	out := []string{id.Normal}
	if u, err := uuid.Parse(id.Normal); err == nil {
		dashed := u.String()
		for _, f := range []string{dashed, strings.ReplaceAll(dashed, "-", "")} {
			if !slices.Contains(out, f) {
				out = append(out, f)
			}
		}
	}
	return out
}

// hashes are the salted hashes a column tagged with s's value and digest
// holds.
func (s Subject) hashes(digest string) []string {
	if digest == "" {
		return s.plain
	}
	return s.digests[digest]
}

// truncations are what Iceberg writes as a column's lower and upper bounds
// when s, longer than Iceberg keeps, is its smallest or largest value:
// for a string, its first boundLength characters, and those with the last
// one that can be incremented incremented and the rest dropped
// (truncateStringMax in Java's Iceberg); for a binary value, which
// compares as hex, its first boundLength bytes, and those incremented as
// a number with trailing 0xff bytes dropped (truncateBinaryMax).
func truncations(s string) []string {
	var out []string
	if r := []rune(s); len(r) > boundLength {
		r = r[:boundLength]
		out = append(out, string(r))
		for i := len(r) - 1; i >= 0; i-- {
			next := r[i] + 1
			if next >= 0xd800 && next <= 0xdfff {
				next = 0xe000
			}
			if next <= unicode.MaxRune {
				out = append(out, string(r[:i])+string(next))
				break
			}
		}
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) > boundLength {
		b = b[:boundLength]
		out = append(out, hex.EncodeToString(b))
		for i := len(b) - 1; i >= 0; i-- {
			if b[i] < 0xff {
				up := slices.Clone(b[:i+1])
				up[i]++
				out = append(out, hex.EncodeToString(up))
				break
			}
		}
	}
	return out
}

// matcher answers whether values are a request's identifiers.
type matcher struct {
	salt string
	fold *folding
	// exact are the hashes of identifiers and their digests; prefixes of
	// their truncations in bounds.
	exact, prefixes map[string]bool
}

func newMatcher(r *Request) *matcher {
	m := &matcher{salt: r.salt, fold: r.fold, exact: map[string]bool{}, prefixes: map[string]bool{}}
	for _, s := range r.Subjects {
		for _, h := range s.plain {
			m.exact[h] = true
		}
		for _, hs := range s.digests {
			for _, h := range hs {
				m.exact[h] = true
			}
		}
		for _, h := range s.prefixes {
			m.prefixes[h] = true
		}
	}
	return m
}

func (m *matcher) matches(s string) bool { return m.exact[salted(m.salt, m.fold.normal(s))] }

// matchesBound reports whether a manifest bound may hold an identifier: it
// is one, or what one is truncated to.
func (m *matcher) matchesBound(s string) bool {
	h := salted(m.salt, m.fold.normal(s))
	return m.exact[h] || m.prefixes[h]
}

// redactor removes a request's identifiers from history text, wherever
// they are in it: every occurrence of an identifier, compared without
// regard to case, as it is, with its quotes doubled as in an SQL string,
// as URLs and paths escape it (o%27brien%40), or of a hex digest of it,
// that is not part of a longer word (an identifier that starts or ends
// with a letter or digit must not have one beside it there). It
// implements audit.Redactor.
type redactor struct {
	ac   *automaton
	fold *folding
}

func newRedactor(r *Request) *redactor {
	var patterns []string
	for _, s := range r.Subjects {
		for _, id := range s.ids {
			for _, form := range append(id.forms(), r.fold.normal(id.Given)) {
				patterns = append(patterns, form, strings.ReplaceAll(form, "'", "''"),
					url.PathEscape(form), url.QueryEscape(form))
			}
			for _, digest := range digests {
				for _, form := range []string{id.Given, id.Normal} {
					patterns = append(patterns, hex.EncodeToString(digest([]byte(form))))
				}
			}
		}
	}
	for i, p := range patterns {
		patterns[i] = r.fold.normal(p)
	}
	slices.Sort(patterns)
	patterns = slices.Compact(slices.DeleteFunc(patterns, func(p string) bool { return p == "" }))
	return &redactor{ac: newAutomaton(patterns), fold: r.fold}
}

// Redact implements audit.Redactor.
func (x *redactor) Redact(text string) (string, bool) {
	if text == "" {
		return text, false
	}
	// Match on the lower-cased text, and map what matched back to the
	// text as it is: start is the offset in text of the character each
	// byte of low comes from.
	low := make([]byte, 0, len(text))
	start := make([]int, 0, len(text)+1)
	for i, r := range text {
		n := len(low)
		low = x.fold.appendLower(low, r)
		for range len(low) - n {
			start = append(start, i)
		}
	}
	start = append(start, len(text))
	var spans [][2]int
	for _, m := range x.ac.find(low) {
		from, to := start[m[0]], start[m[1]]
		if to <= from || !boundary(text, from, to) {
			continue
		}
		spans = append(spans, [2]int{from, to})
	}
	if len(spans) == 0 {
		return text, false
	}
	slices.SortFunc(spans, func(a, b [2]int) int { return a[0] - b[0] })
	var b strings.Builder
	at := 0
	for _, s := range spans {
		if s[0] < at {
			// Overlapping matches redact their union.
			if s[1] > at {
				at = s[1]
			}
			continue
		}
		b.WriteString(text[at:s[0]])
		b.WriteString("***")
		at = s[1]
	}
	b.WriteString(text[at:])
	return b.String(), true
}

// boundary reports whether text[from:to] is not part of a longer word.
func boundary(text string, from, to int) bool {
	word := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }
	first, _ := utf8.DecodeRuneInString(text[from:])
	last, _ := utf8.DecodeLastRuneInString(text[:to])
	if word(first) && from > 0 {
		if before, _ := utf8.DecodeLastRuneInString(text[:from]); word(before) {
			return false
		}
	}
	if word(last) && to < len(text) {
		if after, _ := utf8.DecodeRuneInString(text[to:]); word(after) {
			return false
		}
	}
	return true
}

// automaton finds many byte strings in a text at once (Aho-Corasick).
type automaton struct {
	// edges are the trie's, by state<<8 | byte.
	edges map[uint64]int32
	fail  []int32
	// out are the lengths of the patterns that end at each state.
	out [][]int32
}

func newAutomaton(patterns []string) *automaton {
	a := &automaton{edges: map[uint64]int32{}, fail: []int32{0}, out: [][]int32{nil}}
	kids := [][]int32{nil}
	char := []byte{0}
	for _, p := range patterns {
		var s int32
		for i := 0; i < len(p); i++ {
			n, ok := a.edges[uint64(s)<<8|uint64(p[i])]
			if !ok {
				n = int32(len(a.fail))
				a.edges[uint64(s)<<8|uint64(p[i])] = n
				a.fail, a.out = append(a.fail, 0), append(a.out, nil)
				kids, char = append(kids, nil), append(char, p[i])
				kids[s] = append(kids[s], n)
			}
			s = n
		}
		a.out[s] = append(a.out[s], int32(len(p)))
	}
	queue := slices.Clone(kids[0])
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		for _, n := range kids[s] {
			for f := a.fail[s]; ; f = a.fail[f] {
				if g, ok := a.edges[uint64(f)<<8|uint64(char[n])]; ok {
					a.fail[n] = g
					break
				}
				if f == 0 {
					break
				}
			}
			a.out[n] = append(a.out[n], a.out[a.fail[n]]...)
			queue = append(queue, n)
		}
	}
	return a
}

// find returns where patterns occur in text, as [start, end) byte offsets.
func (a *automaton) find(text []byte) [][2]int {
	var out [][2]int
	var s int32
	for i, c := range text {
		for {
			if n, ok := a.edges[uint64(s)<<8|uint64(c)]; ok {
				s = n
				break
			}
			if s == 0 {
				break
			}
			s = a.fail[s]
		}
		for _, l := range a.out[s] {
			out = append(out, [2]int{i + 1 - int(l), i + 1})
		}
	}
	return out
}

// longHex is how hashes look in error messages: DuckDB truncates long
// values it quotes, so a hash can show as few as 16 of its digits.
var longHex = regexp.MustCompile(`[0-9a-fA-F]{16,}`)

// scrub removes from a failed run's error what could identify the person:
// the identifiers, and the request's salt and hashes, which DuckDB quotes
// from statements, whole or cut short, as runs of 16 hex digits or more
// of one of them. Other long runs of digits stay: snapshot IDs, storage
// request IDs and the like, which say what went wrong.
func scrub(r *Request, msg string) string {
	if len(r.Subjects) > 0 && r.Subjects[0].ids != nil {
		msg, _ = newRedactor(r).Redact(msg)
	}
	secrets := r.secrets()
	return longHex.ReplaceAllStringFunc(msg, func(run string) string {
		low := strings.ToLower(run)
		for _, s := range secrets {
			if strings.Contains(s, low) || len(s) >= 16 && strings.Contains(low, s[:16]) {
				return "***"
			}
		}
		return run
	})
}

// secrets are the request's salts and hashes, in lower-case hex.
func (r *Request) secrets() []string {
	var out []string
	for _, s := range []string{r.salt, r.Salt} {
		if s != "" {
			out = append(out, strings.ToLower(s))
		}
	}
	for _, sub := range r.Subjects {
		out = append(out, sub.plain...)
		out = append(out, sub.prefixes...)
		for _, hs := range sub.digests {
			out = append(out, hs...)
		}
	}
	return out
}
