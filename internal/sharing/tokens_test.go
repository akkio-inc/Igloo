package sharing

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestTokens(t *testing.T) {
	tok, err := newToken(tokenPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok.Text, tokenPrefix) || strings.ContainsAny(tok.Text, " /+=") {
		t.Fatalf("token %q is not a bearer token of URL-safe characters", tok.Text)
	}
	id, hash, ok := parseToken(tokenPrefix, tok.Text)
	if !ok || id != tok.ID || !sameHash(hash, tok.Hash) {
		t.Fatalf("parse(%q) = %q %x %v, want %q %x", tok.Text, id, hash, ok, tok.ID, tok.Hash)
	}

	// A token is not an activation code, and a changed secret is a
	// different token with the same ID.
	if _, _, ok := parseToken(activationPrefix, tok.Text); ok {
		t.Error("a bearer token parsed as an activation code")
	}
	// The last character of the secret carries only some of its bits;
	// change one in the middle.
	mid := len(tok.Text) - 20
	swap := byte('A')
	if tok.Text[mid] == 'A' {
		swap = 'B'
	}
	tampered := tok.Text[:mid] + string(swap) + tok.Text[mid+1:]
	if id, hash, ok := parseToken(tokenPrefix, tampered); !ok || id != tok.ID || sameHash(hash, tok.Hash) {
		t.Error("a changed secret should parse to the same ID and another hash")
	}
	for _, bad := range []string{"", tokenPrefix, tok.Text + "x", strings.Replace(tok.Text, "_", "-", 1), "dst_" + strings.Repeat("z", 24) + tok.Text[28:]} {
		if _, _, ok := parseToken(tokenPrefix, bad); ok {
			t.Errorf("parse(%q) succeeded", bad)
		}
	}

	other, _ := newToken(tokenPrefix)
	if other.ID == tok.ID || bytes.Equal(other.Hash, tok.Hash) {
		t.Error("two tokens are the same")
	}
	if sameHash(tok.Hash, tok.Hash[:16]) {
		t.Error("hashes of different lengths compare equal")
	}
}

func TestSealer(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	files, err := newSealer(key, "file url v1")
	if err != nil {
		t.Fatal(err)
	}
	tokens, _ := newSealer(key, "provider token v1")
	if _, err := newSealer(key[:31], "x"); err == nil {
		t.Error("a short key was accepted")
	}

	now := time.Unix(1_800_000_000, 0)
	g := fileGrant{Token: "abc", Path: "s3://bucket/warehouse/t/data/f.parquet", Size: 42, Expires: now.Add(time.Hour).Unix(), Table: "s.p.t"}
	text, err := files.sealFile(g)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "bucket") || strings.Contains(text, "warehouse") {
		t.Error("the file URL reveals the path")
	}
	got, err := files.openFile(text, now)
	if err != nil || got != g {
		t.Fatalf("openFile = %+v, %v; want %+v", got, err, g)
	}
	if _, err := files.openFile(text, now.Add(time.Hour)); err == nil {
		t.Error("an expired file URL opened")
	}
	if _, err := tokens.openFile(text, now); err == nil {
		t.Error("a key for another purpose opened a file URL")
	}
	raw := []byte(text)
	raw[len(raw)/2] ^= 1
	if _, err := files.openFile(string(raw), now); err == nil {
		t.Error("a tampered file URL opened")
	}
	if _, err := files.openFile("not base64!", now); err == nil {
		t.Error("garbage opened")
	}

	sealed, err := tokens.seal([]byte("secret token"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("secret")) {
		t.Error("the sealed token holds the plain text")
	}
	if plain, err := tokens.open(sealed); err != nil || string(plain) != "secret token" {
		t.Errorf("open = %q, %v", plain, err)
	}
}
