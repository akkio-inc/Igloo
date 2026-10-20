package sharing

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProxy(t *testing.T) {
	data := []byte("PAR1 some parquet bytes PAR1")
	var lastRange string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastRange = r.Header.Get("Range")
		if r.URL.Query().Get("sig") == "expired" {
			http.Error(w, "Request has expired", http.StatusForbidden)
			return
		}
		w.Header().Set("Set-Cookie", "tracking=1")
		http.ServeContent(w, r, "f.parquet", time.Time{}, bytes.NewReader(data))
	}))
	defer upstream.Close()

	p, err := NewProxy(nil)
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve()    //nolint:errcheck
	defer p.Close() //nolint:errcheck

	file := func(sig string) remoteFile {
		return remoteFile{ID: "file-1", URL: upstream.URL + "/f.parquet?sig=" + sig, Size: int64(len(data)), Expires: time.Now().Add(time.Hour)}
	}
	urls := p.register("partners.tvco", []remoteFile{file("expired")}, nil)
	if len(urls) != 1 || !strings.HasPrefix(urls[0], p.Prefix()) || !strings.HasSuffix(urls[0], ".parquet") ||
		strings.Contains(urls[0], "file-1") || strings.Contains(urls[0], "tvco") {
		t.Fatalf("proxy URL %q", urls)
	}

	get := func(method, u string, header ...string) (int, http.Header, string) {
		t.Helper()
		req, _ := http.NewRequest(method, u, nil)
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header, string(b)
	}

	// The provider's address is on loopback, which file downloads may
	// not reach unless it is a provider's own endpoint.
	if status, _, _ := get("GET", urls[0]); status != http.StatusBadGateway {
		t.Errorf("a loopback upstream: %d", status)
	}
	p.dialer.allow(endpointHostPort(upstream.URL))

	// An expired URL makes the proxy list the files again and retry.
	refreshed := 0
	p.refresh = func(_ context.Context, table string) error {
		refreshed++
		if table != "partners.tvco" {
			t.Errorf("refreshed %q", table)
		}
		p.register(table, []remoteFile{file("fresh")}, nil)
		return nil
	}
	status, h, body := get("GET", urls[0], "Range", "bytes=0-3")
	if status != http.StatusPartialContent || body != "PAR1" || refreshed != 1 || lastRange != "bytes=0-3" {
		t.Errorf("range read: %d %q, refreshed %d times, upstream saw %q", status, body, refreshed, lastRange)
	}
	if h.Get("Set-Cookie") != "" || h.Get("Content-Range") == "" {
		t.Errorf("headers passed on: %v", h)
	}
	status, h, _ = get("HEAD", urls[0])
	if status != 200 || h.Get("Content-Length") != "28" || h.Get("ETag") == "" || h.Get("Last-Modified") == "" {
		t.Errorf("HEAD: %d %v", status, h)
	}
	if time.Since(p.lastUsed("partners.tvco")) > time.Minute {
		t.Error("reading a file did not mark the table used")
	}

	// Nothing else is served.
	for _, u := range []string{p.Prefix() + "nope.parquet", strings.Replace(urls[0], p.Prefix(), "http://"+p.ln.Addr().String()+"/other/", 1)} {
		if status, _, _ := get("GET", u); status != http.StatusNotFound {
			t.Errorf("GET %s: %d", u, status)
		}
	}
	if status, _, _ := get("POST", urls[0]); status != http.StatusNotFound {
		t.Errorf("POST: %d", status)
	}
	p.forget("partners.tvco")
	if status, _, _ := get("GET", urls[0]); status != http.StatusNotFound {
		t.Errorf("a forgotten table's file: %d", status)
	}

	// Partition values go into Hive-style directories, escaped.
	value, empty := "a/b=c d", ""
	values := map[string]*string{"day": &value, "c": &empty}
	urls = p.register("t", []remoteFile{{ID: "x", URL: upstream.URL, Size: 1, Expires: time.Now().Add(time.Hour), PartitionValues: values}},
		func(int) string { return hiveDirs([]string{"day", "c", "missing"}, values) })
	if !strings.Contains(urls[0], "/day=a%2Fb%3Dc%20d/c=__HIVE_DEFAULT_PARTITION__/missing=__HIVE_DEFAULT_PARTITION__/") {
		t.Errorf("partitioned URL %q", urls[0])
	}

	// Files Igloo makes are served from memory, ranges too, under a URL
	// that follows from their contents.
	made := p.serve("t", []byte("PAR1 made PAR1"), time.Now().Add(time.Hour))
	if again := p.serve("t", []byte("PAR1 made PAR1"), time.Now().Add(time.Hour)); again != made || !strings.HasPrefix(made, p.Prefix()) {
		t.Errorf("made file URLs %q, %q", made, again)
	}
	if status, _, body := get("GET", made, "Range", "bytes=5-8"); status != http.StatusPartialContent || body != "made" {
		t.Errorf("a made file: %d %q", status, body)
	}
	if status, h, _ := get("HEAD", made); status != 200 || h.Get("Content-Length") != "14" {
		t.Errorf("HEAD of a made file: %d %v", status, h)
	}
	// A newer file of the table's leaves the older one a while.
	newer := p.serve("t", []byte("PAR1 newer PAR1"), time.Now().Add(time.Hour))
	p.mu.RLock()
	older := p.files[strings.TrimSuffix(made[strings.LastIndexByte(made, '/')+1:], ".parquet")]
	p.mu.RUnlock()
	if newer == made || older == nil || time.Until(older.expires) > madeFileGrace {
		t.Errorf("an older made file: %+v", older)
	}
}
