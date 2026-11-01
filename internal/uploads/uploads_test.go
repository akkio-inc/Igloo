package uploads

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3 answers as S3 does for a bucket's multipart uploads, two to a page.
type fakeS3 struct {
	mu      sync.Mutex
	uploads map[string]time.Time // key, by upload ID key+"-id"
	aborted []string
}

func (f *fakeS3) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Has("uploads"):
		var keys []string
		for k := range f.uploads {
			if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("key-marker") {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		truncated := len(keys) > 2
		if truncated {
			keys = keys[:2]
		}
		fmt.Fprintf(rw, "<ListMultipartUploadsResult><IsTruncated>%v</IsTruncated>", truncated)
		if truncated {
			fmt.Fprintf(rw, "<NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>%s-id</NextUploadIdMarker>", keys[1], keys[1])
		}
		for _, k := range keys {
			fmt.Fprintf(rw, "<Upload><Key>%s</Key><UploadId>%s-id</UploadId><Initiated>%s</Initiated></Upload>", k, k, f.uploads[k].UTC().Format(time.RFC3339))
		}
		fmt.Fprint(rw, "</ListMultipartUploadsResult>")
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if q.Get("uploadId") != key+"-id" {
			http.Error(rw, "wrong upload", http.StatusBadRequest)
			return
		}
		if _, ok := f.uploads[key]; !ok {
			rw.WriteHeader(http.StatusNotFound)
			fmt.Fprint(rw, "<Error><Code>NoSuchUpload</Code></Error>")
			return
		}
		delete(f.uploads, key)
		f.aborted = append(f.aborted, key)
		rw.WriteHeader(http.StatusNoContent)
	default:
		http.Error(rw, "unexpected "+r.URL.String(), http.StatusBadRequest)
	}
}

// TestAbortUnder checks that uploads are listed page by page under the prefix,
// and that those before the time given go.
func TestAbortUnder(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	fake := &fakeS3{uploads: map[string]time.Time{
		"w/t/data/igloo-write-1/a":     now.Add(-time.Hour),
		"w/t/data/igloo-write-1/b":     now.Add(-time.Hour),
		"w/t/data/igloo-write-1/c":     now.Add(-time.Hour),
		"w/t/data/igloo-write-1/young": now.Add(time.Hour),
		"w/t/data/other":               now.Add(-time.Hour),
		"w/u/data/igloo-write-2/a":     now.Add(-time.Hour),
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL), Region: "us-east-1", UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
	})
	store := &s3Store{client: client, scheme: "s3", bucket: "bucket"}

	got, err := store.List(ctx, "s3://bucket/w/t/data/igloo-write-1/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].Path != "s3://bucket/w/t/data/igloo-write-1/a" {
		t.Errorf("listed %+v", got)
	}
	n, err := AbortUnder(ctx, store, "s3://bucket/w/t/data/igloo-write-1/", now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || len(fake.uploads) != 3 {
		t.Errorf("aborted %d, left %v", n, fake.uploads)
	}
	if _, ok := fake.uploads["w/t/data/igloo-write-1/young"]; !ok {
		t.Error("one that began after the time went")
	}
	if n, err = AbortUnder(ctx, store, "s3://bucket/w/t/data/igloo-write-1/", time.Time{}); err != nil || n != 1 {
		t.Errorf("with no time: %d, %v", n, err)
	}
	// Gone already, as a lifecycle rule or another replica had it.
	if err := store.Abort(ctx, Upload{Path: "s3://bucket/w/t/data/igloo-write-1/a", id: "w/t/data/igloo-write-1/a-id"}); err != nil {
		t.Errorf("an upload already gone: %v", err)
	}
	if _, err := store.List(ctx, "s3://other/x/"); err == nil {
		t.Error("listed another bucket's")
	}
}

// TestFor checks which stores have uploads, and that one that needs what it was
// not given says so.
func TestFor(t *testing.T) {
	if s, err := For(icebergio.LocalFS{}, "/tmp/w/", Keys{}); s != nil || err != nil {
		t.Errorf("a directory: %v, %v", s, err)
	}
	if s, err := For(icebergio.LocalFS{}, "file:///tmp/w/", Keys{}); s != nil || err != nil {
		t.Errorf("file://: %v, %v", s, err)
	}
	if _, err := For(icebergio.LocalFS{}, "s3://b/w/", Keys{}); err == nil {
		t.Error("s3:// with no S3 client")
	}
	if _, err := For(icebergio.LocalFS{}, "gs://b/w/", Keys{}); err == nil {
		t.Error("gs:// with no keys")
	}
	s, err := For(icebergio.LocalFS{}, "gs://b/w/", Keys{GCSKeyID: "k", GCSSecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if g, ok := s.(*s3Store); !ok || g.bucket != "b" || g.scheme != "gs" {
		t.Errorf("gs://: %+v", s)
	}
	if _, err := For(icebergio.LocalFS{}, "abfss://c@a.dfs.core.windows.net/w/", Keys{}); err == nil {
		t.Error("abfss:// with no Azure client")
	}
	if got, err := containerRoot("abfss://c@a.dfs.core.windows.net/w/x"); err != nil || got != "abfss://c@a.dfs.core.windows.net/" {
		t.Errorf("container root %q, %v", got, err)
	}
}
