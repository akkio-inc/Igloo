package uploads

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeCopies answers as S3 does for a copy in parts of a file of size bytes, whose
// version changes at the part changeAt, if set.
type fakeCopies struct {
	mu                sync.Mutex
	size              int64
	ranges            map[int]string
	etags             map[string]bool
	completed         string
	aborted           int
	changeAt, started int
}

func (f *fakeCopies) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		rw.Header().Set("Content-Length", strconv.FormatInt(f.size, 10))
		rw.Header().Set("ETag", `"v1"`)
	case r.Method == http.MethodPost && q.Has("uploads"):
		fmt.Fprint(rw, "<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>")
	case r.Method == http.MethodPut && q.Has("partNumber"):
		n, _ := strconv.Atoi(q.Get("partNumber"))
		f.started++
		f.etags[r.Header.Get("x-amz-copy-source-if-match")] = true
		if f.changeAt > 0 && f.started >= f.changeAt {
			rw.WriteHeader(http.StatusPreconditionFailed)
			fmt.Fprint(rw, "<Error><Code>PreconditionFailed</Code></Error>")
			return
		}
		if !strings.HasPrefix(r.Header.Get("x-amz-copy-source"), "bucket/w/a%20b/") {
			http.Error(rw, "the source: "+r.Header.Get("x-amz-copy-source"), http.StatusBadRequest)
			return
		}
		f.ranges[n] = r.Header.Get("x-amz-copy-source-range")
		fmt.Fprintf(rw, "<CopyPartResult><ETag>\"p%d\"</ETag></CopyPartResult>", n)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		b, _ := io.ReadAll(r.Body)
		f.completed = string(b)
		fmt.Fprint(rw, "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>")
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		f.aborted++
		rw.WriteHeader(http.StatusNoContent)
	default:
		http.Error(rw, "unexpected "+r.URL.String(), http.StatusBadRequest)
	}
}

func copierOf(t *testing.T, f *fakeCopies) *s3Store {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client := s3.New(s3.Options{BaseEndpoint: aws.String(srv.URL), Region: "us-east-1", UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", "")})
	return &s3Store{client: client, scheme: "s3", bucket: "bucket"}
}

// TestCopyInParts checks that a file past what CopyObject copies goes in parts that
// cover it, in order, of the one version of it.
func TestCopyInParts(t *testing.T) {
	const size = 5<<30 + 7
	f := &fakeCopies{size: size, ranges: map[int]string{}, etags: map[string]bool{}}
	if err := copierOf(t, f).CopyInParts(t.Context(), "s3://bucket/w/a b/f.parquet", "s3://bucket/w/sealed/f.parquet"); err != nil {
		t.Fatal(err)
	}
	want := int(int64(size)/copyPartBytes) + 1
	if len(f.ranges) != want {
		t.Fatalf("%d parts, want %d", len(f.ranges), want)
	}
	var next int64
	for n := 1; n <= want; n++ {
		var first, last int64
		if _, err := fmt.Sscanf(f.ranges[n], "bytes=%d-%d", &first, &last); err != nil {
			t.Fatalf("part %d: %q", n, f.ranges[n])
		}
		if first != next {
			t.Errorf("part %d starts at %d, want %d", n, first, next)
		}
		next = last + 1
	}
	if next != size {
		t.Errorf("the parts end at %d, want %d", next, size)
	}
	if len(f.etags) != 1 || !f.etags[`"v1"`] {
		t.Errorf("parts of versions %v", f.etags)
	}
	if strings.Index(f.completed, "p1&#34;") > strings.Index(f.completed, "p2&#34;") || !strings.Contains(f.completed, fmt.Sprintf("p%d&#34;", want)) {
		t.Errorf("completed with %.200s", f.completed)
	}
	if f.aborted != 0 {
		t.Errorf("%d aborted", f.aborted)
	}
}

// TestCopyInPartsOverwritten checks that a file that changes during its copy fails it,
// and that what it began goes.
func TestCopyInPartsOverwritten(t *testing.T) {
	f := &fakeCopies{size: 1 << 30, ranges: map[int]string{}, etags: map[string]bool{}, changeAt: 3}
	if err := copierOf(t, f).CopyInParts(t.Context(), "s3://bucket/w/a b/f.parquet", "s3://bucket/w/sealed/f.parquet"); err == nil {
		t.Fatal("a file overwritten during its copy was copied")
	}
	if f.aborted != 1 || f.completed != "" {
		t.Errorf("aborted %d times, and completed %q", f.aborted, f.completed)
	}
}

// TestCopierFor checks which stores copy in parts.
func TestCopierFor(t *testing.T) {
	if CopierFor(nil, "gs://b/w") != nil || CopierFor(nil, "abfss://c@a.dfs.core.windows.net/w") != nil || CopierFor(nil, "/local/w") != nil {
		t.Error("a store with no limit on a copy")
	}
}
