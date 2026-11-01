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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
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

// fakeAzure answers as Azure's blob endpoint does for a container's listing of
// uncommitted blobs, a blob's staged blocks and the account's kind.
type fakeAzure struct {
	code    string // an error of this code to the listing, if set
	status  int
	list    string
	blocks  map[string]int
	deleted []string
}

func (f *fakeAzure) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := strings.TrimPrefix(r.URL.Path, "/warehouse/")
	switch {
	case q.Get("comp") == "list":
		if f.code != "" {
			rw.Header().Set("x-ms-error-code", f.code)
			rw.WriteHeader(f.status)
			fmt.Fprintf(rw, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>no</Message></Error>`, f.code)
			return
		}
		rw.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(rw, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="warehouse"><Blobs>%s</Blobs><NextMarker/></EnumerationResults>`, f.list)
	case q.Get("comp") == "blocklist" && r.Method == http.MethodGet:
		n, ok := f.blocks[name]
		if !ok {
			rw.Header().Set("x-ms-error-code", "BlobNotFound")
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		rw.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(rw, `<?xml version="1.0" encoding="utf-8"?><BlockList><CommittedBlocks/><UncommittedBlocks>`)
		for i := range n {
			fmt.Fprintf(rw, `<Block><Name>%d</Name><Size>4</Size></Block>`, i)
		}
		fmt.Fprint(rw, `</UncommittedBlocks></BlockList>`)
	case q.Get("comp") == "blocklist" && r.Method == http.MethodPut:
		rw.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodDelete:
		f.deleted = append(f.deleted, name)
		rw.WriteHeader(http.StatusAccepted)
	default:
		http.Error(rw, "unexpected "+r.URL.String(), http.StatusBadRequest)
	}
}

func listedBlob(name string, length int, extra string) string {
	return fmt.Sprintf(`<Blob><Name>%s</Name><Properties><Last-Modified>Sun, 01 Nov 2026 10:00:00 GMT</Last-Modified><Etag>e</Etag><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType>%s</Properties></Blob>`, name, length, extra)
}

func azureStoreOf(t *testing.T, f *fakeAzure) *azureStore {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client, err := container.NewClientWithNoCredential(srv.URL+"/warehouse", &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}, InsecureAllowCredentialWithHTTP: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &azureStore{client: client, base: "abfss://warehouse@a.dfs.core.windows.net/"}
}

// TestAzureListingFailsClosed checks that no refusal of the listing passes for having
// no uploads: real accounts of either kind list them, and anything else must stop an
// erasure request from completing.
func TestAzureListingFailsClosed(t *testing.T) {
	ctx := context.Background()
	const prefix = "abfss://warehouse@a.dfs.core.windows.net/w/"
	for _, c := range []struct {
		name string
		fake fakeAzure
	}{
		{"the query refused", fakeAzure{code: "InvalidQueryParameterValue", status: 400}},
		{"as an unsupported one", fakeAzure{code: "UnsupportedQueryParameter", status: 400}},
		{"not found", fakeAzure{code: "ContainerNotFound", status: 404}},
		{"conflict", fakeAzure{code: "ContainerBeingDeleted", status: 409}},
		{"any other 400", fakeAzure{code: "InvalidUri", status: 400}},
		{"too many requests", fakeAzure{code: "ServerBusy", status: 429}},
		{"denied", fakeAzure{code: "AuthorizationPermissionMismatch", status: 403}},
		{"the store is down", fakeAzure{code: "InternalError", status: 500}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := c.fake
			if got, err := azureStoreOf(t, &f).List(ctx, prefix); err == nil {
				t.Errorf("no error, %d uploads: a request could complete without seeing them", len(got))
			}
		})
	}
}

// TestAzureUncommitted checks which listed blobs count, and that one's end goes
// through a commit of no blocks and a delete.
func TestAzureUncommitted(t *testing.T) {
	ctx := context.Background()
	f := &fakeAzure{blocks: map[string]int{"w/staged": 2, "w/empty": 0}, list: listedBlob("w/full", 100, "") +
		listedBlob("w/dir", 0, "<ResourceType>directory</ResourceType>") + listedBlob("w/staged", 0, "") + listedBlob("w/empty", 0, "") + listedBlob("w/gone", 0, "")}
	s := azureStoreOf(t, f)
	got, err := s.List(ctx, "abfss://warehouse@a.dfs.core.windows.net/w/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Path != "abfss://warehouse@a.dfs.core.windows.net/w/staged" || got[0].Initiated.IsZero() {
		t.Fatalf("listed %+v", got)
	}
	if err := s.Abort(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.deleted, []string{"w/staged"}) {
		t.Errorf("deleted %q", f.deleted)
	}
	// A folder by the metadata a filesystem gives it, not asked for staged blocks.
	f.list, f.blocks = `<Blob><Name>w/folder</Name><Properties><Last-Modified>Sun, 01 Nov 2026 10:00:00 GMT</Last-Modified><Etag>e</Etag><Content-Length>0</Content-Length><BlobType>BlockBlob</BlobType></Properties><Metadata><hdi_isfolder>true</hdi_isfolder></Metadata></Blob>`, nil
	if got, err := s.List(ctx, "abfss://warehouse@a.dfs.core.windows.net/w/"); err != nil || len(got) != 0 {
		t.Errorf("a folder: %+v, %v", got, err)
	}
}
