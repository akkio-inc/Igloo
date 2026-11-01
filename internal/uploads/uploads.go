// Package uploads finds and ends the writes object stores keep unfinished: what
// a process that stopped had uploaded of a file, which no listing of files
// shows. These are not out of reach. With the right to write, S3 and GCS list the
// multipart uploads and their parts, and whoever finishes one reads the file,
// and on Azure a blob's uncommitted blocks can be committed. So what a statement
// that stopped had of a table's rows stays recoverable until an upload is
// aborted or the bucket's lifecycle rule does it, and an erasure request has to
// see to them.
package uploads

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Upload is a file not finished in a store.
type Upload struct {
	// Path is where the file would be, spelled as the locations given to List.
	Path string
	// Initiated is when it began.
	Initiated time.Time
	// id tells it from others of the same Path.
	id string
}

// Store lists and ends a store's unfinished writes.
type Store interface {
	// List returns the unfinished writes under prefix, a location.
	List(ctx context.Context, prefix string) ([]Upload, error)
	// Abort ends one, and what it had goes. One already gone is no error.
	Abort(ctx context.Context, u Upload) error
}

// Keys are what the store's own IO does not say. GCS's IO uses the JSON API,
// which knows no multipart uploads; DuckDB uses its XML API, with HMAC keys, and
// these are those.
type Keys struct {
	GCSKeyID, GCSSecret string
}

// gcsEndpoint is GCS's XML API, which speaks S3's.
const gcsEndpoint = "https://storage.googleapis.com"

// For returns the Store of the location's store, reached through fsys, or nil for
// a store with no such thing, as the local file system.
func For(fsys icebergio.IO, location string, keys Keys) (Store, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("the location %s: %w", location, err)
	}
	as, _ := fsys.(interface{ As(any) bool })
	switch u.Scheme {
	case "s3", "s3a", "s3n":
		var client *s3.Client
		if as == nil || !as.As(&client) {
			return nil, fmt.Errorf("the store of %s has no S3 client", location)
		}
		return &s3Store{client: client, scheme: u.Scheme, bucket: u.Host}, nil
	case "gs", "gcs":
		if keys.GCSKeyID == "" || keys.GCSSecret == "" {
			return nil, fmt.Errorf("the store of %s needs HMAC keys to see its multipart uploads", location)
		}
		client := s3.New(s3.Options{
			BaseEndpoint: aws.String(gcsEndpoint),
			Region:       "auto",
			UsePathStyle: true,
			Credentials:  credentials.NewStaticCredentialsProvider(keys.GCSKeyID, keys.GCSSecret, ""),
		})
		return &s3Store{client: client, scheme: u.Scheme, bucket: u.Host}, nil
	case "az", "azure", "abfs", "abfss", "wasb", "wasbs":
		var client *container.Client
		if as == nil || !as.As(&client) {
			return nil, fmt.Errorf("the store of %s has no Azure client", location)
		}
		base, err := containerRoot(location)
		if err != nil {
			return nil, err
		}
		return &azureStore{client: client, base: base}, nil
	}
	return nil, nil
}

// s3Store is S3's multipart uploads, and those of the stores that speak it.
type s3Store struct {
	client         *s3.Client
	scheme, bucket string
}

func (s *s3Store) key(location string) (string, error) {
	root := s.scheme + "://" + s.bucket + "/"
	key, ok := strings.CutPrefix(location, root)
	if !ok {
		return "", fmt.Errorf("%s is not under %s", location, root)
	}
	return key, nil
}

// List implements Store.
func (s *s3Store) List(ctx context.Context, prefix string) ([]Upload, error) {
	key, err := s.key(prefix)
	if err != nil {
		return nil, err
	}
	in := &s3.ListMultipartUploadsInput{Bucket: &s.bucket, Prefix: &key}
	var out []Upload
	for {
		page, err := s.client.ListMultipartUploads(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, u := range page.Uploads {
			out = append(out, Upload{
				Path:      s.scheme + "://" + s.bucket + "/" + aws.ToString(u.Key),
				Initiated: aws.ToTime(u.Initiated),
				id:        aws.ToString(u.UploadId),
			})
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, nil
		}
		in.KeyMarker, in.UploadIdMarker = page.NextKeyMarker, page.NextUploadIdMarker
	}
}

// Abort implements Store.
func (s *s3Store) Abort(ctx context.Context, u Upload) error {
	key, err := s.key(u.Path)
	if err != nil {
		return err
	}
	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: &key, UploadId: &u.id})
	var gone *s3types.NoSuchUpload
	if errors.As(err, &gone) {
		return nil
	}
	return err
}

// azureStore is Azure's uncommitted blobs: those with blocks staged and none
// committed, which a listing that asks for them shows, of no length. A blob
// committed before keeps what it has, with blocks staged since it, and a blob on
// a filesystem written through DFS is a file of no length until it is flushed:
// both are files that listings show.
type azureStore struct {
	client *container.Client
	base   string
}

// containerRoot is a location's container's root: everything up to the path.
func containerRoot(location string) (string, error) {
	scheme, rest, ok := strings.Cut(location, "://")
	if !ok {
		return "", fmt.Errorf("%s is not a URL", location)
	}
	host, _, ok := strings.Cut(rest, "/")
	if !ok {
		return location + "/", nil
	}
	return scheme + "://" + host + "/", nil
}

// List implements Store. A store that takes no such listing, as a filesystem may
// not, has none to list: what a DFS write leaves is a file.
func (s *azureStore) List(ctx context.Context, prefix string) ([]Upload, error) {
	key, ok := strings.CutPrefix(prefix, s.base)
	if !ok {
		return nil, fmt.Errorf("%s is not under %s", prefix, s.base)
	}
	pager := s.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  &key,
		Include: container.ListBlobsInclude{UncommittedBlobs: true},
	})
	var out []Upload
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			var re *azcore.ResponseError
			if errors.As(err, &re) && re.StatusCode >= 400 && re.StatusCode < 500 &&
				re.StatusCode != http.StatusUnauthorized && re.StatusCode != http.StatusForbidden {
				return nil, nil
			}
			return nil, err
		}
		for _, b := range page.Segment.BlobItems {
			if b.Name == nil || b.Properties == nil || b.Properties.ContentLength == nil || *b.Properties.ContentLength != 0 {
				continue
			}
			at := time.Time{}
			if b.Properties.LastModified != nil {
				at = *b.Properties.LastModified
			}
			out = append(out, Upload{Path: s.base + *b.Name, Initiated: at, id: *b.Name})
		}
	}
	return out, nil
}

// Abort implements Store. Committing no blocks drops those staged, and takes the
// blob's place, and the delete then takes that.
func (s *azureStore) Abort(ctx context.Context, u Upload) error {
	blob := s.client.NewBlockBlobClient(u.id)
	if _, err := blob.CommitBlockList(ctx, nil, nil); err != nil {
		return err
	}
	_, err := blob.Delete(ctx, nil)
	return err
}

// AbortUnder ends every write under prefix that began before the time, if one is
// given, and returns how many it ended.
func AbortUnder(ctx context.Context, store Store, prefix string, before time.Time) (int, error) {
	if store == nil {
		return 0, nil
	}
	list, err := store.List(ctx, prefix)
	if err != nil {
		return 0, err
	}
	var errs []error
	n := 0
	for _, u := range list {
		if !before.IsZero() && !u.Initiated.Before(before) {
			continue
		}
		if err := store.Abort(ctx, u); err != nil {
			errs = append(errs, fmt.Errorf("abort the upload of %s: %w", u.Path, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}
