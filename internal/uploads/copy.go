package uploads

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

const (
	// CopyInPartsFrom is the size from which a file on S3 is copied in parts. S3's
	// CopyObject refuses more than 5 GiB, and is slow before that: 415 MB took it
	// 12 s, on real S3, about 35 MB/s.
	CopyInPartsFrom = 128 << 20
	// copyPartBytes and copyPartsAtOnce are how: far more parts than S3 would mind,
	// which never allows more than 10,000 of a file, up to 640 GiB.
	copyPartBytes   = 64 << 20
	copyPartsAtOnce = 8
	mostCopyParts   = 10_000
)

// PartCopier copies a file in parts, as one version of it, on a store that can.
type PartCopier interface {
	CopyInParts(ctx context.Context, src, dst string) error
}

// CopierFor returns how to copy large files of the location's store, reached through
// fsys, or nil for a store whose own Copy has no such limit: GCS rewrites and Azure
// copies server-side at any size, and GCS's XML API copies no parts.
func CopierFor(fsys icebergio.IO, location string) PartCopier {
	u, err := url.Parse(location)
	if err != nil {
		return nil
	}
	switch u.Scheme {
	case "s3", "s3a", "s3n":
		as, _ := fsys.(interface{ As(any) bool })
		var client *s3.Client
		if as != nil && as.As(&client) {
			return &s3Store{client: client, scheme: u.Scheme, bucket: u.Host}
		}
	}
	return nil
}

// CopyInParts implements PartCopier. Each part asks for the version of the file the
// first saw, so that a file overwritten meanwhile fails the copy, as it would not
// otherwise: parts of two files would make a file neither was. What it began goes,
// if it fails.
func (s *s3Store) CopyInParts(ctx context.Context, src, dst string) (err error) {
	srcKey, err := s.key(src)
	if err != nil {
		return err
	}
	dstKey, err := s.key(dst)
	if err != nil {
		return err
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &srcKey})
	if err != nil {
		return err
	}
	size, etag := aws.ToInt64(head.ContentLength), aws.ToString(head.ETag)
	parts := int64(max((size+copyPartBytes-1)/copyPartBytes, 1))
	if parts > mostCopyParts {
		return fmt.Errorf("%s is too large to copy, at %d bytes", src, size)
	}
	made, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: &dstKey})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, aerr := s.client.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: &dstKey, UploadId: made.UploadId})
			err = errors.Join(err, aerr)
		}
	}()
	source := s.bucket + "/" + (&url.URL{Path: srcKey}).EscapedPath()
	var mu sync.Mutex
	done := make([]s3types.CompletedPart, 0, parts)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(copyPartsAtOnce)
	for n := range parts {
		g.Go(func() error {
			first, last := n*copyPartBytes, min((n+1)*copyPartBytes, size)-1
			number := int32(n + 1) //nolint:gosec // at most mostCopyParts
			in := &s3.UploadPartCopyInput{Bucket: &s.bucket, Key: &dstKey, UploadId: made.UploadId, PartNumber: &number, CopySource: &source,
				CopySourceIfMatch: &etag, CopySourceRange: aws.String("bytes=" + strconv.FormatInt(first, 10) + "-" + strconv.FormatInt(last, 10))}
			out, err := s.client.UploadPartCopy(gctx, in)
			if err != nil {
				return err
			}
			if out.CopyPartResult == nil {
				return fmt.Errorf("no result of copying part %d of %s", number, src)
			}
			mu.Lock()
			defer mu.Unlock()
			done = append(done, s3types.CompletedPart{PartNumber: &number, ETag: out.CopyPartResult.ETag})
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	slices.SortFunc(done, func(a, b s3types.CompletedPart) int { return int(*a.PartNumber - *b.PartNumber) })
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &s.bucket, Key: &dstKey, UploadId: made.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: done}})
	return err
}
