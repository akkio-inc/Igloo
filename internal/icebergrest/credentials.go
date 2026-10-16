package icebergrest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/akkio-inc/igloo/internal/config"
)

// Credentials are storage credentials for one table's files, as Iceberg
// REST client properties (s3.access-key-id and so on).
type Credentials struct {
	// Prefix is the location they reach: the table's.
	Prefix  string
	Config  map[string]string
	Expires time.Time
}

// storageCredentials is the "storage-credentials" field of a response.
func (c *Credentials) storageCredentials() []map[string]any {
	return []map[string]any{{"prefix": c.Prefix, "config": c.Config}}
}

// Access is what vended credentials may do with a table's files.
type Access int

const (
	// ReadFiles reads them.
	ReadFiles Access = iota
	// WriteFiles reads, writes and deletes them.
	WriteFiles
	// AddFiles reads and writes them but deletes none, for a table whose
	// files clones read: an engine's own snapshot expiry or orphan cleanup
	// would delete files the clones need.
	AddFiles
)

// credentials describes credentials of a, for the history.
func (a Access) credentials() string {
	switch a {
	case WriteFiles:
		return "read-write credentials"
	case AddFiles:
		return "read-write credentials that delete nothing"
	}
	return "read credentials"
}

// Vendor hands out storage credentials limited to one table.
type Vendor interface {
	// Vend returns credentials that do what access says with the files
	// under location, and reach nothing else. user is who they are for,
	// for the storage provider's logs. It returns ErrNoCredentials when it
	// cannot limit credentials to location.
	Vend(ctx context.Context, location string, access Access, user string) (Credentials, error)
}

// ErrNoCredentials means credentials limited to the table cannot be given,
// so none are.
var ErrNoCredentials = errors.New("no credentials limited to the table can be given")

// credentialLifetime is how long vended credentials last: the longest AWS
// allows when a role assumes another.
const credentialLifetime = time.Hour

// compatRoleARN is the role asked for on S3-compatible stores, which serve
// STS on their own endpoint, ignore the role, and limit the caller's own
// access by the session policy.
const compatRoleARN = "arn:aws:iam::000000000000:role/igloo"

// S3Vendor vends credentials for Amazon S3 and S3-compatible stores with
// STS AssumeRole and a session policy that allows one table's prefix,
// read-only or read-write. The result can do no more than the role (on
// AWS, IGLOO_S3_STS_ROLE_ARN) or Igloo's own keys (S3-compatible stores),
// and no more than the policy.
//
// A store that accepted the policy but ignored it would hand out Igloo's
// whole access, so before the first credentials leave, the vendor gets
// some for a scratch prefix and checks that they are refused outside it.
// If they are not, it hands out none.
type S3Vendor struct {
	assume    func(ctx context.Context, in *sts.AssumeRoleInput) (*sts.AssumeRoleOutput, error)
	head      func(ctx context.Context, creds aws.Credentials, key string) (status int, err error)
	roleARN   string
	partition string
	bucket    string
	// root is the warehouse's key prefix, without slashes.
	root string
	// kms grants use of the bucket's KMS key, which the role limits; only
	// AWS knows such keys.
	kms bool
	// props are the client properties that say how to reach the store.
	props map[string]string
	log   *slog.Logger

	mu sync.Mutex
	// checked is set once the store has been seen to honor the policy;
	// ignored is set if it did not.
	checked bool
	ignored bool
}

// NewS3Vendor returns a vendor for an s3:// warehouse, or nil when it
// cannot vend: on AWS without IGLOO_S3_STS_ROLE_ARN.
func NewS3Vendor(ctx context.Context, s config.Storage, log *slog.Logger) (*S3Vendor, error) {
	if s.Type != config.StorageS3 || s.S3.Endpoint == "" && s.S3.STSRoleARN == "" {
		return nil, nil
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(s.S3.Region)}
	if s.S3.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s.S3.AccessKeyID, s.S3.SecretAccessKey, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("AWS configuration: %w", err)
	}

	v := &S3Vendor{
		roleARN:   s.S3.STSRoleARN,
		partition: "aws",
		bucket:    s.Bucket,
		root:      s.Prefix,
		props:     map[string]string{"client.region": s.S3.Region, "s3.region": s.S3.Region},
		log:       log.With("component", "iceberg"),
	}
	var endpoint string
	if s.S3.Endpoint != "" {
		scheme := "https://"
		if !s.S3.UseSSL {
			scheme = "http://"
		}
		endpoint = scheme + s.S3.Endpoint
		v.props["s3.endpoint"] = endpoint
		if v.roleARN == "" {
			v.roleARN = compatRoleARN
		}
	} else {
		v.kms = true
		v.partition = strings.SplitN(v.roleARN, ":", 3)[1]
	}
	pathStyle := s.S3.URLStyle == "path"
	if pathStyle {
		v.props["s3.path-style-access"] = "true"
	}

	stsClient := sts.NewFromConfig(cfg, func(o *sts.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = pathStyle
	})
	v.assume = func(ctx context.Context, in *sts.AssumeRoleInput) (*sts.AssumeRoleOutput, error) {
		return stsClient.AssumeRole(ctx, in)
	}
	v.head = func(ctx context.Context, creds aws.Credentials, key string) (int, error) {
		_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(v.bucket), Key: aws.String(key)},
			func(o *s3.Options) { o.Credentials = credentials.StaticCredentialsProvider{Value: creds} })
		var re *awshttp.ResponseError
		if errors.As(err, &re) {
			return re.HTTPStatusCode(), nil
		}
		if err != nil {
			return 0, err
		}
		return http.StatusOK, nil
	}
	return v, nil
}

// Vend implements Vendor.
func (v *S3Vendor) Vend(ctx context.Context, location string, access Access, user string) (Credentials, error) {
	prefix, err := v.tablePrefix(location)
	if err != nil {
		v.log.Warn("no credentials for a table outside the warehouse", "location", location, "err", err)
		return Credentials{}, ErrNoCredentials
	}
	if err := v.check(ctx); err != nil {
		return Credentials{}, err
	}
	creds, err := v.credentials(ctx, prefix, access, "igloo-"+user)
	if err != nil {
		return Credentials{}, err
	}
	config := map[string]string{
		"s3.access-key-id":               creds.AccessKeyID,
		"s3.secret-access-key":           creds.SecretAccessKey,
		"s3.session-token":               creds.SessionToken,
		"s3.session-token-expires-at-ms": strconv.FormatInt(creds.Expires.UnixMilli(), 10),
	}
	for k, val := range v.props {
		config[k] = val
	}
	return Credentials{Prefix: "s3://" + v.bucket + "/" + prefix, Config: config, Expires: creds.Expires}, nil
}

// credentials assumes the role with a policy allowing prefix only.
func (v *S3Vendor) credentials(ctx context.Context, prefix string, access Access, session string) (aws.Credentials, error) {
	policy, err := sessionPolicy(v.partition, v.bucket, prefix, access, v.kms)
	if err != nil {
		return aws.Credentials{}, err
	}
	out, err := v.assume(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(v.roleARN),
		RoleSessionName: aws.String(sessionName(session)),
		DurationSeconds: aws.Int32(int32(credentialLifetime.Seconds())),
		Policy:          aws.String(policy),
	})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("assume %s: %w", v.roleARN, err)
	}
	c := out.Credentials
	if c == nil || c.AccessKeyId == nil || c.SecretAccessKey == nil || c.SessionToken == nil || c.Expiration == nil {
		return aws.Credentials{}, errors.New("STS returned incomplete credentials")
	}
	return aws.Credentials{AccessKeyID: *c.AccessKeyId, SecretAccessKey: *c.SecretAccessKey,
		SessionToken: *c.SessionToken, Expires: *c.Expiration, CanExpire: true}, nil
}

// checkPrefix is the scratch prefix of the policy check, under the
// warehouse; nothing is written there.
const checkPrefix = ".igloo-credential-check"

// check makes sure, once, that the store limits credentials to their
// session policy: credentials for a scratch prefix must be refused a key
// outside it. A store that answers anything but 403 ignores the policy,
// and gets no credentials handed out for it.
func (v *S3Vendor) check(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case v.checked:
		return nil
	case v.ignored:
		return ErrNoCredentials
	}
	scratch := v.key(checkPrefix)
	creds, err := v.credentials(ctx, scratch, ReadFiles, "igloo-check")
	if err != nil {
		return err
	}
	status, err := v.head(ctx, creds, v.key(checkPrefix+"-outside"))
	if err != nil {
		return fmt.Errorf("check that storage credentials stay in their table: %w", err)
	}
	if status != http.StatusForbidden {
		v.ignored = true
		v.log.Error("the object store does not limit STS credentials to their session policy, so other engines get no storage credentials",
			"status", status)
		return ErrNoCredentials
	}
	v.checked = true
	return nil
}

// key is a key under the warehouse root.
func (v *S3Vendor) key(name string) string {
	if v.root == "" {
		return name
	}
	return v.root + "/" + name
}

// safeSegment is what a path segment of a table location may be. Policy
// wildcards (*, ?) and variables (${...}) would widen a session policy.
var safeSegment = regexp.MustCompile(`^[A-Za-z0-9._=+-]+$`)

// tablePrefix returns the key prefix of a table at location, which must be
// a plain path in the warehouse, below its root.
func (v *S3Vendor) tablePrefix(location string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if u.Scheme != "s3" || u.Host != v.bucket || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", fmt.Errorf("not in bucket %s", v.bucket)
	}
	key := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/"), "/")
	for _, seg := range strings.Split(key, "/") {
		if !safeSegment.MatchString(seg) || seg == "." || seg == ".." {
			return "", fmt.Errorf("unexpected path segment %q", seg)
		}
	}
	if v.root != "" && !strings.HasPrefix(key, v.root+"/") {
		return "", fmt.Errorf("not under the warehouse prefix %s", v.root)
	}
	return key, nil
}

// sessionName is an STS role session name for who (2 to 64 of
// [A-Za-z0-9+=,.@_-]), which shows in the storage provider's logs.
func sessionName(who string) string {
	out := []byte(who)
	for i, c := range out {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("+=,.@_-", c) >= 0) {
			out[i] = '_'
		}
	}
	if len(out) > 64 {
		out = out[:64]
	}
	return string(out)
}

type statement struct {
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// sessionPolicy allows reading the objects under prefix in bucket and
// listing them, and as access says writing and deleting them.
func sessionPolicy(partition, bucket, prefix string, access Access, kms bool) (string, error) {
	bucketARN := "arn:" + partition + ":s3:::" + bucket
	objects := []string{"s3:GetObject", "s3:GetObjectVersion"}
	keys := []string{"kms:Decrypt"}
	if access != ReadFiles {
		// PutObject covers multipart uploads.
		objects = append(objects, "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts")
		keys = append(keys, "kms:GenerateDataKey", "kms:Encrypt")
	}
	if access == WriteFiles {
		objects = append(objects, "s3:DeleteObject")
	}
	statements := []statement{
		{Effect: "Allow", Action: objects, Resource: []string{bucketARN + "/" + prefix + "/*"}},
		{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{bucketARN},
			Condition: map[string]map[string][]string{"StringLike": {"s3:prefix": {prefix + "/*"}}}},
		{Effect: "Allow", Action: []string{"s3:GetBucketLocation"}, Resource: []string{bucketARN}},
	}
	if kms {
		// The role's own policy names the key, if any.
		statements = append(statements, statement{Effect: "Allow", Action: keys, Resource: []string{"*"}})
	}
	b, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements})
	return string(b), err
}
