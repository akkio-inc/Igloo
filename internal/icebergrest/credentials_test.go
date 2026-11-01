package icebergrest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/akkio-inc/igloo/internal/config"
)

// fakeSTS records AssumeRole calls and answers with credentials named after
// the call.
type fakeSTS struct {
	calls []*sts.AssumeRoleInput
	err   error
}

func (f *fakeSTS) assume(_ context.Context, in *sts.AssumeRoleInput) (*sts.AssumeRoleOutput, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sts.AssumeRoleOutput{Credentials: &types.Credentials{
		AccessKeyId:     aws.String("ASIA" + *in.RoleSessionName),
		SecretAccessKey: aws.String("secret"),
		SessionToken:    aws.String("token"),
		Expiration:      aws.Time(time.UnixMilli(1_800_000_000_000)),
	}}, nil
}

func newTestVendor(t *testing.T, endpoint, role string) (*S3Vendor, *fakeSTS, *[]string) {
	t.Helper()
	s := config.Storage{Type: config.StorageS3, Location: "s3://bucket/wh", Bucket: "bucket", Prefix: "wh",
		S3: config.S3{Region: "eu-west-1", Endpoint: endpoint, URLStyle: "path", UseSSL: true, AccessKeyID: "AKIAIGLOO", SecretAccessKey: "s", STSRoleARN: role}}
	v, err := NewS3Vendor(context.Background(), s, slog.New(slog.DiscardHandler))
	if err != nil || v == nil {
		t.Fatalf("vendor: %v, %v", v, err)
	}
	f := &fakeSTS{}
	v.assume = f.assume
	var heads []string
	v.head = func(_ context.Context, creds aws.Credentials, key string) (int, error) {
		heads = append(heads, creds.AccessKeyID+" "+key)
		return 403, nil
	}
	return v, f, &heads
}

func TestS3VendorNeedsARoleOnAWS(t *testing.T) {
	v, err := NewS3Vendor(context.Background(), config.Storage{Type: config.StorageS3, Bucket: "b", S3: config.S3{Region: "us-east-1"}},
		slog.New(slog.DiscardHandler))
	if v != nil || err != nil {
		t.Errorf("got %v, %v; want no vendor", v, err)
	}
}

func TestS3Vendor(t *testing.T) {
	v, f, heads := newTestVendor(t, "storage:9000", "")
	ctx := context.Background()

	creds, err := v.Vend(ctx, "s3://bucket/wh/0193/0194/", ReadFiles, "nora")
	if err != nil {
		t.Fatal(err)
	}
	if creds.Prefix != "s3://bucket/wh/0193/0194" {
		t.Errorf("prefix %q", creds.Prefix)
	}
	for k, want := range map[string]string{
		"s3.access-key-id":               "ASIAigloo-nora",
		"s3.secret-access-key":           "secret",
		"s3.session-token":               "token",
		"s3.session-token-expires-at-ms": "1800000000000",
		"s3.endpoint":                    "https://storage:9000",
		"s3.path-style-access":           "true",
		"client.region":                  "eu-west-1",
		"s3.region":                      "eu-west-1",
	} {
		if creds.Config[k] != want {
			t.Errorf("%s = %q, want %q", k, creds.Config[k], want)
		}
	}

	// The first credentials are checked against the store once: some for a
	// scratch prefix must be refused a key outside it.
	if len(f.calls) != 2 || *f.calls[0].RoleSessionName != "igloo-check" || *f.calls[0].RoleArn != compatRoleARN ||
		len(*heads) != 1 || (*heads)[0] != "ASIAigloo-check wh/.igloo-credential-check-outside" {
		t.Fatalf("check: %d calls, heads %v", len(f.calls), *heads)
	}
	if _, err := v.Vend(ctx, "s3://bucket/wh/0193/0195", WriteFiles, "walt"); err != nil || len(f.calls) != 3 || len(*heads) != 1 {
		t.Errorf("second vend: %v, %d calls", err, len(f.calls))
	}
	if d := *f.calls[1].DurationSeconds; d != 3600 {
		t.Errorf("duration %d", d)
	}

	var read, write struct {
		Statement []statement
	}
	_ = json.Unmarshal([]byte(*f.calls[1].Policy), &read)
	_ = json.Unmarshal([]byte(*f.calls[2].Policy), &write)
	if got := read.Statement[0]; strings.Join(got.Action, ",") != "s3:GetObject,s3:GetObjectVersion" ||
		got.Resource[0] != "arn:aws:s3:::bucket/wh/0193/0194/*" {
		t.Errorf("read policy: %s", *f.calls[1].Policy)
	}
	if got := read.Statement[1]; got.Action[0] != "s3:ListBucket" || got.Resource[0] != "arn:aws:s3:::bucket" ||
		got.Condition["StringLike"]["s3:prefix"][0] != "wh/0193/0194/*" {
		t.Errorf("list statement: %s", *f.calls[1].Policy)
	}
	if !strings.Contains(*f.calls[2].Policy, `"s3:PutObject"`) || !strings.Contains(*f.calls[2].Policy, `"s3:DeleteObject"`) ||
		strings.Contains(*f.calls[1].Policy, "PutObject") {
		t.Errorf("write policy: %s", *f.calls[2].Policy)
	}
	// A table whose files clones read: written, never deleted.
	if _, err := v.Vend(ctx, "s3://bucket/wh/0193/0196", AddFiles, "walt"); err != nil {
		t.Fatal(err)
	}
	if p := *f.calls[3].Policy; !strings.Contains(p, `"s3:PutObject"`) || strings.Contains(p, "DeleteObject") {
		t.Errorf("write policy without deletes: %s", p)
	}
	if strings.Contains(*f.calls[1].Policy, "kms:") {
		t.Errorf("S3-compatible stores have no KMS: %s", *f.calls[1].Policy)
	}

	// Locations outside the warehouse, or that would widen the policy, get
	// none.
	for _, loc := range []string{
		"s3://other/wh/t", "s3://bucket/elsewhere/t", "s3://bucket/wh", "s3://bucket/wh/*", "s3://bucket/wh/a/../../x",
		"s3://bucket/wh/${aws:username}", "s3://bucket/wh/t?x=1", "gs://bucket/wh/t", "s3://bucket/wh//t", "s3://bucket/wh/t%2A",
		// Distributed execution's exchange, whatever a table's metadata says.
		"s3://bucket/wh/_igloo_shuffle", "s3://bucket/wh/_igloo_shuffle/q1/0",
	} {
		if _, err := v.Vend(ctx, loc, ReadFiles, "nora"); !errors.Is(err, ErrNoCredentials) {
			t.Errorf("%s: got %v", loc, err)
		}
	}

	f.err = errors.New("throttled")
	if _, err := v.Vend(ctx, "s3://bucket/wh/0193/0194", ReadFiles, "nora"); err == nil || errors.Is(err, ErrNoCredentials) {
		t.Errorf("STS failure: %v", err)
	}
}

func TestS3VendorRefusesStoresThatIgnoreThePolicy(t *testing.T) {
	v, f, _ := newTestVendor(t, "storage:9000", "")
	v.head = func(context.Context, aws.Credentials, string) (int, error) { return 404, nil }
	for range 2 {
		if _, err := v.Vend(context.Background(), "s3://bucket/wh/t", ReadFiles, "nora"); !errors.Is(err, ErrNoCredentials) {
			t.Fatalf("got %v", err)
		}
	}
	if len(f.calls) != 1 {
		t.Errorf("checked %d times, want once", len(f.calls))
	}
}

// TestStatementDirectoryPolicy checks the credentials workers write a
// statement's files with: a table's directory of the statement's alone, no
// right to delete, as AddFiles is for other engines.
func TestStatementDirectoryPolicy(t *testing.T) {
	prefix := "warehouse/ns/t/data/igloo-write-0123456789abcdef"
	if got, err := (&S3Vendor{bucket: "b", root: "warehouse"}).tablePrefix("s3://b/" + prefix); err != nil || got != prefix {
		t.Fatalf("prefix %q, %v", got, err)
	}
	policy, err := sessionPolicy("aws", "b", prefix, AddFiles, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(policy, "DeleteObject") {
		t.Errorf("may delete: %s", policy)
	}
	if !strings.Contains(policy, `"arn:aws:s3:::b/`+prefix+`/*"`) || !strings.Contains(policy, `"`+prefix+`/*"`) || strings.Count(policy, "warehouse/") != 2 {
		t.Errorf("reaches more than the directory: %s", policy)
	}
	if !strings.Contains(policy, `"Effect":"Deny","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::b/*..*"`) {
		t.Errorf("may write keys with .. in them: %s", policy)
	}
	// As long as a statement's attempt's directory gets, in a long bucket's name: STS
	// takes at most 2,048 characters.
	long := "warehouse/01a198da-29a4-7ef0-b1d4-615134765204/data/igloo-write-0123456789abcdef/0123456789abcdef0123456789abcdef-2/s12-3456/a12/f"
	if policy, err := sessionPolicy("aws", strings.Repeat("b", 63), long, AddFiles, true); err != nil || len(policy) > 2048 {
		t.Errorf("%d characters of policy, %v", len(policy), err)
	}
}

func TestS3VendorOnAWS(t *testing.T) {
	v, f, _ := newTestVendor(t, "", "arn:aws-us-gov:iam::123456789012:role/igloo-engines")
	creds, err := v.Vend(context.Background(), "s3://bucket/wh/t", WriteFiles, "a_very_long_user_name_that_goes_on_and_on_and_on_beyond_limits")
	if err != nil {
		t.Fatal(err)
	}
	in := f.calls[1]
	if *in.RoleArn != "arn:aws-us-gov:iam::123456789012:role/igloo-engines" || len(*in.RoleSessionName) != 64 {
		t.Errorf("assumed %s as %s", *in.RoleArn, *in.RoleSessionName)
	}
	if !strings.Contains(*in.Policy, `"arn:aws-us-gov:s3:::bucket/wh/t/*"`) || !strings.Contains(*in.Policy, `"kms:GenerateDataKey"`) {
		t.Errorf("policy: %s", *in.Policy)
	}
	if _, ok := creds.Config["s3.endpoint"]; ok {
		t.Errorf("AWS needs no endpoint: %v", creds.Config)
	}
}
