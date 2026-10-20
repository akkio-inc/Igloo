package federation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// AWS's Iceberg REST endpoints, AWS Glue's
// (https://glue.<region>.amazonaws.com/iceberg) and S3 Tables'
// (https://s3tables.<region>.amazonaws.com/iceberg), take requests signed
// with Signature Version 4 rather than bearer tokens. The proxy signs each
// request it forwards, with the connection's access key or AWS's default
// credentials, which DuckDB never sees either.

// emptyPayload is the SHA-256 of an empty body: the proxy sends only GETs.
const emptyPayload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// sign signs req for c's signing_name and region.
func (p *icebergProxy) sign(ctx context.Context, c Connection, req *http.Request) error {
	creds, err := p.awsCredentials(ctx, c)
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyPayload)
	return v4.NewSigner().SignHTTP(ctx, creds, req, emptyPayload, c.Iceberg.SigningName, c.Iceberg.Region, time.Now())
}

// awsCredentials returns the connection's access key, "key_id:secret" or
// "key_id:secret:session_token" in credential, or else AWS's default
// credentials, kept and renewed by the SDK.
func (p *icebergProxy) awsCredentials(ctx context.Context, c Connection) (aws.Credentials, error) {
	s := c.Iceberg
	if s.Credential != "" || s.CredentialSecret != "" {
		v, err := p.secretValue(c, s.Credential, s.CredentialSecret, "credential")
		if err != nil {
			return aws.Credentials{}, err
		}
		parts := strings.SplitN(strings.TrimSpace(v), ":", 3)
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			return aws.Credentials{}, errors.New("with signing_name, credential is an AWS access key, key_id:secret")
		}
		creds := aws.Credentials{AccessKeyID: parts[0], SecretAccessKey: parts[1], Source: "connection"}
		tokens.Store(parts[1], true)
		if len(parts) == 3 {
			creds.SessionToken = parts[2]
			tokens.Store(parts[2], true)
		}
		return creds, nil
	}
	p.mu.Lock()
	provider := p.aws
	p.mu.Unlock()
	if provider == nil {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s.Region))
		if err != nil {
			return aws.Credentials{}, fmt.Errorf("AWS's default credentials: %w", err)
		}
		provider = aws.NewCredentialsCache(cfg.Credentials)
		p.mu.Lock()
		p.aws = provider
		p.mu.Unlock()
	}
	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("AWS's default credentials: %w", err)
	}
	return creds, nil
}
