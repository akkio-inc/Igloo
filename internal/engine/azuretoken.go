package engine

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// DuckDB's credential chain for Azure asks the identity for a token as it opens a
// client, and on an Azure machine that is the instance metadata service, which
// allows a VM a few requests a second. A table of 128 partitions took a statement
// past that within a second, on a real account, and every one after it for a while:
// "Failed to get token from ChainedTokenCredential", from a 429. The chain also takes
// no client ID, so that a machine of two identities gets no token at all. So Igloo
// asks for the token, as often as it expires, and DuckDB uses that.

const (
	azureStorageScope = "https://storage.azure.com/.default"
	// azureTokenRetry is how soon it asks again after it could not get a token, and
	// the least it waits between tokens.
	azureTokenRetry = time.Minute
)

// newAzureCredential is the identity Igloo itself has on Azure, as its own client
// of the store does: AZURE_CLIENT_ID, Workload Identity, a managed identity.
var newAzureCredential = func() (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}

// azureTokenWait is how long after it was got a token should be replaced: half of
// what it has left, so that statements have long to finish on the one they began
// on.
func azureTokenWait(now, expires time.Time) time.Duration {
	return max(expires.Sub(now)/2, azureTokenRetry)
}

// azureTokenSecret is the statement that has DuckDB use token.
func azureTokenSecret(s config.Storage, token string) string {
	return secret(storageScope(s), "TYPE azure", "PROVIDER access_token", "ACCESS_TOKEN "+literal(token), "ACCOUNT_NAME "+literal(s.Azure.Account))
}

// startAzureTokens keeps DuckDB's secret for an Azure warehouse with no account key
// holding a token of Igloo's, until stopAzureTokens. The credential chain of
// storageStatements stands in until the first.
func (e *Engine) startAzureTokens(ctx context.Context, cfg config.Config, log *slog.Logger) {
	s := cfg.Storage
	if s.Type != config.StorageAzure || s.Azure.AccountKey != "" {
		return
	}
	cred, err := newAzureCredential()
	if err != nil {
		log.Warn("no Azure identity for DuckDB's tokens; it asks for them itself, which the metadata service limits", "err", err)
		return
	}
	stop, done := make(chan struct{}), make(chan struct{})
	e.stopAzureTokens = func() {
		close(stop)
		<-done
	}
	wait := e.renewAzureToken(ctx, cred, s, log)
	go func() {
		defer close(done)
		for {
			t := time.NewTimer(wait)
			select {
			case <-stop:
				t.Stop()
				return
			case <-t.C:
			}
			wait = e.renewAzureToken(context.Background(), cred, s, log)
		}
	}()
}

// renewAzureToken has DuckDB use a token of the identity's, and says how soon to
// renew it.
func (e *Engine) renewAzureToken(ctx context.Context, cred azcore.TokenCredential, s config.Storage, log *slog.Logger) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tok, err := cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{azureStorageScope}})
	if err != nil {
		log.Warn("could not get a token of the Azure identity for DuckDB; it goes on with what it has", "err", err)
		return azureTokenRetry
	}
	c, err := e.connector.Connect(ctx)
	if err == nil {
		defer c.Close() //nolint:errcheck
		_, err = c.(*duckdb.Conn).ExecContext(ctx, azureTokenSecret(s, tok.Token), nil)
	}
	if err != nil {
		// Never echo the statement, which DuckDB's error can: it holds the token.
		log.Warn("could not give DuckDB a token of the Azure identity; it goes on with what it has", "refused", !strings.Contains(err.Error(), "context"))
		return azureTokenRetry
	}
	return azureTokenWait(time.Now(), tok.ExpiresOn)
}
