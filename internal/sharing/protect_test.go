package sharing

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xdg-go/scram"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// TestProtectedTables attaches a policy to a shared table: from then on
// recipients get nothing of it, not even files listed before, and the
// history says which policies stopped them.
func TestProtectedTables(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tok := f.token
	_, _, body := f.call("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}")
	fileURL := ndjson(t, body)[2]["file"].(map[string]any)["url"].(string)

	f.policies["public.campaigns"] = []string{"client_rows", "mask_email"}
	for _, req := range [][2]string{
		{"GET", "/shares/clients/schemas/public/tables/campaigns/version"},
		{"HEAD", "/shares/clients/schemas/public/tables/campaigns"},
		{"GET", "/shares/clients/schemas/public/tables/campaigns/metadata"},
		{"POST", "/shares/clients/schemas/public/tables/campaigns/query"},
	} {
		status, _, got := f.call(req[0], req[1], tok, "{}")
		if status != 403 {
			t.Errorf("%s %s: %d %s, want 403", req[0], req[1], status, got)
		}
		if req[0] == "HEAD" {
			continue
		}
		if !strings.Contains(got, `"errorCode":"PERMISSION_DENIED"`) || !strings.Contains(got, "masking or row access policies") {
			t.Errorf("%s %s: %s does not say why", req[0], req[1], got)
		}
		if strings.Contains(got, "mask_email") || strings.Contains(got, "client_rows") {
			t.Errorf("%s %s: the recipient is told the provider's policy names: %s", req[0], req[1], got)
		}
	}
	status, _, got := f.call("GET", fileURL, "", "")
	if status != 403 || !strings.Contains(got, "masking or row access policies") {
		t.Errorf("a file listed before the policy: %d %s", status, got)
	}
	status, _, _ = f.call("HEAD", fileURL, "", "")
	if status != 403 {
		t.Errorf("HEAD of a file listed before the policy: %d", status)
	}
	// The share still lists the table, and its other tables are served.
	f.expect("GET", "/shares/clients/all-tables", tok, "", 200, `"name":"campaigns"`)
	f.expect("GET", "/shares/clients/schemas/reports/tables/secret/version", tok, "", 200, "")

	qs, err := f.audit.Queries(ctx, audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	refused := 0
	for _, q := range qs {
		if slices.Equal(q.Policies, []string{"client_rows", "mask_email"}) {
			refused++
			if q.Outcome != audit.OutcomeDenied || q.SQLState != "42501" || !slices.Equal(q.TablesRead, []string{"lake.public.campaigns"}) ||
				q.User != "recipient:acme" {
				t.Errorf("history record of a refusal: %+v", q)
			}
		}
	}
	if refused != 6 {
		t.Errorf("%d refusals in the history, want the 4 requests and 2 downloads", refused)
	}

	// Without the policy, the table is served again.
	delete(f.policies, "public.campaigns")
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}", 200, `"file"`)
	f.expect("GET", fileURL, "", "", 200, "PAR1")

	// Until Igloo can tell which tables have policies, it serves none.
	f.server.SetProtection(nil)
	f.expect("GET", "/shares/clients/schemas/public/tables/campaigns/version", tok, "", 503, "cannot check")
	f.expect("GET", fileURL, "", "", 503, "cannot check")
	f.expect("GET", "/shares/clients/all-tables", tok, "", 200, `"name":"campaigns"`)
}

// TestAddProtectedTable refuses to share a table with policies.
func TestAddProtectedTable(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	svc := &Service{store: f.store, source: f.src, server: f.server, log: slog.New(slog.DiscardHandler)}
	add := SharedTable{Share: "internal", Schema: "copies", Name: "campaigns", SourceSchema: "public", SourceTable: "campaigns", AddedBy: "igloo"}

	f.policies["public.campaigns"] = []string{"client_rows", "pii_hash"}
	err := svc.AddTable(ctx, add)
	if !IsProtected(err) || !strings.Contains(err.Error(), "public.campaigns has masking or row access policies (client_rows, pii_hash)") {
		t.Errorf("sharing a protected table: %v", err)
	}
	tables, _ := f.store.ShareTables(ctx, "internal")
	if len(tables) != 1 {
		t.Errorf("a protected table was added: %+v", tables)
	}

	f.server.SetProtection(nil)
	if err := svc.AddTable(ctx, add); err == nil || IsProtected(err) {
		t.Errorf("sharing while policies cannot be checked: %v", err)
	}

	f.server.SetProtection(func(context.Context, string, string) []string { return nil })
	if err := svc.AddTable(ctx, add); err != nil {
		t.Errorf("sharing a table without policies: %v", err)
	}
}

// TestRecipientAndAccessTokens keeps recipients' bearer tokens and Igloo's
// access tokens apart: neither has the other's form, and the sharing
// endpoint refuses access tokens. (SQL, Flight SQL and the console refusing
// recipients' tokens is tested in package pgwire.)
func TestRecipientAndAccessTokens(t *testing.T) {
	f := newFixture(t)
	for _, prefix := range []string{tokenPrefix, activationPrefix} {
		tok, err := newToken(prefix)
		if err != nil {
			t.Fatal(err)
		}
		if auth.LooksLikeToken(tok.Text) {
			t.Errorf("%s looks like an Igloo access token", tok.Text)
		}
		if strings.HasPrefix(tok.Text, auth.TokenPrefix) {
			t.Errorf("%s has the access tokens' prefix", tok.Text)
		}
	}
	if auth.LooksLikeToken(f.token) {
		t.Errorf("the recipient's token %s looks like an access token", f.token)
	}

	access, _, _, err := auth.NewToken("acme", "ci", "igloo", time.Now().Add(time.Hour), scram.KeyFactors{Salt: "0123456789abcdef", Iters: 4096})
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{tokenPrefix, activationPrefix} {
		if _, _, ok := parseToken(prefix, access); ok {
			t.Errorf("an access token parses as a %s token", prefix)
		}
	}
	f.expect("GET", "/shares", access, "", 401, "missing or invalid bearer token")
	f.expect("GET", "/shares", "Bearer "+access, "", 401, "")
	f.expect("POST", "/activate/"+access, "", "", 404, "")
	// And a recipient's token is not a recipient's activation code.
	f.expect("POST", "/activate/"+f.token, "", "", 404, "")
}
