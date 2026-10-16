package policy

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

var (
	maskEmail = Policy{Kind: Masking, Name: "mask_email", Args: []Arg{{"val", "VARCHAR"}}, Returns: "VARCHAR",
		Body: "CASE WHEN is_role_in_session('pii') THEN val ELSE '***' END"}
	maskID   = Policy{Kind: Masking, Name: "mask_id", Args: []Arg{{"val", "INTEGER"}}, Returns: "INTEGER", Body: "0"}
	byClient = Policy{Kind: RowAccess, Name: "by_client", Args: []Arg{{"cid", "VARCHAR"}}, Returns: "BOOLEAN",
		Body: "cid = 'acme'"}
)

// testStore checks a Store that starts empty.
func testStore(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	update := func(change func(*Catalog) error) error { return s.Update(ctx, change) }
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	v0, err := s.Version(ctx)
	must(err)
	for _, p := range []Policy{maskEmail, maskID, byClient} {
		must(update(func(c *Catalog) error { return c.PutPolicy(p, false, now) }))
	}
	if err := update(func(c *Catalog) error { return c.PutPolicy(maskEmail, false, now) }); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate policy: %v", err)
	}
	other := byClient
	other.Name = "mask_email"
	if err := update(func(c *Catalog) error { return c.PutPolicy(other, true, now) }); !errors.Is(err, ErrExists) {
		t.Errorf("replacing a policy of another kind: %v", err)
	}
	if v, _ := s.Version(ctx); v != v0+3 {
		t.Errorf("version %d after three changes from %d; failed changes must not count", v, v0)
	}

	must(update(func(c *Catalog) error {
		return c.SetMask(ColumnMask{Schema: "crm", Table: "Contacts", Column: "Email", Policy: "mask_email"}, false, now)
	}))
	if err := update(func(c *Catalog) error {
		return c.SetMask(ColumnMask{Schema: "CRM", Table: "contacts", Column: "email", Policy: "mask_id"}, false, now)
	}); !errors.Is(err, ErrExists) {
		t.Errorf("a second mask without FORCE: %v", err)
	}
	if err := update(func(c *Catalog) error {
		return c.SetMask(ColumnMask{Schema: "crm", Table: "contacts", Column: "id", Policy: "by_client"}, false, now)
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("a row access policy as a mask: %v", err)
	}
	must(update(func(c *Catalog) error {
		return c.AddRowFilter(RowFilter{Schema: "crm", Table: "contacts", Policy: "by_client", Columns: []string{"client_id"}}, now)
	}))
	if err := update(func(c *Catalog) error {
		return c.AddRowFilter(RowFilter{Schema: "crm", Table: "CONTACTS", Policy: "by_client", Columns: []string{"x"}}, now)
	}); !errors.Is(err, ErrExists) {
		t.Errorf("a second row access policy: %v", err)
	}
	if err := update(func(c *Catalog) error {
		return c.AddRowFilter(RowFilter{Schema: "crm", Table: "other", Policy: "by_client", Columns: []string{"a", "b"}}, now)
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("wrong number of columns: %v", err)
	}

	// Policies in use can be neither dropped nor given another signature.
	if err := update(func(c *Catalog) error { return c.DropPolicy(Masking, "mask_email") }); !errors.Is(err, ErrInUse) {
		t.Errorf("dropping a policy in use: %v", err)
	}
	changed := maskEmail
	changed.Args = []Arg{{"val", "INTEGER"}}
	changed.Returns = "INTEGER"
	if err := update(func(c *Catalog) error { return c.PutPolicy(changed, true, now) }); !errors.Is(err, ErrInUse) {
		t.Errorf("changing the signature of a policy in use: %v", err)
	}
	body := maskEmail
	body.Body = "'redacted'"
	must(update(func(c *Catalog) error { return c.PutPolicy(body, true, now.Add(time.Second)) }))

	// Tags.
	must(update(func(c *Catalog) error {
		return c.PutTag(Tag{Name: "pii", AllowedValues: []string{"email", "ip"}}, false, now)
	}))
	if err := update(func(c *Catalog) error {
		return c.SetColumnTag(ColumnTag{Schema: "crm", Table: "events", Column: "ua", Tag: "pii", Value: "user_agent"}, now)
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a value the tag does not allow: %v", err)
	}
	must(update(func(c *Catalog) error {
		return c.SetColumnTag(ColumnTag{Schema: "crm", Table: "events", Column: "ip", Tag: "pii", Value: "ip"}, now)
	}))
	must(update(func(c *Catalog) error { return c.SetTagPolicy("pii", "mask_email") }))
	two := maskEmail
	two.Name = "mask_email_2"
	must(update(func(c *Catalog) error { return c.PutPolicy(two, false, now) }))
	if err := update(func(c *Catalog) error { return c.SetTagPolicy("pii", "mask_email_2") }); !errors.Is(err, ErrExists) {
		t.Errorf("two tag policies for one type: %v", err)
	}
	must(update(func(c *Catalog) error { return c.SetTagPolicy("pii", "mask_id") }))
	if err := update(func(c *Catalog) error { return c.DropTag("pii") }); !errors.Is(err, ErrInUse) {
		t.Errorf("dropping a tag in use: %v", err)
	}

	snap, err := s.Load(ctx)
	must(err)
	if snap.Empty() || !snap.ProtectedNames()["contacts"] || !snap.ProtectedNames()["events"] {
		t.Fatalf("protected names: %v", snap.ProtectedNames())
	}
	r := snap.Rules("CRM", "contacts")
	if r == nil || r.FilterPolicy == nil || r.FilterPolicy.Name != "by_client" || r.Filter.Columns[0] != "client_id" {
		t.Fatalf("rules of crm.contacts: %+v", r)
	}
	if col := r.Column("EMAIL"); !col.Masked() || col.Mask.Body != "'redacted'" {
		t.Errorf("email column: %+v", col)
	}
	ip := snap.Rules("crm", "events").Column("ip")
	if !ip.Masked() || ip.Mask != nil || len(ip.TagMasks) != 2 || ip.TagMasks[0].Value != "ip" {
		t.Errorf("tagged column: %+v", ip)
	}
	if snap.Rules("crm", "nothing") != nil {
		t.Error("an unprotected table has rules")
	}
	// The table CREATE OR REPLACE TABLE writes first has the rules of the
	// one it replaces.
	if snap.Rules("crm", "Contacts__igloo_new_0a1b2c3d") != r {
		t.Error("a staging table is unprotected")
	}

	// A tag without masking policies protects nothing.
	must(update(func(c *Catalog) error { return c.UnsetTagPolicy("pii", "mask_email") }))
	must(update(func(c *Catalog) error { return c.UnsetTagPolicy("pii", "mask_id") }))
	must(update(func(c *Catalog) error { return c.UnsetMask("", "crm", "contacts", "email") }))
	must(update(func(c *Catalog) error { return c.DropRowFilter("", "crm", "contacts", "") }))
	if err := update(func(c *Catalog) error { return c.DropRowFilter("", "crm", "contacts", "") }); !errors.Is(err, ErrNotFound) {
		t.Errorf("dropping a missing row filter: %v", err)
	}
	snap, err = s.Load(ctx)
	must(err)
	if !snap.Empty() {
		t.Errorf("still protected: %v", snap.ProtectedNames())
	}
	if tagged := snap.Tagged("crm", "events"); tagged == nil || len(tagged.Column("ip").Tags) != 1 {
		t.Errorf("tags are kept: %+v", tagged)
	}
	must(update(func(c *Catalog) error { return c.UnsetColumnTag("", "crm", "events", "IP", "pii") }))
	must(update(func(c *Catalog) error { return c.DropTag("pii") }))
	must(update(func(c *Catalog) error { return c.DropPolicy(Masking, "mask_email") }))
	if err := update(func(c *Catalog) error { return c.DropPolicy(Masking, "by_client") }); !errors.Is(err, ErrNotFound) {
		t.Errorf("dropping a policy as the wrong kind: %v", err)
	}
}

// TestConnectionTables checks that attachments to a table of a connection
// apply to that table only, not to a table of the Iceberg catalog of the
// same schema and name, nor the other way round.
func TestConnectionTables(t *testing.T) {
	now := time.Now()
	var c Catalog
	mask := Policy{Kind: Masking, Name: "hide", Args: []Arg{{Name: "v", Type: "VARCHAR"}}, Returns: "VARCHAR", Body: "'x'"}
	rows := Policy{Kind: RowAccess, Name: "none", Args: []Arg{{Name: "v", Type: "INTEGER"}}, Returns: "BOOLEAN", Body: "false"}
	for _, p := range []Policy{mask, rows} {
		if err := c.PutPolicy(p, false, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SetMask(ColumnMask{Catalog: "crm", Schema: "sales", Table: "customers", Column: "email", Policy: "hide"}, false, now); err != nil {
		t.Fatal(err)
	}
	if err := c.AddRowFilter(RowFilter{Catalog: "crm", Schema: "sales", Table: "customers", Policy: "none", Columns: []string{"id"}}, now); err != nil {
		t.Fatal(err)
	}
	// The same names in the Iceberg catalog are another table.
	if err := c.AddRowFilter(RowFilter{Schema: "sales", Table: "customers", Policy: "none", Columns: []string{"id"}}, now); err != nil {
		t.Errorf("a row filter on the Iceberg table of the same name: %v", err)
	}
	snap := NewSnapshot(1, c)
	r := snap.ConnectionRules("CRM", "Sales", "customers")
	if r == nil || r.Catalog != "crm" || r.Filter == nil || !r.Column("email").Masked() {
		t.Fatalf("rules of crm.sales.customers: %+v", r)
	}
	if lake := snap.Rules("sales", "customers"); lake == nil || lake.Column("email").Masked() {
		t.Errorf("the Iceberg table has the connection table's mask: %+v", lake)
	}
	if snap.ConnectionRules("ops", "sales", "customers") != nil || snap.ConnectionRules("", "sales", "customers") != nil {
		t.Error("rules for another connection")
	}
	if !snap.ProtectedNames()["customers"] {
		t.Error("the table's name is not protected")
	}
	if err := c.UnsetMask("", "sales", "customers", "email"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unsetting the connection table's mask on the Iceberg table: %v", err)
	}
	if err := c.UnsetMask("crm", "sales", "customers", "email"); err != nil {
		t.Error(err)
	}
	if err := c.DropPolicy(RowAccess, "none"); !errors.Is(err, ErrInUse) || !strings.Contains(err.Error(), "crm.sales.customers") {
		t.Errorf("dropping a policy in use on a connection table: %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	testStore(t, NewMemoryStore())
}

// TestPostgresStore runs against a real database when
// IGLOO_TEST_DATABASE_URL is set (make test starts one).
func TestPostgresStore(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, `UPDATE igloo_policy.catalog SET doc = '{}'`); err != nil {
		t.Fatal(err)
	}
	// Migrations are idempotent, as several replicas run them.
	if s2, err := OpenPostgres(ctx, url); err != nil {
		t.Fatal(err)
	} else {
		s2.Close()
	}
	testStore(t, s)
}
