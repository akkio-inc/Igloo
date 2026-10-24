package pgwire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// idTest is a replica with ID domains on, and a way to read what users
// may not: the dictionaries and the columns of ids.
type idTest struct {
	t     *testing.T
	vc    *viewCatalog
	r     *viewReplica
	admin *pgx.Conn
}

func newIDTest(t *testing.T) *idTest {
	t.Helper()
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) {
		o.IDDomainsOn = true
		o.Tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	})
	it := &idTest{t: t, vc: vc, r: r, admin: r.admin(t, pgx.QueryExecModeSimpleProtocol)}
	return it
}

func (it *idTest) exec(qs ...string) {
	it.t.Helper()
	for _, q := range qs {
		mustExec(it.t, it.admin, q)
	}
}

// internal runs q on a connection of the engine's own, past every check.
func (it *idTest) internal(q string) []string {
	it.t.Helper()
	ctx := context.Background()
	conn, err := it.r.eng.Connect(ctx)
	if err != nil {
		it.t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	rows, err := queryRows(ctx, conn, q)
	if err != nil {
		it.t.Fatalf("%s: %v", q, err)
	}
	var out []string
	for _, row := range rows {
		parts := make([]string, len(row))
		for i, v := range row {
			parts[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func (it *idTest) one(q string) string {
	it.t.Helper()
	got := it.internal(q)
	if len(got) != 1 {
		it.t.Fatalf("%s: %q", q, got)
	}
	return got[0]
}

// id returns value's id in table's column person_id, "<nil>" for none.
func (it *idTest) id(table, value string) string {
	it.t.Helper()
	return it.one("SELECT CAST(max(_igloo_id_person_id) AS VARCHAR) FROM lake.public." + table + " WHERE person_id = " + quoteLiteral(value))
}

// consistent checks that every id in the tables is the dictionary's for its
// value, that the dictionary gives each value one id and each id one value,
// densely from 0, and that no row with a value lacks an id when complete.
func (it *idTest) consistent(domain string, complete bool, tables ...string) {
	it.t.Helper()
	dict := "lake." + iddomain.DictionarySchema + "." + domain
	if got := it.one("SELECT count(*) = count(DISTINCT value) AND count(*) = count(DISTINCT id) AND coalesce(max(id), -1) = count(*) - 1 FROM " + dict); got != "true" {
		it.t.Errorf("the dictionary of %s is not one id a value, densely: %q", domain, it.internal("SELECT value, id FROM "+dict+" ORDER BY id"))
	}
	for _, table := range tables {
		tbl := "lake.public." + table
		if got := it.one("SELECT count(*) FROM " + tbl + " AS t JOIN " + dict + " AS d ON d.value = t.person_id WHERE t._igloo_id_person_id <> d.id"); got != "0" {
			it.t.Errorf("%s: %s rows have another id than the dictionary's", table, got)
		}
		if got := it.one("SELECT count(*) FROM " + tbl + " AS t WHERE t._igloo_id_person_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + dict + " AS d WHERE d.id = t._igloo_id_person_id)"); got != "0" {
			it.t.Errorf("%s: %s rows have an id the dictionary does not", table, got)
		}
		if complete {
			if got := it.one("SELECT count(*) FROM " + tbl + " WHERE person_id IS NOT NULL AND _igloo_id_person_id IS NULL"); got != "0" {
				it.t.Errorf("%s: %s rows with a value have no id", table, got)
			}
		}
	}
}

// TestIDAssignment gives values ids as Igloo writes member tables, and
// maps the rows other writes leave without, checking each time that ids
// agree with the dictionary across tables.
func TestIDAssignment(t *testing.T) {
	it := newIDTest(t)
	ctx := context.Background()
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people AS SELECT 'p' || i AS person_id, CAST(i AS INTEGER) AS n FROM range(10) r(i)",
		"CREATE TABLE lake.public.events AS SELECT 'p' || (i % 20) AS person_id, CAST(i AS INTEGER) AS t FROM range(5) r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.events ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"ALTER TABLE lake.public.events ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv := it.r.srv

	// Rows from before the column joined get ids from maintenance.
	if got := it.one("SELECT count(_igloo_id_person_id) FROM lake.public.people"); got != "0" {
		t.Fatalf("ids before mapping: %s", got)
	}
	srv.MapIDDomains(ctx)
	it.consistent("people", true, "people", "events")
	if got := it.one("SELECT count(*) FROM lake.igloo_domains.people"); got != "10" {
		t.Errorf("values in the dictionary: %s", got)
	}

	// An INSERT writes the ids the dictionary has, and gives new values
	// theirs before it returns.
	p3 := it.id("people", "p3")
	it.exec("INSERT INTO lake.public.people VALUES ('p3', 33), ('new1', 1)",
		"INSERT INTO lake.public.people SELECT 'p' || i, CAST(i AS INTEGER) FROM range(5, 16) r(i)")
	if got := it.one("SELECT count(DISTINCT _igloo_id_person_id) FROM lake.public.people WHERE person_id = 'p3'"); got != "1" || it.id("people", "p3") != p3 {
		t.Errorf("p3's ids: %s distinct, %s, was %s", got, it.id("people", "p3"), p3)
	}
	it.consistent("people", true, "people", "events")
	if got := it.one("SELECT count(*) FROM lake.igloo_domains.people"); got != "17" {
		t.Errorf("values in the dictionary: %s", got)
	}

	// CREATE OR REPLACE of a member table makes a member table, filled
	// with ids.
	it.exec("CREATE OR REPLACE TABLE lake.public.events AS SELECT 'p' || (i % 20) AS person_id, CAST(i AS INTEGER) AS t FROM range(100) r(i)")
	it.consistent("people", true, "people", "events")
	if got := it.id("events", "p3"); got != p3 {
		t.Errorf("p3's id in the new events: %s, want %s", got, p3)
	}

	// COPY INTO a member table.
	dir := filepath.Join(strings.TrimPrefix(it.vc.cfg.Storage.Location, "file://"), "drops")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "people.csv"), []byte("person_id,n\np1,100\ncopy1,101\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	it.exec("COPY INTO lake.public.people FROM '" + dir + "/people.csv'")
	it.consistent("people", true, "people", "events")
	if it.id("people", "copy1") == "<nil>" {
		t.Error("a value COPY INTO loaded has no id")
	}

	// In a transaction block, values the dictionary has get their ids;
	// new ones wait for maintenance. So do other engines' writes, and the
	// rows an UPDATE of the member column leaves without.
	p1 := it.id("people", "p1")
	it.exec("BEGIN", "INSERT INTO lake.public.people VALUES ('tx1', 1), ('p1', 2)", "COMMIT")
	it.internal("INSERT INTO lake.public.people (person_id, n) VALUES ('raw1', 0), ('p2', 0)")
	it.exec("UPDATE lake.public.people SET person_id = 'p4' WHERE person_id = 'new1'")
	if got := it.one("SELECT count(*) FROM lake.public.people WHERE person_id = 'p1' AND _igloo_id_person_id = " + p1); got != "3" {
		t.Errorf("rows of p1 with its id: %s", got)
	}
	if got := it.one("SELECT count(*) FROM lake.public.people WHERE _igloo_id_person_id IS NULL"); got != "4" {
		t.Errorf("rows without ids: %s, want tx1's, raw1's, p2's and the updated one", got)
	}
	it.consistent("people", false, "people", "events")
	srv.MapIDDomains(ctx)
	it.consistent("people", true, "people", "events")

	// Maintenance leaves a table it found mapped alone until it changes.
	if got := it.one("SELECT count(*) FROM lake.public.people"); got == "" {
		t.Fatal("no rows")
	}
	srv.MapIDDomains(ctx)
	commits := it.vc.fake.Commits()
	srv.MapIDDomains(ctx)
	if it.vc.fake.Commits() != commits {
		t.Error("mapping tables found mapped committed")
	}
}

// TestIDAssignmentLease leaves new values' ids to maintenance while
// another holds their domain's lease, without holding up the write or
// writes in other domains, and stops handing out ids below 2^32.
func TestIDAssignmentLease(t *testing.T) {
	saved := idLeaseWait
	idLeaseWait = 200 * time.Millisecond
	defer func() { idLeaseWait = saved }()
	it := newIDTest(t)
	ctx := context.Background()
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people (person_id VARCHAR, n INTEGER)",
		"CREATE TABLE lake.public.devices (person_id VARCHAR, n INTEGER)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.devices ALTER COLUMN person_id SET TAG pii = 'device_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"CREATE ID DOMAIN devices (VARCHAR) TAG pii = 'device_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"ALTER TABLE lake.public.devices ALTER COLUMN person_id SET ID DOMAIN devices",
		"INSERT INTO lake.public.people VALUES ('p1', 1)",
	)
	srv := it.r.srv
	p1 := it.id("people", "p1")
	unlock, err := srv.idDomains.Lock(ctx, "people", "someone else", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	it.exec("INSERT INTO lake.public.people VALUES ('busy1', 1), ('p1', 2)")
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("a write waited %v for a busy lease", took)
	}
	if it.id("people", "busy1") != "<nil>" || it.one("SELECT count(*) FROM lake.public.people WHERE person_id = 'p1' AND _igloo_id_person_id = "+p1) != "2" {
		t.Errorf("with the lease busy: busy1 %s, p1 rows %q", it.id("people", "busy1"), it.internal("SELECT * FROM lake.public.people"))
	}
	// Another domain's writes do not wait for it.
	start = time.Now()
	it.exec("INSERT INTO lake.public.devices VALUES ('d1', 1)")
	if took := time.Since(start); took > idLeaseWait {
		t.Errorf("a write of another domain took %v", took)
	}
	if it.id("devices", "d1") == "<nil>" {
		t.Error("a write of another domain got no id")
	}
	unlock()
	srv.MapIDDomains(ctx)
	it.consistent("people", true, "people")

	// A domain with two ids left gives one to a new value, and none to
	// two more, which would need more than it has left: ids never wrap.
	next, gen, err := srv.idDomains.Next(ctx, "devices")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.idDomains.Allocate(ctx, "devices", gen, iddomain.MaxIDs-next-2); err != nil {
		t.Fatal(err)
	}
	it.exec("INSERT INTO lake.public.devices VALUES ('d2', 2)")
	if got := it.id("devices", "d2"); got != fmt.Sprint(iddomain.MaxIDs-2) {
		t.Errorf("the last but one id: %s, want %d", got, iddomain.MaxIDs-2)
	}
	it.exec("INSERT INTO lake.public.devices VALUES ('d3', 3), ('d4', 4)")
	if it.id("devices", "d3") != "<nil>" || it.id("devices", "d4") != "<nil>" {
		t.Errorf("ids past the last: %s, %s", it.id("devices", "d3"), it.id("devices", "d4"))
	}
	if got := it.one("SELECT max(_igloo_id_person_id) < 4294967296 FROM lake.public.devices"); got != "true" {
		t.Errorf("past the limit: %q", it.internal("SELECT * FROM lake.public.devices"))
	}
	if got := it.one("SELECT count(*) = count(DISTINCT value) AND count(*) = count(DISTINCT id) FROM lake.igloo_domains.devices"); got != "true" {
		t.Errorf("the dictionary of devices: %q", it.internal("SELECT value, id FROM lake.igloo_domains.devices"))
	}
}

// TestIDAssignmentConcurrentWriters writes two tables of one domain at
// once, many times over, with values both writers find new: each value gets
// one id, which both tables hold.
func TestIDAssignmentConcurrentWriters(t *testing.T) {
	it := newIDTest(t)
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people (person_id VARCHAR, n INTEGER)",
		"CREATE TABLE lake.public.events (person_id VARCHAR, n INTEGER)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.events ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"ALTER TABLE lake.public.events ALTER COLUMN person_id SET ID DOMAIN people",
	)
	const rounds = 8
	var wg sync.WaitGroup
	errs := make(chan error, 2*rounds)
	for _, table := range []string{"people", "events"} {
		conn := it.r.admin(t, pgx.QueryExecModeSimpleProtocol)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range rounds {
				// Values 0-29 of the round's range, which the other writer
				// writes too, and some of this writer's own.
				q := fmt.Sprintf("INSERT INTO lake.public.%s SELECT 'c' || (%d + i), CAST(i AS INTEGER) FROM range(30) r(i) "+
					"UNION ALL SELECT '%s' || i || '_%d', 0 FROM range(5) r(i)", table, round*20, table, round)
				if _, err := conn.Exec(context.Background(), q); err != nil {
					errs <- fmt.Errorf("%s: %w", table, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	it.r.srv.MapIDDomains(context.Background())
	it.consistent("people", true, "people", "events")
	if got := it.one("SELECT count(*) FROM lake.igloo_domains.people"); got != fmt.Sprint((rounds-1)*20+30+2*5*rounds) {
		t.Errorf("values in the dictionary: %s", got)
	}
}

// TestIDAssignmentErasedMeanwhile appends no value to the dictionary that
// an erasure deleted the rows of between the assignment finding it and
// taking the lease: the erasure counts itself under the lease, and the
// assignment checks the count there.
func TestIDAssignmentErasedMeanwhile(t *testing.T) {
	it := newIDTest(t)
	ctx := context.Background()
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people (person_id VARCHAR, n INTEGER)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"INSERT INTO lake.public.people VALUES ('p1', 1)",
	)
	srv := it.r.srv
	found, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	srv.afterNewValues = func() {
		once.Do(func() {
			close(found)
			<-proceed
		})
	}
	done := make(chan error, 1)
	go func() {
		_, err := it.admin.Exec(ctx, "INSERT INTO lake.public.people VALUES ('gone1', 1), ('p1', 2)")
		done <- err
	}()
	<-found
	// What an erasure of gone1 does last, while the assignment waits.
	unlock, err := srv.idDomains.Lock(ctx, "people", "erasure/test", time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.idDomains.MarkErased(ctx, "people"); err != nil {
		t.Fatal(err)
	}
	it.internal("DELETE FROM lake.public.people WHERE person_id = 'gone1'")
	unlock()
	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := it.one("SELECT count(*) FROM lake.igloo_domains.people WHERE value = 'gone1'"); got != "0" {
		t.Errorf("an erased value came back into the dictionary: %s", got)
	}
	srv.afterNewValues = nil
	// Values found afterwards get their ids as usual.
	it.exec("INSERT INTO lake.public.people VALUES ('p2', 2)")
	it.consistent("people", true, "people")
}

// TestIDAssignmentDeferred leaves the new values of writes smaller than
// IGLOO_ID_SYNC_MIN_ROWS to a mapping pass shortly after, which several
// such writes share, and gives a larger write's new values ids before it
// returns.
func TestIDAssignmentDeferred(t *testing.T) {
	saved := idDeferredDelay
	idDeferredDelay = 3 * time.Second
	defer func() { idDeferredDelay = saved }()
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) {
		o.IDDomainsOn = true
		o.IDSyncMinRows = 100
		o.Tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	})
	it := &idTest{t: t, vc: vc, r: r, admin: r.admin(t, pgx.QueryExecModeSimpleProtocol)}
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people (person_id VARCHAR, n INTEGER)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
	)
	// Large enough: at once.
	it.exec("INSERT INTO lake.public.people SELECT 'p' || i, CAST(i AS INTEGER) FROM range(150) r(i)")
	it.consistent("people", true, "people")
	// Small writes: values the dictionary has get their ids as they are
	// written, new ones shortly after.
	commits := it.vc.fake.Commits()
	it.exec("INSERT INTO lake.public.people VALUES ('p1', 1), ('small1', 1)",
		"INSERT INTO lake.public.people VALUES ('small2', 2)")
	if it.id("people", "small1") != "<nil>" || it.id("people", "small2") != "<nil>" {
		t.Errorf("small writes' new values got ids at once: %s %s", it.id("people", "small1"), it.id("people", "small2"))
	}
	if got := it.one("SELECT count(*) FROM lake.public.people WHERE person_id = 'p1' AND _igloo_id_person_id IS NOT NULL"); got != "2" {
		t.Errorf("rows of p1 with ids: %s", got)
	}
	if n := it.vc.fake.Commits() - commits; n != 2 {
		t.Errorf("two small writes made %d commits, want their own two only", n)
	}
	deadline := time.Now().Add(20 * time.Second)
	for it.id("people", "small2") == "<nil>" && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	it.consistent("people", true, "people")
	// One pass for both: the dictionary's append and the table's fill.
	if n := it.vc.fake.Commits() - commits; n != 4 {
		t.Errorf("the small writes and their pass made %d commits, want 4", n)
	}
}
