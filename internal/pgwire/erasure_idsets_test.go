package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/erasure"
)

// recordingIDSets stands in for derived sets of ids, as bitmap indexes: it
// records what requests have it erase, and answers verification's counts
// from holding, the last answer repeating.
type recordingIDSets struct {
	mu      sync.Mutex
	erased  []string
	holding []int64
	asked   int
}

func (f *recordingIDSets) Erase(_ context.Context, domain string, ids []int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.erased = append(f.erased, fmt.Sprintf("%s %v", domain, ids))
	return nil
}

func (f *recordingIDSets) Holding(_ context.Context, _ string, _ []int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked++
	if len(f.holding) == 0 {
		return 0, nil
	}
	n := f.holding[0]
	if len(f.holding) > 1 {
		f.holding = f.holding[1:]
	}
	return n, nil
}

// TestErasureIDSets erases people whose identifiers a domain's dictionary
// gave ids: the request looks the ids up, keeps them sealed, has the sets
// derived from them erase them at the end of the expire step, counts those
// still holding them in its verification, which sends it back to expire,
// and forgets them when it ends.
func TestErasureIDSets(t *testing.T) {
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) { o.IDDomainsOn = true })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people AS SELECT 'p' || i AS person_id, i AS n FROM range(20) r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
	} {
		mustExec(t, admin, q)
	}
	conn, err := r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO lake.igloo_domains.people VALUES ('p0', 0, now()), ('p1', 7, now()), ('p2', 9, now())", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	sets := &recordingIDSets{}
	store := erasure.NewMemoryStore()
	svc := erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: r.srv.gov.store.Load, Engine: r.eng,
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Warehouse: vc.cfg.Storage.Location + "/",
		IDDomains: r.srv.idDomains, IDSets: sets})
	erase := func(identifier string) *erasure.Request {
		t.Helper()
		req, err := svc.Submit(ctx, "privacy", []erasure.Wanted{{Tag: "pii", Value: "person_id", Identifiers: []string{identifier}}})
		if err != nil {
			t.Fatal(err)
		}
		var got *erasure.Request
		for range 6 {
			if err := svc.Run(ctx, req.ID); err != nil {
				t.Fatal(err)
			}
			if got, err = store.Get(ctx, req.ID); err != nil {
				t.Fatal(err)
			}
			if got.Status == erasure.StatusCompleted {
				break
			}
		}
		if got.Status != erasure.StatusCompleted {
			t.Fatalf("erasing %s: status %s at %s: %s", identifier, got.Status, got.Step, got.Error)
		}
		// The ids are gone with the identifiers.
		if b, _ := json.Marshal(got); got.SealedIDs != "" || strings.Contains(string(b), "sealed_ids") {
			t.Errorf("erasing %s: the request kept its ids: %s", identifier, b)
		}
		return got
	}

	got := erase("p1")
	if strings.Join(sets.erased, "; ") != "people [7]" || sets.asked != 1 {
		t.Errorf("erasing p1: erased %q, verification asked %d times", sets.erased, sets.asked)
	}
	if got.Verification == nil || got.Verification.IDSets != 0 || got.Rounds != 0 {
		t.Errorf("erasing p1: verification %+v after %d rounds", got.Verification, got.Rounds)
	}

	// A set still holding the id sends the request back to expire, which
	// has the sets erase it again.
	sets.erased, sets.asked, sets.holding = nil, 0, []int64{2, 0}
	got = erase("p2")
	if strings.Join(sets.erased, "; ") != "people [9]; people [9]" || sets.asked != 2 {
		t.Errorf("erasing p2: erased %q, verification asked %d times", sets.erased, sets.asked)
	}
	if got.Rounds != 1 || got.Verification == nil || got.Verification.IDSets != 0 {
		t.Errorf("erasing p2: %d rounds, verification %+v", got.Rounds, got.Verification)
	}

	// A value that gets its id after the request looked the ids up, as when
	// maintenance maps rows another engine wrote while the request deletes
	// from the tables, is looked up again under the domain's lease just
	// before the last delete from the dictionary.
	racing := &insertingRows{eng: r.eng, after: "public.people",
		insert: "INSERT INTO lake.igloo_domains.people VALUES ('p0', 11, now())"}
	conn, err = r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM lake.igloo_domains.people WHERE value = 'p0'", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	svc = erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: r.srv.gov.store.Load, Engine: r.eng,
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Warehouse: vc.cfg.Storage.Location + "/",
		IDDomains: r.srv.idDomains, IDSets: sets, Rows: racing})
	sets.erased, sets.asked, sets.holding = nil, 0, nil
	erase("p0")
	if strings.Join(sets.erased, "; ") != "people [11]" || !racing.inserted {
		t.Errorf("erasing p0, given an id meanwhile: erased %q (inserted: %v)", sets.erased, racing.inserted)
	}

	// Someone with no id in a dictionary has nothing for the sets to erase.
	sets.erased, sets.asked, sets.holding = nil, 0, nil
	erase("nobody@example.com")
	if len(sets.erased) != 0 || sets.asked != 0 {
		t.Errorf("erasing someone without an id: erased %q, asked %d times", sets.erased, sets.asked)
	}
}

// insertingRows deletes, counts and looks up rows with DuckDB statements,
// as the erasure service's own does, and runs insert once, right after the
// first DELETE from the table named after: a write that lands while the
// request deletes.
type insertingRows struct {
	eng      *engine.Engine
	after    string
	insert   string
	inserted bool
}

func (x *insertingRows) name(tbl *table.Table) string {
	return quoteName(append([]string{"lake"}, tbl.Identifier()...))
}

func (x *insertingRows) exec(ctx context.Context, q string) (int64, error) {
	conn, err := x.eng.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	res, err := conn.ExecContext(ctx, q, nil)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (x *insertingRows) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	n, err := x.exec(ctx, "DELETE FROM "+x.name(tbl)+" WHERE "+cond)
	if err == nil && !x.inserted && strings.Join(tbl.Identifier(), ".") == x.after {
		x.inserted = true
		_, err = x.exec(ctx, x.insert)
	}
	return n, err
}

func (x *insertingRows) Count(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	return x.scalarList(ctx, "SELECT count(*) FROM "+x.name(tbl)+" WHERE "+cond)
}

func (x *insertingRows) scalarList(ctx context.Context, q string) (int64, error) {
	ids, err := x.ints(ctx, q)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	return ids[0], nil
}

func (x *insertingRows) ints(ctx context.Context, q string) ([]int64, error) {
	conn, err := x.eng.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	rows, err := queryRows(ctx, conn, q)
	if err != nil {
		return nil, err
	}
	var out []int64
	for _, row := range rows {
		if n, ok := row[0].(int64); ok {
			out = append(out, n)
		}
	}
	return out, nil
}

func (x *insertingRows) IDs(ctx context.Context, tbl *table.Table, cond string) ([]int64, error) {
	return x.ints(ctx, "SELECT DISTINCT CAST(id AS BIGINT) FROM "+x.name(tbl)+" WHERE id IS NOT NULL AND "+cond)
}
