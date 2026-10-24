package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// TestViewStarLeavesOutIDs checks that a star in a view over a member table
// stands for the columns statements see, without the column of ids: a
// column list names the columns it means, the catalog's listings show
// those names, and another replica, as after a restart, loads the view
// alike. Views recorded with the column of ids among their columns, as
// Igloo recorded them before, load with the names they meant.
func TestViewStarLeavesOutIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	vc := newViewCatalog(t)
	withIDs := func(o *Options) {
		o.IDDomainsOn = true
		o.Tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	}
	a := vc.replica(t, withIDs)
	admin := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE TABLE lake.public.people (person_id VARCHAR, n INTEGER)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"INSERT INTO lake.public.people VALUES ('p1', 1), ('p2', 2)",
		"CREATE TABLE lake.public.survey AS SELECT 1 AS resp_id, 'yes' AS answer_text",
		// E2's view: the population's star, then a survey's columns, named.
		"CREATE VIEW lake.public.v (who, num, resp_id, answer) AS SELECT s.*, sv.resp_id, sv.answer_text " +
			"FROM lake.public.people AS s LEFT JOIN lake.public.survey AS sv ON s.n = sv.resp_id",
		"CREATE VIEW lake.public.everyone AS SELECT * FROM lake.public.people",
	} {
		mustExec(t, admin, q)
	}

	// Views as Igloo recorded them before: the column of ids among their
	// columns, under its own name or a column list's name shifted onto it.
	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	columns := func(names ...string) []catalog.Column {
		out := make([]catalog.Column, len(names))
		for i, n := range names {
			out[i] = catalog.Column{Name: n, Type: "VARCHAR"}
		}
		return out
	}
	for _, v := range []catalog.View{
		{Schema: "public", Name: "old_list", Columns: columns("who", "num", "resp_id", "answer", "answer_text"),
			SQL: "SELECT s.*, sv.resp_id, sv.answer_text FROM lake.public.people AS s LEFT JOIN lake.public.survey AS sv ON s.n = sv.resp_id"},
		{Schema: "public", Name: "old_star", Columns: columns("person_id", "n", "_igloo_id_person_id"),
			SQL: "SELECT * FROM lake.public.people"},
	} {
		if err := cat.PutView(ctx, v, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.objects.SyncViews(ctx); err != nil {
		t.Fatal(err)
	}
	b := vc.replica(t, withIDs)
	if _, err := b.objects.SyncViews(ctx); err != nil {
		t.Fatal(err)
	}

	want := map[string]struct{ columns, rows string }{
		"v":        {"who,num,resp_id,answer", "p1|1|1|yes p2|2|NULL|NULL"},
		"old_list": {"who,num,resp_id,answer", "p1|1|1|yes p2|2|NULL|NULL"},
		"everyone": {"person_id,n", "p1|1 p2|2"},
		"old_star": {"person_id,n", "p1|1 p2|2"},
	}
	for name, r := range map[string]*viewReplica{"the creating replica": a, "another replica": b} {
		conn := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		for view, w := range want {
			if got := strings.Join(columnNames(t, conn, "lake.public."+view), ","); got != w.columns {
				t.Errorf("%s, %s: columns %s, want %s", name, view, got, w.columns)
			}
			listed := texts(t, conn, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position", view)
			if got := strings.Join(listed, ","); got != w.columns {
				t.Errorf("%s, %s: information_schema lists %s, want %s", name, view, got, w.columns)
			}
			if got := strings.Join(texts(t, conn, "SELECT * FROM lake.public."+view+" ORDER BY 1"), " "); got != w.rows {
				t.Errorf("%s, %s: rows %s, want %s", name, view, got, w.rows)
			}
		}
	}
}
