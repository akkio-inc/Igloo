package engine

import (
	"context"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// respellTimeout bounds looking tables up again by the catalog's names (see
// respell).
const respellTimeout = 30 * time.Second

// respell gives back the catalog's names to DuckDB-Iceberg's entries for
// the tables that statements or views named in another case since it last
// ran. The catalog finds a table by its name in any case, and DuckDB-Iceberg
// names its entry for a table as it was last looked up, so listings would
// list it so (see catalogproxy's names.go). Looking each table up again by
// the catalog's name, which the catalog proxy noted, names the entry so
// again; the lookup is a bind, which loads the table from the catalog as
// any bind of it does, and runs nothing.
//
// Listings run under the catalog lock held exclusively, so CatalogLock calls
// this once it has the lock: whatever a statement bound before, the listing
// lists the catalog's names. Each table that cannot be looked up (dropped
// meanwhile, say) is left as it is.
func (e *Engine) respell() {
	if e.proxy == nil || e.catalog == "" {
		return
	}
	tables := e.proxy.Respelled()
	if len(tables) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), respellTimeout)
	defer cancel()
	c, err := e.connector.Connect(ctx)
	if err != nil {
		e.log.Warn("cannot give tables back the names the catalog spells them with", "err", err)
		return
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	for _, t := range tables {
		name := ident(e.catalog) + "." + ident(strings.Join(t.Namespace, ".")) + "." + ident(t.Name)
		// pragma_table_info looks the table up as it is bound, and reads
		// nothing of its data.
		stmt, err := conn.PrepareContext(ctx, "SELECT 1 FROM pragma_table_info("+literal(name)+") LIMIT 0")
		if err != nil {
			e.log.Debug("cannot give a table back the name the catalog spells it with", "table", name, "err", err)
			continue
		}
		_ = stmt.Close()
	}
}
