package pgwire

import (
	"context"
	"strings"
	"sync"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// functionList is what the rewrites of statements and the result cache
// need to know of DuckDB's functions: which are volatile (see volatileSet),
// and which give results that can change between runs or sessions (see
// unstableFunctions). Listing them takes DuckDB some 25 ms and walks the
// catalog, which preparing a statement does holding the catalog lock
// exclusively, so every statement of a dashboard's burst that repeats a
// subquery waited for the others' listings. They are the same in every
// session, Igloo's own macros included (it makes them in each), so they
// are listed once for each catalog generation; a session that may have
// temporary macros of its own has its own listed every time.
type functionList struct {
	mu                 sync.Mutex
	gen                uint64
	volatile, unstable map[string]bool
}

// functionListQuery lists DuckDB's functions, one a row: the name,
// stability, whether it is built in, and a macro's definition, separated
// by tabs.
const functionListQuery = `SELECT system.main.lower(function_name) || chr(9) || coalesce(stability, '') || chr(9) ||
	CAST(internal AS VARCHAR) || chr(9) || coalesce(macro_definition, '') FROM system.main.duckdb_functions()
	WHERE stability <> 'CONSISTENT' OR macro_definition IS NOT NULL`

// functionSets returns the volatile and the unstable functions.
func (s *Server) functionSets(ctx context.Context, sess *session) (volatile, unstable map[string]bool, err error) {
	gen := s.catalogGeneration()
	f := &s.functions
	if !sess.temps {
		f.mu.Lock()
		if f.volatile != nil && f.gen == gen {
			defer f.mu.Unlock()
			return f.volatile, f.unstable, nil
		}
		f.mu.Unlock()
	}
	// Listing them walks the catalog (see catalogRunLock).
	release := s.holdCatalog(sess)
	rows, err := queryStrings(ctx, sess, functionListQuery)
	release()
	if err != nil {
		return nil, nil, err
	}
	volatile, unstable = functionSetsOf(rows)
	if !sess.temps {
		f.mu.Lock()
		f.gen, f.volatile, f.unstable = gen, volatile, unstable
		f.mu.Unlock()
	}
	return volatile, unstable, nil
}

// functionSetsOf reads functionListQuery's rows: volatileSet's volatile
// functions and macros, and unstableFunctions' unstable ones.
func functionSetsOf(rows []string) (volatile, unstable map[string]bool) {
	// volatileSet's rows: volatile functions' names, and the definitions of
	// macros that are not built in.
	var volatileRows []string
	unstable = map[string]bool{}
	for k := range sessionFunctions {
		unstable[k] = true
	}
	macros := map[string][]string{}
	for _, r := range rows {
		parts := strings.SplitN(r, "\t", 4)
		if len(parts) != 4 {
			continue
		}
		name, stability, internal, def := parts[0], parts[1], parts[2], parts[3]
		if stability == "VOLATILE" {
			volatileRows = append(volatileRows, name)
		}
		if def != "" && internal != "true" {
			volatileRows = append(volatileRows, name+"\t"+def)
		}
		switch {
		case (stability == "VOLATILE" || stability == "CONSISTENT_WITHIN_QUERY") && name != "error":
			unstable[name] = true
		case def != "":
			for _, w := range sqllex.CodeWords(def) {
				macros[name] = append(macros[name], strings.ToLower(w))
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for name, words := range macros {
			if unstable[name] {
				continue
			}
			for _, w := range words {
				if unstable[w] {
					unstable[name], changed = true, true
					break
				}
			}
		}
	}
	return volatileSet(volatileRows), unstable
}
