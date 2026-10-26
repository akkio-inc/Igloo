package pgwire

import (
	"context"
	"strings"
	"sync"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// functionList is what the rewrites of statements and the result cache
// need to know of DuckDB's functions: which are volatile (see volatileSet),
// which give results that can change between runs or sessions (see
// sessionFunctions), and which are aggregates. Listing them takes DuckDB
// some 25 ms and walks the catalog, which preparing a statement does
// holding the catalog lock exclusively, so every statement of a
// dashboard's burst that repeats a subquery waited for the others'
// listings. They are the same in every session, Igloo's own macros
// included (it makes them in each), so they are listed once for each
// catalog generation; a session that may have temporary macros of its own
// has its own listed every time.
type functionList struct {
	mu   sync.Mutex
	gen  uint64
	sets *functionSets
}

// functionSets are the functions a functionList knows, by lower-case name.
type functionSets struct {
	volatile, unstable, aggregates map[string]bool
}

// functionListQuery lists DuckDB's functions, one a row: the name,
// stability, whether it is built in, its type, and a macro's definition,
// separated by tabs.
const functionListQuery = `SELECT system.main.lower(function_name) || chr(9) || coalesce(stability, '') || chr(9) ||
	CAST(internal AS VARCHAR) || chr(9) || function_type || chr(9) || coalesce(macro_definition, '')
	FROM system.main.duckdb_functions()
	WHERE stability <> 'CONSISTENT' OR macro_definition IS NOT NULL OR function_type = 'aggregate'`

// functionSets returns the functions of the session's connection.
func (s *Server) functionSets(ctx context.Context, sess *session) (*functionSets, error) {
	gen := s.catalogGeneration()
	f := &s.functions
	if !sess.temps {
		f.mu.Lock()
		if f.sets != nil && f.gen == gen {
			defer f.mu.Unlock()
			return f.sets, nil
		}
		f.mu.Unlock()
	}
	// Listing them walks the catalog (see catalogRunLock).
	release := s.holdCatalog(sess)
	rows, err := queryStrings(ctx, sess, functionListQuery)
	release()
	if err != nil {
		return nil, err
	}
	sets := functionSetsOf(rows)
	if !sess.temps {
		f.mu.Lock()
		f.gen, f.sets = gen, sets
		f.mu.Unlock()
	}
	return sets, nil
}

// functionSetsOf reads functionListQuery's rows: volatileSet's volatile
// functions and macros, the unstable ones (see sessionFunctions), and the
// aggregates.
func functionSetsOf(rows []string) *functionSets {
	// volatileSet's rows: volatile functions' names, and the definitions of
	// macros that are not built in.
	var volatileRows []string
	unstable, aggregates := map[string]bool{}, map[string]bool{}
	for k := range sessionFunctions {
		unstable[k] = true
	}
	macros := map[string][]string{}
	for _, r := range rows {
		parts := strings.SplitN(r, "\t", 5)
		if len(parts) != 5 {
			continue
		}
		name, stability, internal, kind, def := parts[0], parts[1], parts[2], parts[3], parts[4]
		if kind == "aggregate" {
			aggregates[name] = true
		}
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
	return &functionSets{volatile: volatileSet(volatileRows), unstable: unstable, aggregates: aggregates}
}
