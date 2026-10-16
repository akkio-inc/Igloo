package engine

import (
	"errors"
	"sync"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// parser counts the statements in a query without running any of them.
// Preparing a multi-statement query on a session connection would execute
// every statement but the last, so callers that must vet each statement
// first count them here. It uses its own empty in-memory DuckDB, which only
// ever parses.
type parser struct {
	mu   sync.Mutex
	db   mapping.Database
	conn mapping.Connection
}

func newParser() (*parser, error) {
	p := &parser{}
	if mapping.Open(":memory:", &p.db) == mapping.StateError {
		return nil, errors.New("open parser database")
	}
	if mapping.Connect(p.db, &p.conn) == mapping.StateError {
		mapping.Close(&p.db)
		return nil, errors.New("connect parser database")
	}
	return p, nil
}

// count returns the number of statements in query, or DuckDB's syntax error.
func (p *parser) count(query string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var stmts mapping.ExtractedStatements
	n := mapping.ExtractStatements(p.conn, query, &stmts)
	defer mapping.DestroyExtracted(&stmts)
	if n == 0 {
		if msg := mapping.ExtractStatementsError(stmts); msg != "" {
			return 0, errors.New(msg)
		}
	}
	return int(n), nil
}

func (p *parser) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	mapping.Disconnect(&p.conn)
	mapping.Close(&p.db)
}
