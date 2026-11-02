package pgwire

import (
	"errors"
	"strings"
	"sync/atomic"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// DuckDB's dbgen, and dsdgen where the tpcds extension is loaded, generate data
// with the benchmarks' own generators, which keep their state in globals of the
// process: two at once crash it, on a thread of DuckDB's, for every session, and
// whatever restarts the server starts it again with nothing to say why. Only an
// admin may call them (see requiredRole), and one statement of a process may at a
// time: the others are refused, as a statement could wait for minutes behind
// one. Any statement that mentions them must call them, not keep them for later,
// in a macro, a view or a prepared statement that would call them unseen.

// dataGenRunning says a statement of this process is calling a generator. It is
// the process's, not a server's, as the generators' state is.
var dataGenRunning atomic.Bool

var (
	errDataGenBusy = pgerror.WithCode(errors.New("another statement is generating data with dbgen or dsdgen, which keep their state in the process: "+
		"run them one at a time"), codes.LockNotAvailable)
	errDataGenKept = pgerror.WithCode(errors.New("dbgen and dsdgen may only be called directly, by CALL or in a SELECT: "+
		"run one at a time, and a macro, view or prepared statement could call them unseen"), codes.FeatureNotSupported)
)

// mentionsDataGen reports whether query's code names a generator. Most
// statements do not spell them, and pay for a search of the text alone.
func mentionsDataGen(query string) bool {
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "dbgen") && !strings.Contains(lower, "dsdgen") {
		return false
	}
	return mentionsFunction(query, sqllex.CodeWords(query), "dbgen", "dsdgen")
}

// holdDataGen returns what to call when st, which may call a generator, has run;
// an error if it may not run now.
func holdDataGen(st *statement) (release func(), err error) {
	if !mentionsDataGen(st.query) {
		return func() {}, nil
	}
	if t := st.stmtType; t != duckdb.STATEMENT_TYPE_CALL && t != duckdb.STATEMENT_TYPE_SELECT {
		return nil, errDataGenKept
	}
	if !dataGenRunning.CompareAndSwap(false, true) {
		return nil, errDataGenBusy
	}
	return func() { dataGenRunning.Store(false) }, nil
}
