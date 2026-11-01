package mapping

import bindings "github.com/duckdb/duckdb-go-bindings"

// Igloo patch: a pending result that streams, for Stmt.QueryStreamContext.
// DuckDB's C API marks duckdb_pending_prepared_streaming deprecated, along
// with the rest of its streaming results, but has nothing else that returns
// a result's rows as DuckDB computes them, which duckdb_fetch_chunk then
// reads.
var PendingPreparedStreaming = bindings.PendingPreparedStreaming //nolint:staticcheck
