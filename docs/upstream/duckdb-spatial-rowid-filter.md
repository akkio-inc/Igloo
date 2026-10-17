# Draft issue for duckdb/duckdb-spatial: filters on rowid fail in the R-tree optimizer

Not filed yet. In Igloo it affects DuckDB's own tables (temporary tables,
and every table of a server without a catalog) and views over them; Iceberg
tables have no rowid. Igloo answers these statements with SQLSTATE 0A000
until its DuckDB has a fix (see `rowidFilterFailure` in
`internal/pgwire/errors.go`); remove that, and this file, once it does.

---

**Title:** INTERNAL Error "optional_idx cannot be initialized with an invalid index" on a filter on rowid with spatial loaded

### What happens

With spatial loaded, a filter on `rowid` that DuckDB pushes into the table
scan as an expression filter fails while the statement is optimized, on any
table, R-tree index or not:

```sql
LOAD spatial;
CREATE TABLE t AS SELECT range AS i FROM range(10);
SELECT count(*) FROM t WHERE rowid % 2 = 0;
```

```
INTERNAL Error: optional_idx cannot be initialized with an invalid index
```

The query returns 5 without `LOAD spatial`, and with spatial loaded after
`SET disabled_optimizers = 'extension'`. Other filters that fail the same
way: `hash(rowid) % 3 = 0`, `rowid::VARCHAR = '5'`, `rowid IS NOT NULL`,
`i = 5 AND rowid % 3 = 0`, and `DELETE`/`UPDATE ... WHERE rowid % 100 = 0`.
So do views and `EXPLAIN`. Comparisons with constants (`rowid < 5`,
`rowid IN (1, 2)`) and filters that also read another column work: DuckDB
turns those into constant filters or keeps them in a `FILTER` above the
scan.

On a table with an R-tree index, a constant filter on `rowid` next to an
indexable predicate fails another way:

```sql
LOAD spatial;
CREATE TABLE p AS SELECT range AS id, ST_Point(range % 100, range // 100) AS geom FROM range(100000);
CREATE INDEX p_idx ON p USING RTREE (geom);
SELECT count(*) FROM p WHERE ST_Intersects(geom, ST_MakeEnvelope(0, 0, 2, 2)) AND rowid < 1000;
```

```
INTERNAL Error: Attempted to access index -1 within vector of size 2
```

### Versions

DuckDB v1.5.6 with spatial `04270fe` (the `v1.5-variegata` build from the
extension repository), linux_arm64. The code below is unchanged on
`v1.5-variegata` at `9bfcf30e`. On `main` (`v2.0-cyanoptera`) the loop was
rewritten to take the column through `get.GetColumnIndex(proj_id)`; we have
not tested it.

### Cause

`RTreeIndexScanOptimizer::TryOptimize`
([`rtree_index_plan_scan.cpp` L207-L213 at 04270fe](https://github.com/duckdb/duckdb-spatial/blob/04270fe7bb4eb412765cce8afde0276a9d81d3ba/src/spatial/index/rtree/rtree_index_plan_scan.cpp#L207-L213))
walks every `LogicalGet`'s `table_filters.filters` and passes each
expression filter's key to `TryOptimizeGet`, whose parameter is an
`optional_idx`. The key is the column id. For `rowid` it is
`COLUMN_IDENTIFIER_ROW_ID`, `(column_t)-1`, the same value as
`DConstants::INVALID_INDEX`, so the implicit `optional_idx(idx_t)`
constructor throws before `TryOptimizeGet` even checks for a `seq_scan`.
Core's filter pushdown (`TryPushdownGenericExpression`) creates such a
filter for a predicate that reads only `rowid`, is not a constant
comparison, and, if it can throw, is the only filter left.

The second failure is in `RewriteGetToIndexScan`
([L251-L253](https://github.com/duckdb/duckdb-spatial/blob/04270fe7bb4eb412765cce8afde0276a9d81d3ba/src/spatial/index/rtree/rtree_index_plan_scan.cpp#L251-L253)),
which pulls the scan's filters up into a `LogicalFilter` and looks up each
filter's type as `get.returned_types[column_id]`, out of range for `rowid`.

### Suggested fix

- In `TryOptimize`, skip filters on virtual columns, which no R-tree index
  covers: `if (IsVirtualColumn(entry.first)) { continue; }` before the call.
- In `RewriteGetToIndexScan`, take the filter column's type from
  `get.GetColumnType(ColumnIndex(column_id))`, which knows virtual columns,
  rather than `get.returned_types[column_id]`.

A test could be the two queries above, expecting 5 and 9.
