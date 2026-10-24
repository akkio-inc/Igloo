# rb: exact distinct counts in roaring bitmaps

`rb` is Igloo's own DuckDB extension. It counts distinct integer IDs exactly
in [roaring bitmaps](https://roaringbitmap.org/) (CRoaring, vendored in
`third_party/croaring`), which hold a dense ID in two bytes or less, where
DuckDB's `COUNT(DISTINCT)` hash table holds 72-110 bytes for each
36-character ID. On a synthetic copy of the F1 signals report at 1x, counting
into bitmaps took 35 s with no spill and 2.8 GB of memory, where the hash took
183 s and spilled 51 GB. Where the hash fits in memory it stays a little
faster (1.1-1.6x on 30 million rows in 10 to 100,000 groups, 3x for 1,000
groups of a small integer key, whose partial states DuckDB combines on one
thread), which is why nothing uses the bitmaps unless asked (see
`rb_count_distinct` below).

A group's ids are buffered and added in sorted batches. Its set is a sorted
array while the ids are sparse (a roaring container costs some 50 bytes of its
own, which a few ids in each 2^16 range of a large domain do not repay) and a
roaring bitmap once they are dense, and a group that never fills its buffer is
counted from the buffer alone.

## Functions

IDs are `UINTEGER`: a `BIGINT` must be cast (`CAST(x AS UINTEGER)` fails on a
value outside 0..2^32-1), and no overload narrows silently. Bitmaps are BLOBs
in CRoaring's [portable format](https://github.com/RoaringBitmap/RoaringFormatSpec),
run-length encoded where smaller, readable by every Roaring implementation
(Go's `github.com/RoaringBitmap/roaring` included). An empty set is an empty
bitmap, never NULL.

| Function | Returns |
|---|---|
| `rb_agg(UINTEGER)` | aggregate: the bitmap of the non-NULL IDs |
| `rb_or_agg(BLOB)` | aggregate: the union of the non-NULL bitmaps |
| `rb_count_distinct(UINTEGER)` | aggregate: `COUNT(DISTINCT id)` |
| `rb_count_distinct(UINTEGER, k)` | aggregate: `COUNT(DISTINCT id, k)` for `k` a DATE or an integer type, in either order (`(UINTEGER, UINTEGER)` takes the first as the ID); rows with a NULL are skipped; one bitmap is kept per `k`, so `k` should have few values |
| `rb_cardinality(BLOB)` | BIGINT |
| `rb_and`, `rb_or`, `rb_andnot` `(BLOB, BLOB)` | BLOB |
| `rb_and_cardinality`, `rb_or_cardinality`, `rb_andnot_cardinality` `(BLOB, BLOB)` | BIGINT |
| `rb_contains(BLOB, UINTEGER)` | BOOLEAN |
| `rb_to_list(BLOB)` | `UINTEGER[]` |

The scalars return NULL when an argument is NULL, and refuse a BLOB that is
not a valid bitmap (`rb: invalid bitmap BLOB ...`).

`rb_contains` reads a bitmap that is known when the statement is bound, such
as a literal or a variable's (`getvariable`), once, at bind. DuckDB hands
every chunk of rows its own copy of a constant BLOB, and of a scalar
subquery's, before the function sees it. So testing a billion rows against
a cohort's bitmap of tens of megabytes copies, and would read, the bitmap
for each of half a million chunks. A subquery's bitmap is still copied and
read once per chunk: hand a large one over in a variable (`SET VARIABLE b =
(SELECT ...)`, then `rb_contains(getvariable('b'), id)`).

## Settings

- `rb_count_distinct`: `'off'` (the default) or `'always'`. With `'always'`,
  an optimizer rule counts every `COUNT(DISTINCT x)` of a `UINTEGER`,
  `USMALLINT` or `UTINYINT` with `rb_count_distinct(x)`: same results, no
  FILTER or ORDER BY on the aggregate, window aggregates left alone. The rule
  weighs nothing: DuckDB's estimates of the rows reaching an aggregate are
  20-34x off on Iceberg tables, so Igloo decides per statement, from its own
  estimate of the rows the statement reads, and sets `'always'` around the
  statements it picks (`internal/pgwire/bitmaps.go`). Igloo's distributed
  planner reads it too, to combine workers' partial counts as bitmaps.
- `rb_memory_budget`: the most memory the bitmaps of one aggregate, or of one
  scalar expression, may take, such as `'2GB'`; empty (the default) leaves
  them to DuckDB's `memory_limit`. A scalar function's bitmaps are those it
  reads from its BLOBs (one for each argument while a call runs, or a
  constant's for as long as the statement). Bitmaps count toward
  `memory_limit` either way, and a statement whose bitmaps do not fit fails
  with an error that starts `rb: bitmap memory budget exceeded`, on which
  Igloo runs it again once without them: with `rb_count_distinct = 'off'`,
  or from the tables rather than bitmap indexes. The BLOBs themselves are
  DuckDB's values, which it copies between operators as it does any string.

## Building and loading

`extensions/rb/build.sh OUT_DIR` builds `rb.duckdb_extension` and its
SHA-256 for the machine's Linux architecture, in seconds: it compiles against
DuckDB's headers at the version duckdb-go's bindings are built from, and
leaves DuckDB's symbols to the binary that loads it (duckdb-go links DuckDB
with `-rdynamic`), so the extension always runs the igloo binary's own
DuckDB. The image builds it in the same step as the binary, which carries
the hash; at startup Igloo checks the file in
`$IGLOO_DUCKDB_EXTENSION_DIR/igloo/` against the hash and DuckDB's version,
loads it before DuckDB's lockdown and turns unsigned loading off, on the
server and on workers. `igloo check-extensions` checks that it loads.

The tests are in `test/*.test` (sqllogictest), run by
`internal/engine`'s `TestRBFunctions` against Igloo's DuckDB with the
extension built for the tests (`IGLOO_TEST_RB_EXTENSION`, which
`scripts/test.sh` and CI set).
