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

## Distinct counts for the planner

DuckDB estimates the rows an equality filter leaves of a table as its rows
over the column's distinct count, and joins' sizes from their keys' counts,
when the scan's table function reports them; DuckDB-Iceberg reports none, and
DuckDB takes a fifth of the table instead. `igloo_set_distinct_counts(spec)`
hands DuckDB counts Igloo keeps (`ANALYZE`, see `internal/pgwire/analyze.go`):
`spec` is a line for each column, `catalog.schema.table`, the column and its
count, separated by tabs, and replaces the database's counts; it returns how
many columns it holds. An optimizer pass that runs before DuckDB's own has
each scan of a table with counts report them beside its table function's own
statistics, for that statement (`src/rb_stats.cpp`).

## Faster trim

DuckDB's `trim` decodes every character of a string to find Unicode's space
separators, the characters it trims, and copies what is left, trimmed or not:
some 300 ns a row for short strings. At load, the extension has the
one-argument `trim`, `ltrim` and `rtrim` run its own code instead, with
DuckDB's results (`src/rb_strings.cpp`): a space is trimmed by its byte, only
the characters at a string's ends that are not ASCII are decoded, and a
string left as it was is not copied. Over tens of millions of short strings, a filter
of `NOT trim(s) IN (...)` took 4.9 s with DuckDB's `trim` and takes 0.65 s,
against 0.3 s without `trim`.

## Snowflake's division

The Snowflake translator writes `a / b` as `sf_div(a, b)`, macros
(`internal/snowflake/macros.go`) that divide integers exactly to 6 decimal
places, a decimal to its scale plus 6 (at most 12), anything else as
DuckDB's `/` does, and fail on division by zero. DuckDB expands a macro
wherever it is called and binds every branch of it, each operand named some
25 times, so a statement's divisions took most of its prepare: 158 of the
208 ms a dashboard's statement of 76 divisions took. `igloo_sf_div(a, b)`
divides as the macros do, to the bit, NULLs and errors included, and picks
its branch once, from the operands' types (`src/rb_divide.cpp`); with the
extension loaded, Igloo defines `sf_div` as a call of it
(`snowflake.MacrosFor`).

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
