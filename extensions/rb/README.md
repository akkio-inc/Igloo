# rb: exact distinct counts in roaring bitmaps

`rb` is Igloo's own DuckDB extension. It counts distinct integer IDs exactly
in [roaring bitmaps](https://roaringbitmap.org/) (CRoaring, vendored in
`third_party/croaring`), which hold a dense ID in two bytes or less, where
DuckDB's `COUNT(DISTINCT)` hash table holds 72-110 bytes for each
36-character ID. On a synthetic copy of the F1 signals report at 1x, counting
into bitmaps took 35 s with no spill and 2.8 GB of memory, where the hash took
183 s and spilled 51 GB. Where the hash fits in memory it stays faster, which
is why nothing uses the bitmaps unless asked (see `rb_count_distinct` below).

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

The setting `rb_count_distinct` (`'off'`, the default, `'auto'` or
`'always'`) is where the planner rule that rewrites `COUNT(DISTINCT)` of a
`UINTEGER` into `rb_count_distinct` will look.

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
