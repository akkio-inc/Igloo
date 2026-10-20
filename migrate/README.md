# Migration report

`python -m migrate report` tells a Snowflake or Databricks customer how far
their real workload gets on Igloo: it reads the source's query history,
groups it into query shapes, finds the tables in Igloo, replays the queries
there, compares results with the source on a small budgeted sample, and
writes an HTML, Markdown and JSON report. It is read-only on both sides by
default. [Migrating from Snowflake or Databricks](../docs/migrating.md)
describes what it reads, the grants it needs, what it runs, its privacy
defaults and how to read the report.

```sh
pip install -r migrate/requirements.txt
python -m migrate report --source snowflake --since 30d \
  --igloo 'host=igloo.example.com port=5432 user=migrate dbname=igloo' \
  --map 'PROD.ANALYTICS.*=lake.analytics.*' --out ~/igloo-migration
```

Write `--out` outside the repository (the tool refuses a directory inside
it): the extract holds query text as the source recorded it.

## Layout

| Module | What it does |
|---|---|
| `cli.py` | The command line, and the run from extraction to report |
| `sources/snowflake.py`, `sources/databricks.py` | Reading history, table sizes and spend from each source, and the budgeted comparison re-runs |
| `workload.py` | Groups of executions, merged into shapes |
| `sqltext.py` | Token-level operations: shapes, redaction, renaming tables in place |
| `statements.py` | Parse-level ones: statement classes, tables read and written, the read-only check, sqlglot translation |
| `inventory.py` | Finding source tables in Igloo (`--map` rules), and coverage |
| `igloo.py` | Igloo's tables, running statements with a timeout, error classes |
| `replay.py` | The replay of each shape, and the result comparisons |
| `compare.py` | Canonical values, order-independent checksums, comparing within precision |
| `cost.py`, `report.py` | The cost section, and the report in JSON, Markdown and HTML |

Prices come from [`bench/pricing.toml`](../bench/pricing.toml), read by the
benchmark's own loader; connection settings are the benchmark's.

## Tests

```sh
make migrate-test
```

runs the unit tests in a Python container, as CI does. They use synthetic
fixtures (`tests/fixtures/`) for the sources' rows and need no warehouse
or Igloo. With sqlglot installed, `python -m unittest discover -s
migrate/tests -t .` from the repository's root runs them directly.
