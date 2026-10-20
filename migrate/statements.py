"""What a statement is and what it touches, from its parse tree: its class,
the tables it reads and writes, whether it is safe to run read-only, and
its translation into DuckDB's dialect with sqlglot."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, UnsupportedError

from . import sqltext

# sqlglot logs a warning for every statement it reads as an opaque Command
# (SHOW, GRANT, ALTER WAREHOUSE, ...), which a workload has many of; only
# translate() wants its warnings, and collects them itself.
_SQLGLOT_LOG = logging.getLogger("sqlglot")
_SQLGLOT_LOG.addHandler(logging.NullHandler())
_SQLGLOT_LOG.propagate = False

CLASSES = ("SELECT", "CTAS", "INSERT", "MERGE", "UPDATE", "DELETE", "DDL", "other")
WRITE_CLASSES = ("CTAS", "INSERT", "MERGE", "UPDATE", "DELETE")

# The source's own statement types, by class. Snowflake's QUERY_TYPE and
# Databricks' statement_type; anything not listed is classified from the
# text, and failing that is "other".
SOURCE_TYPES = {
    "SELECT": "SELECT",
    "CREATE_TABLE_AS_SELECT": "CTAS",
    "INSERT": "INSERT",
    "MULTI_TABLE_INSERT": "INSERT",
    "MERGE": "MERGE",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
    "CREATE_TABLE": "DDL",
    "CREATE_VIEW": "DDL",
    "CREATE_SCHEMA": "DDL",
    "CREATE_DATABASE": "DDL",
    "CREATE_MATERIALIZED_VIEW": "DDL",
    "CREATE_EXTERNAL_TABLE": "DDL",
    "ALTER_TABLE": "DDL",
    "ALTER_TABLE_ADD_COLUMN": "DDL",
    "ALTER_TABLE_DROP_COLUMN": "DDL",
    "ALTER_TABLE_MODIFY_COLUMN": "DDL",
    "RENAME_TABLE": "DDL",
    "RENAME_COLUMN": "DDL",
    "DROP": "DDL",
    "DROP_CONSTRAINT": "DDL",
    "TRUNCATE_TABLE": "DDL",
    "TRUNCATE": "DDL",
    "COMMENT": "DDL",
    # Databricks
    "DROP_TABLE": "DDL",
    "OPTIMIZE": "other",
    "REFRESH": "other",
}
# Types that need the text to tell (CREATE may be CTAS or DDL).
AMBIGUOUS_TYPES = {"CREATE", "REPLACE", "ALTER", "OTHER", "UNKNOWN", ""}

# Function names that must never reach the source warehouse in a re-run,
# because calling them changes something: Snowflake's system functions
# (SYSTEM$CANCEL_QUERY, SYSTEM$ABORT_SESSION, ...), sequences, and
# Databricks functions that call out to other services.
UNSAFE_FUNCTIONS = re.compile(r"^(SYSTEM\$.*|NEXTVAL|HTTP_REQUEST|AI_QUERY|AI_[A-Z_]+|VECTOR_SEARCH)$", re.I)

# Statement node types that change data, schema, state or privileges.
WRITE_NODES = tuple(
    getattr(exp, name)
    for name in ("Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "TruncateTable", "Command", "Copy",
                 "Set", "Use", "Transaction", "Commit", "Rollback", "Grant", "Revoke", "Comment", "Into", "LoadData",
                 "Refresh", "Cache", "Uncache", "Pragma", "Analyze", "Export")
    if hasattr(exp, name)
)
QUERY_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)

# Sources' own metadata: never mirrored, since Igloo has its own.
SYSTEM_SCHEMAS = {"information_schema", "account_usage", "organization_usage", "data_sharing_usage", "readers_usage",
                  "pg_catalog"}
SYSTEM_DATABASES = {"snowflake": {"snowflake"}, "databricks": {"system"}}


def parse(sql: str, dialect: str) -> exp.Expression:
    """The statement's parse tree; raises if it is not exactly one statement
    sqlglot can read."""
    trees = [t for t in sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE) if t is not None]
    if len(trees) != 1:
        raise ValueError(f"expected one statement, found {len(trees)}")
    return trees[0]


def try_parse(sql: str, dialect: str) -> exp.Expression | None:
    try:
        return parse(sql, dialect)
    except Exception:  # noqa: BLE001 - sqlglot raises several kinds
        return None


def classify(source_type: str | None, sql: str, dialect: str, tree: exp.Expression | None = None) -> str:
    """One of CLASSES, from the source's statement type where it is
    unambiguous and otherwise from the text."""
    st = (source_type or "").upper().replace(" ", "_")
    if st in SOURCE_TYPES:
        return SOURCE_TYPES[st]
    if st.startswith(("CREATE_", "ALTER_", "DROP_", "RENAME_")):
        # ALTER_TABLE_..., DROP_VIEW: DDL on data; ALTER_SESSION,
        # ALTER_WAREHOUSE_SUSPEND, CREATE_ROLE: not.
        return "DDL" if re.search(r"TABLE|VIEW|SCHEMA|DATABASE|COLUMN|CONSTRAINT", st) else "other"
    if st and st not in AMBIGUOUS_TYPES:
        return "other"
    tree = tree if tree is not None else try_parse(sql, dialect)
    if tree is not None:
        return _class_of_tree(tree)
    words = re.findall(r"[A-Za-z_]+", _strip_leading_comments(sql).upper())[:6]
    if not words:
        return "other"
    if words[0] in ("SELECT", "WITH"):
        return "SELECT"
    if words[0] in ("CREATE", "REPLACE") and "AS" in words and "TABLE" in words:
        return "CTAS"
    if words[0] in ("INSERT", "MERGE", "UPDATE", "DELETE"):
        return words[0]
    if words[0] in ("CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "RENAME"):
        return "DDL"
    return "other"


def _strip_leading_comments(sql: str) -> str:
    return re.sub(r"^(\s|--[^\n]*|//[^\n]*|/\*.*?\*/)+", "", sql, flags=re.S)


def _class_of_tree(tree: exp.Expression) -> str:
    if isinstance(tree, QUERY_ROOTS):
        return "CTAS" if tree.args.get("into") else "SELECT"
    if isinstance(tree, exp.Insert):
        return "INSERT"
    if isinstance(tree, exp.Merge):
        return "MERGE"
    if isinstance(tree, exp.Update):
        return "UPDATE"
    if isinstance(tree, exp.Delete):
        return "DELETE"
    if isinstance(tree, exp.Create):
        if isinstance(tree.expression, QUERY_ROOTS) and (tree.args.get("kind") or "").upper() == "TABLE":
            return "CTAS"
        return "DDL"
    if isinstance(tree, (exp.Drop, exp.TruncateTable)) or type(tree).__name__ in ("Alter", "AlterTable", "Comment"):
        return "DDL"
    return "other"


@dataclass(frozen=True)
class TableRef:
    """A table or view a statement names."""

    written: tuple[str, ...]  # as written, lower case: ('lineitem',) or ('db', 'schema', 'lineitem')
    key: tuple[str, str, str]  # resolved against the session's namespace, lower case; '' where unknown
    name: str  # resolved, for display, in the source's case
    kind: str = "table"  # table, system (the source's own metadata), stage (files), function (a table function)

    @property
    def resolved(self) -> bool:
        return all(self.key)


@dataclass
class Tables:
    reads: list[TableRef] = field(default_factory=list)
    writes: list[TableRef] = field(default_factory=list)
    target_must_exist: bool = False  # INSERT, MERGE, UPDATE, DELETE write into an existing table
    parsed: bool = True


def _part(ident: exp.Expression | str | None, dialect: str) -> str:
    """A name part in the case the source stores it: Snowflake keeps quoted
    names as written and upper-cases the rest; Databricks is case
    insensitive and stores lower case."""
    if ident is None or ident == "":
        return ""
    if isinstance(ident, str):
        return ident
    name = ident.name
    quoted = bool(ident.args.get("quoted")) if isinstance(ident, exp.Identifier) else False
    if dialect == "snowflake" and not quoted:
        return name.upper()
    if dialect == "databricks":
        return name.lower()
    return name


def _ref(table: exp.Table, dialect: str, db: str | None, schema: str | None, kind: str = "table") -> TableRef:
    parts = [p for p in (table.args.get("catalog"), table.args.get("db"), table.this) if p is not None and p != ""]
    written = tuple(p.name.lower() for p in parts if isinstance(p, exp.Expression))
    names = [_part(p, dialect) for p in parts]
    while len(names) < 3:
        names.insert(0, (schema if len(names) == 1 else db) or "")
    resolved = tuple(names[-3:])
    sys_dbs = SYSTEM_DATABASES.get(dialect, set())
    if kind == "table" and (resolved[0].lower() in sys_dbs or resolved[1].lower() in SYSTEM_SCHEMAS):
        kind = "system"
    return TableRef(written=written, key=tuple(n.lower() for n in resolved), name=".".join(n for n in resolved if n) or
                    ".".join(names), kind=kind)


def tables(tree: exp.Expression, dialect: str, db: str | None = None, schema: str | None = None) -> Tables:
    """The tables a statement reads and writes, resolved against the session's
    database and schema."""
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    out = Tables()
    target = None
    if isinstance(tree, (exp.Insert, exp.Merge, exp.Update, exp.Delete, exp.Create)):
        target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        if not isinstance(target, exp.Table):
            target = None
    elif isinstance(tree, QUERY_ROOTS) and isinstance(tree.args.get("into"), exp.Into):
        target = tree.args["into"].this
    if target is not None:
        out.writes.append(_ref(target, dialect, db, schema))
        out.target_must_exist = not isinstance(tree, (exp.Create, *QUERY_ROOTS))
    seen = set()
    for t in tree.find_all(exp.Table):
        if t is target:
            continue
        if not isinstance(t.this, exp.Identifier):
            if isinstance(t.this, exp.Var) and t.this.name.startswith("@"):
                ref = TableRef(written=(t.this.name.lower(),), key=("", "", t.this.name.lower()), name=t.this.name,
                               kind="stage")
            else:
                fn = t.this.sql(dialect=dialect) if t.this is not None else "?"
                ref = TableRef(written=(), key=("", "", fn.lower()), name=fn, kind="function")
        elif not t.args.get("db") and t.name.lower() in ctes:
            continue
        else:
            ref = _ref(t, dialect, db, schema)
        if ref.key not in seen:
            seen.add(ref.key)
            out.reads.append(ref)
    # Table functions over the source's metadata, such as
    # TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()), are not Table nodes.
    for f in tree.find_all(exp.Dot):
        if any(isinstance(p, exp.Identifier) and p.name.lower() in SYSTEM_SCHEMAS for p in f.find_all(exp.Identifier)):
            key = ("", "information_schema", f.sql(dialect=dialect).lower()[:80])
            if key not in seen:
                seen.add(key)
                out.reads.append(TableRef(written=(), key=key, name=f.sql(dialect=dialect)[:80], kind="system"))
            break
    return out


def unsafe_functions(tree: exp.Expression) -> list[str]:
    """Calls in the statement that would change something if run."""
    names = []
    for f in tree.find_all(exp.Func):
        name = f.name if isinstance(f, exp.Anonymous) else (f.sql_name() if hasattr(f, "sql_name") else "")
        if name and UNSAFE_FUNCTIONS.match(name):
            names.append(name)
    for col in tree.find_all(exp.Column):
        if col.name.upper() == "NEXTVAL":
            names.append("NEXTVAL")
    return names


def read_only_problem(sql: str, dialect: str, functions: bool = True) -> str | None:
    """Why the statement cannot be run as a read-only query, or None if it
    is one: a single SELECT (or set operation of SELECTs) with nothing in it
    that writes, changes session or account state, or (with functions)
    calls a function of the source's that does. Igloo has none of those
    functions, so a replay there needs no check for them: it fails, and
    says which function is missing."""
    try:
        tree = parse(sql, dialect)
    except Exception as e:  # noqa: BLE001
        return f"cannot parse it to check that it only reads ({type(e).__name__})"
    if isinstance(tree, exp.Command) and str(tree.this).upper() in ("SELECT", "WITH"):
        # sqlglot read it as an opaque command: as good as unparsed.
        return "cannot parse it to check that it only reads (sqlglot read it as an opaque command)"
    if isinstance(tree, exp.Command):
        return f"it is a {str(tree.this).upper()} statement, not a query"
    if not isinstance(tree, QUERY_ROOTS):
        return f"it is a {type(tree).__name__} statement, not a query"
    if tree.args.get("into"):
        return "SELECT ... INTO creates a table"
    for node in tree.walk():
        if isinstance(node, WRITE_NODES):
            return f"it contains a {type(node).__name__}"
    if not functions:
        return None
    bad = unsafe_functions(tree)
    if bad:
        return f"it calls {', '.join(sorted(set(bad)))}"
    if re.search(r"\bNEXTVAL\b", sql, re.I):
        return "it reads a sequence's next value"
    return None


WRITE_WORDS = {"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "TRUNCATE", "COPY", "GRANT", "REVOKE",
               "CALL", "EXECUTE", "PUT", "REMOVE", "UNDROP", "USE", "BEGIN", "COMMIT", "ROLLBACK", "ATTACH", "DETACH",
               "INSTALL", "LOAD", "PRAGMA", "EXPORT", "IMPORT", "VACUUM", "CHECKPOINT", "NEXTVAL", "INTO"}


def read_only_words(sql: str, dialect: str, functions: bool = True) -> str | None:
    """A cruder check for statements sqlglot cannot parse: it starts as a
    query does and has none of the words that write, outside literals."""
    try:
        words = [t.text.upper() for t in sqltext.tokenize(sql, dialect) if t.token_type not in sqltext.LITERALS]
    except Exception:  # noqa: BLE001
        return "cannot read it to check that it only reads"
    if not words or words[0] not in ("SELECT", "WITH", "(", "FROM", "VALUES"):
        return "it does not start as a query does"
    bad = sorted(set(words) & WRITE_WORDS)
    if bad:
        return f"it contains {', '.join(bad)}"
    if functions and any(UNSAFE_FUNCTIONS.match(w) for w in words):
        return "it calls a function that changes something"
    return None


def write_target_problem(sql: str, dialect: str, allowed_schema: tuple[str, str]) -> str | None:
    """Why the statement's writes are not confined to the scratch schema
    (catalog, schema), or None if they are."""
    try:
        tree = parse(sql, dialect)
    except Exception as e:  # noqa: BLE001
        return f"cannot parse it to check where it writes ({type(e).__name__})"
    cls = _class_of_tree(tree)
    if cls not in WRITE_CLASSES:
        return f"it is not a write statement Igloo replays ({cls})"
    found = tables(tree, dialect)
    for w in found.writes:
        cat, sch = (w.written[-3] if len(w.written) == 3 else None), (w.written[-2] if len(w.written) >= 2 else None)
        if sch != allowed_schema[1] or (cat is not None and cat != allowed_schema[0]):
            return f"it writes to {'.'.join(w.written)}, outside the scratch schema"
    # Nothing else in it may write: a MERGE or CTAS holds one statement.
    for node in tree.walk():
        if node is not tree and isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop,
                                                  exp.Command)):
            if isinstance(tree, exp.Merge) and isinstance(node, (exp.Insert, exp.Update, exp.Delete)):
                continue  # MERGE's own WHEN clauses
            return f"it contains a nested {type(node).__name__}"
    return None


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@dataclass
class Translation:
    sql: str | None
    warnings: list[str]
    error: str | None = None


def translate(sql: str, read: str, write: str = "duckdb") -> Translation:
    """The statement in DuckDB's dialect, by sqlglot, with the constructs
    sqlglot translated imperfectly or not at all: a translation with
    warnings can run and still return something else."""
    handler = _Collect()
    logger = logging.getLogger("sqlglot")
    logger.addHandler(handler)
    try:
        try:
            trees = sqlglot.parse(sql, read=read, error_level=ErrorLevel.RAISE)
        except Exception as e:  # noqa: BLE001
            return Translation(None, [], f"sqlglot cannot parse it: {sqltext.first_line(str(e))}")
        trees = [t for t in trees if t is not None]
        if len(trees) != 1:
            return Translation(None, [], f"expected one statement, found {len(trees)}")
        tree = trees[0]
        if isinstance(tree, exp.Command):
            return Translation(None, [], "sqlglot does not understand this statement")
        try:
            out = tree.sql(dialect=write, unsupported_level=ErrorLevel.RAISE)
            warnings: list[str] = []
        except UnsupportedError as e:
            out = tree.sql(dialect=write, unsupported_level=ErrorLevel.IGNORE)
            warnings = [m for m in str(e).split("\n") if m.strip()]
        except Exception as e:  # noqa: BLE001
            return Translation(None, [], f"sqlglot cannot write it: {sqltext.first_line(str(e))}")
        return Translation(out, sorted(set(warnings + handler.messages)))
    finally:
        logger.removeHandler(handler)
