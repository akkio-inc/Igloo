"""Operations on SQL text that work from the tokens, without parsing it:
the shape of a statement, redacting its literals, renaming tables in place.

Working from tokens rather than a parse tree keeps the statement exactly as
it was written apart from what is changed, and works for statements that
sqlglot cannot parse. Comments are not tokens, so they drop out of shapes
and redacted text.
"""

from __future__ import annotations

import hashlib
import re

import sqlglot
from sqlglot.tokens import Token, TokenType

# Tokens whose text is a value written in the statement.
LITERALS = {
    getattr(TokenType, name)
    for name in (
        "STRING", "NUMBER", "NATIONAL_STRING", "RAW_STRING", "NATIONAL_RAW_STRING", "HEREDOC_STRING",
        "HEX_STRING", "BIT_STRING", "BYTE_STRING", "UNICODE_STRING",
    )
    if hasattr(TokenType, name)
}
NAMES = {TokenType.VAR, TokenType.IDENTIFIER}
# Words after which a name is a table's.
TABLE_WORDS = {"INTO", "TABLE", "FROM", "JOIN", "UPDATE", "USING"}

# Functions whose result depends on when, or on chance, rather than on the
# data alone: the same statement run twice need not return the same rows.
TIME_FUNCTIONS = {
    "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME", "LOCALTIMESTAMP", "LOCALTIME", "GETDATE", "SYSDATE",
    "SYSTIMESTAMP", "NOW", "CURRENT_DATETIME", "UNIX_TIMESTAMP", "CURDATE", "TODAY",
}
RANDOM_FUNCTIONS = {"RANDOM", "RAND", "UUID_STRING", "UUID", "SAMPLE", "TABLESAMPLE", "SEQ1", "SEQ2", "SEQ4", "SEQ8",
                    "NORMAL", "UNIFORM", "RANDSTR", "ZIPF", "GEN_RANDOM_UUID"}
# Functions that describe the session or the platform, which differ
# between the source and Igloo by design.
SESSION_FUNCTIONS = {"CURRENT_USER", "CURRENT_ROLE", "CURRENT_DATABASE", "CURRENT_SCHEMA", "CURRENT_SCHEMAS",
                     "CURRENT_WAREHOUSE", "CURRENT_ACCOUNT", "CURRENT_ACCOUNT_NAME", "CURRENT_REGION",
                     "CURRENT_SESSION", "CURRENT_VERSION", "CURRENT_CLIENT", "CURRENT_IP_ADDRESS",
                     "CURRENT_AVAILABLE_ROLES", "CURRENT_SECONDARY_ROLES", "CURRENT_ORGANIZATION_NAME",
                     "CURRENT_CATALOG", "LAST_QUERY_ID", "SESSION_USER", "CURRENT_STATEMENT",
                     "CURRENT_TRANSACTION"}


def tokenize(sql: str, dialect: str) -> list[Token]:
    """The statement's tokens; raises on text the tokenizer cannot read."""
    return sqlglot.Dialect.get_or_raise(dialect).tokenize(sql)


def normalize(sql: str, dialect: str) -> str:
    """The statement's shape: its tokens with literals replaced by ?,
    keywords in upper case, unquoted names in lower case, no comments, and
    lists of values (IN lists, VALUES rows) collapsed so that their length
    does not matter."""
    try:
        tokens = tokenize(sql, dialect)
    except Exception:  # noqa: BLE001 - fall back to a cruder shape
        return _normalize_text(sql)
    while tokens and tokens[-1].token_type == TokenType.SEMICOLON:
        tokens = tokens[:-1]
    out = []
    for t in tokens:
        if t.token_type in LITERALS or t.token_type == TokenType.PLACEHOLDER:
            out.append("?")
        elif t.token_type == TokenType.VAR:
            out.append(t.text.lower())
        elif t.token_type == TokenType.IDENTIFIER:
            out.append('"' + t.text.replace('"', '""') + '"')
        else:
            out.append(t.text.upper())
    return _collapse_lists(" ".join(out))


def _collapse_lists(text: str) -> str:
    text = re.sub(r"\bIN \( \?(?: , \?)* \)", "IN ( ?+ )", text)
    # VALUES (?, ?), (?, ?), ... -> one row.
    return re.sub(r"(\( [^()]* \))(?: , \1)+", r"\1 , ...", text)


def _normalize_text(sql: str) -> str:
    """A shape for text the tokenizer rejects: comments, string and number
    literals and runs of whitespace removed."""
    text = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S)
    text = re.sub(r"'(?:[^']|'')*'", "?", text)
    text = re.sub(r"(?<![\w.])-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?\b", "?", text)
    return _collapse_lists(" ".join(re.findall(r"\w+|[^\w\s]", text.strip().rstrip(";"))).lower())


def shape_hash(normalized: str) -> str:
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def redact(sql: str, dialect: str) -> str:
    """The statement with every literal replaced by ? and comments removed,
    otherwise as written."""
    try:
        tokens = tokenize(sql, dialect)
    except Exception:  # noqa: BLE001
        return _redact_text(sql)
    out, pos = [], 0
    for t in tokens:
        out.append(_strip_comments(sql[pos:t.start]))
        out.append("?" if t.token_type in LITERALS else sql[t.start:t.end + 1])
        pos = t.end + 1
    out.append(_strip_comments(sql[pos:]))
    return re.sub(r"[ \t]+\n", "\n", "".join(out)).strip()


def _strip_comments(gap: str) -> str:
    # Between tokens there is only whitespace and comments (-- , // and /* */).
    return re.sub(r"--[^\n]*|//[^\n]*|/\*.*?\*/", "", gap, flags=re.S)


def _redact_text(sql: str) -> str:
    text = _strip_comments(sql)
    text = re.sub(r"'(?:[^']|'')*'|\$\$.*?\$\$", "?", text, flags=re.S)
    return re.sub(r"(?<![\w.])\d+(?:\.\d+)?(?:[eE][-+]?\d+)?\b", "?", text).strip()


def redact_message(message: str | None) -> str | None:
    """An error message without the values it quotes: quoted strings
    become '?', and the excerpt of the statement that DuckDB and
    PostgreSQL append (LINE n: ...) is dropped."""
    if message is None:
        return None
    message = re.split(r"\n\s*LINE \d+:", message)[0]
    message = re.sub(r"'(?:[^']|'')*'", "'?'", message)
    return message.strip()


def first_line(message: str | None, limit: int = 300) -> str | None:
    if message is None:
        return None
    line = message.strip().splitlines()[0] if message.strip() else ""
    return line[:limit]


def volatility(sql: str, dialect: str) -> set[str]:
    """'time' if the statement's result depends on when it runs, 'random' if
    it depends on chance, 'session' if on who runs it and where."""
    try:
        words = {t.text.upper() for t in tokenize(sql, dialect) if t.token_type not in LITERALS}
    except Exception:  # noqa: BLE001
        words = {w.upper() for w in re.findall(r"\w+", _strip_comments(sql))}
    flags = set()
    if words & TIME_FUNCTIONS:
        flags.add("time")
    if words & RANDOM_FUNCTIONS:
        flags.add("random")
    if words & SESSION_FUNCTIONS:
        flags.add("session")
    return flags


def name_runs(tokens: list[Token]) -> list[tuple[int, int, list[str]]]:
    """Runs of dotted names (a, a.b, a.b.c, ...): (first token index, last
    token index, the names in lower case)."""
    runs, i = [], 0
    while i < len(tokens):
        if tokens[i].token_type in NAMES and (i == 0 or tokens[i - 1].token_type != TokenType.DOT):
            j = i
            while (j + 2 < len(tokens) and tokens[j + 1].token_type == TokenType.DOT
                   and tokens[j + 2].token_type in NAMES):
                j += 2
            runs.append((i, j, [tokens[k].text.lower() for k in range(i, j + 1, 2)]))
            i = j + 1
        else:
            i += 1
    return runs


def rename_tables(sql: str, dialect: str, renames: dict[tuple[str, ...], str]) -> str:
    """Rewrites table names in the text and leaves everything else as
    written. renames maps a name as written, in lower-case parts (('db',
    'schema', 'table') or ('table',)), to its replacement text. A name is
    replaced wherever it starts a dotted run, so column references
    qualified by it (orders.o_orderkey) follow. A one-part name followed by
    a parenthesis is a function call, and is not replaced, unless it comes
    where a table does (INSERT INTO t (a, b))."""
    if not renames:
        return sql
    tokens = tokenize(sql, dialect)
    edits = []
    for first, last, parts in name_runs(tokens):
        for n in (3, 2, 1):
            key = tuple(parts[:n])
            if len(parts) >= n and key in renames:
                end = first + 2 * (n - 1)
                called = end + 1 < len(tokens) and tokens[end + 1].token_type == TokenType.L_PAREN
                after_table_word = first > 0 and tokens[first - 1].text.upper() in TABLE_WORDS
                if n == 1 and called and not after_table_word:
                    break
                edits.append((tokens[first].start, tokens[end].end + 1, renames[key]))
                break
    out, pos = [], 0
    for start, end, text in edits:
        out.append(sql[pos:start])
        out.append(text)
        pos = end
    out.append(sql[pos:])
    return "".join(out)
