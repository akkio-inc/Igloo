// A small SQL tokenizer for syntax highlighting. It follows the same
// lexical rules as the server's (internal/sqllex): '...' and E'...' strings,
// $tag$ quotes, "identifiers", -- and nested /* */ comments.

const KEYWORDS = new Set(`
ADD ALL ALTER ANALYZE AND ANTI ANY AS ASC ASOF AT ATTACH BEGIN BETWEEN BIGINT BOOLEAN BOTH BY CALL CASCADE CASE CAST
CHECK COLLATE COLUMN COLUMNS COMMENT COMMIT CONSTRAINT COPY CREATE CROSS CUBE CURRENT CURRENT_DATE CURRENT_TIMESTAMP
CURRENT_USER CURSOR DATABASE DATE DECIMAL DECLARE DEFAULT DELETE DESC DESCRIBE DETACH DISTINCT DO DOUBLE DROP ELSE END
ESCAPE EXCEPT EXCLUDE EXISTS EXPLAIN EXPORT EXTRACT FALSE FETCH FILTER FIRST FLOAT FOLLOWING FOR FOREIGN FROM FULL
FUNCTION GLOB GRANT GROUP GROUPING HAVING IF ILIKE IN INDEX INNER INSERT INSTALL INTEGER INTERSECT INTERVAL INTO IS
JOIN JSON KEY LAST LATERAL LEADING LEFT LIKE LIMIT LOAD MACRO MAP MATERIALIZED MERGE NATURAL NEXT NOT NOTHING NULL
NULLS NUMERIC OF OFFSET ON ONLY OR ORDER OUTER OVER PARTITION PASSWORD PIVOT POSITIONAL PRAGMA PRECEDING PRIMARY
QUALIFY RANGE RECURSIVE REFERENCES RENAME REPLACE RESET RETURNING REVOKE RIGHT ROLE ROLLBACK ROLLUP ROW ROWS SAMPLE
SCHEMA SELECT SEMI SEQUENCE SESSION SET SETS SHOW SIMILAR SMALLINT SOME STRUCT SUMMARIZE TABLE TABLESAMPLE TEMP
TEMPORARY THEN TIME TIMESTAMP TO TRAILING TRANSACTION TRUE TRUNCATE TRY_CAST TYPE UNBOUNDED UNION UNIQUE UNNEST
UNPIVOT UPDATE USE USER USERS USING VACUUM VALUES VARCHAR VIEW WHEN WHERE WINDOW WITH WITHIN WITHOUT
`.trim().split(/\s+/));

function closeQuote(s, i, q, escapes) {
  for (let j = i + 1; j < s.length; j++) {
    if (escapes && s[j] === '\\') { j++; continue; }
    if (s[j] === q) {
      if (s[j + 1] === q) { j++; continue; }
      return j + 1;
    }
  }
  return s.length;
}

function closeComment(s, i) {
  let depth = 0;
  for (let j = i; j + 1 < s.length; j++) {
    const two = s.slice(j, j + 2);
    if (two === '/*') { depth++; j++; } else if (two === '*/') {
      depth--; j++;
      if (depth === 0) return j + 1;
    }
  }
  return s.length;
}

const identChar = /[A-Za-z0-9_$\u0080-￿]/;

// tokenize splits sql into [type, text] pairs, where type is one of
// keyword, string, number, comment, ident, function or '' for the rest.
export function tokenize(sql) {
  const out = [];
  let plain = '';
  const push = (type, text) => {
    if (type === '') { plain += text; return; }
    if (plain) { out.push(['', plain]); plain = ''; }
    out.push([type, text]);
  };
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    let end;
    if (c === "'") {
      const escapes = i > 0 && /[Ee]/.test(sql[i - 1]) && (i < 2 || !identChar.test(sql[i - 2]));
      end = closeQuote(sql, i, "'", escapes);
      push('string', sql.slice(i, end));
    } else if (c === '"') {
      end = closeQuote(sql, i, '"', false);
      push('ident', sql.slice(i, end));
    } else if (c === '-' && sql[i + 1] === '-') {
      end = sql.indexOf('\n', i);
      if (end < 0) end = sql.length;
      push('comment', sql.slice(i, end));
    } else if (c === '/' && sql[i + 1] === '*') {
      end = closeComment(sql, i);
      push('comment', sql.slice(i, end));
    } else if (c === '$' && (i === 0 || !identChar.test(sql[i - 1]))) {
      const m = /^\$([A-Za-z_\u0080-￿][A-Za-z0-9_\u0080-￿]*)?\$/.exec(sql.slice(i));
      if (m) {
        const close = sql.indexOf(m[0], i + m[0].length);
        end = close < 0 ? sql.length : close + m[0].length;
        push('string', sql.slice(i, end));
      } else {
        end = i + 1;
        push('', c);
      }
    } else if (/[0-9]/.test(c) || (c === '.' && /[0-9]/.test(sql[i + 1] || ''))) {
      const m = /^(?:\d[\d_]*(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(sql.slice(i));
      end = i + m[0].length;
      push('number', m[0]);
    } else if (/[A-Za-z_\u0080-￿]/.test(c)) {
      end = i + 1;
      while (end < sql.length && identChar.test(sql[end])) end++;
      const word = sql.slice(i, end);
      let k = end;
      while (k < sql.length && (sql[k] === ' ' || sql[k] === '\t')) k++;
      if (KEYWORDS.has(word.toUpperCase())) push('keyword', word);
      else if (sql[k] === '(') push('function', word);
      else push('', word);
    } else {
      end = i + 1;
      push('', c);
    }
    i = end;
  }
  if (plain) out.push(['', plain]);
  return out;
}
