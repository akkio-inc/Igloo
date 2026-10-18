// Requests to the console's API. Requests that change anything carry the
// session's CSRF token in a header; it lives only in this module's memory.

let csrf = '';
let onSignedOut = () => {};

export function setSession(token, signedOut) {
  csrf = token;
  if (signedOut) onSignedOut = signedOut;
}

// APIError is a failed request; code is a SQLSTATE when SQL failed.
export class APIError extends Error {
  constructor(status, body = {}) {
    super(body.error || body.message || `request failed (${status})`);
    this.status = status;
    this.code = body.code || '';
    this.detail = body.detail || '';
    this.hint = body.hint || '';
    this.body = body;
  }
}

async function fail(res) {
  const body = await res.json().catch(() => ({ error: res.statusText }));
  if (res.status === 401) onSignedOut(body);
  return new APIError(res.status, body);
}

export async function get(path, { signal } = {}) {
  const res = await fetch(path, { headers: { Accept: 'application/json' }, credentials: 'same-origin', signal });
  if (!res.ok) throw await fail(res);
  return res.json();
}

export async function post(path, body, { signal, raw = false } = {}) {
  const res = await fetch(path, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok) throw await fail(res);
  return raw ? res : res.json();
}

// stream runs a script and calls onEvent with each event the server sends
// (see internal/console/query.go). Aborting signal cancels the statement.
// lookup marks a query a page makes to show itself, rather than one the
// user ran: the history records it under its own application name, and
// the history page leaves it out unless asked.
export async function stream(sql, onEvent, signal, { lookup = false } = {}) {
  const res = await post('/api/query', lookup ? { sql, lookup } : { sql }, { signal, raw: true });
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += value;
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.trim()) onEvent(JSON.parse(line));
    }
  }
}

// SQLError is a statement that failed.
export class SQLError extends Error {
  constructor(e) {
    super(e.message);
    this.code = e.code;
    this.detail = e.detail || '';
    this.hint = e.hint || '';
  }
}

// queryAll runs a script and resolves to the results of its statements:
// {columns: [{name, type}], rows, tag}. The first failure rejects with a
// SQLError.
export async function queryAll(sql, signal, opts) {
  const results = [];
  let failure = null;
  await stream(sql, (ev) => {
    if (ev.statement) results.push({ columns: [], rows: [], tag: '' });
    const cur = results[results.length - 1];
    if (ev.columns) cur.columns = ev.columns;
    else if (ev.rows) for (const r of ev.rows) cur.rows.push(r);
    else if (ev.complete) {
      cur.tag = ev.complete.tag;
      cur.truncated = ev.complete.truncated;
      cur.ms = ev.complete.ms;
    }
    else if (ev.error) failure = new SQLError(ev.error);
  }, signal, opts);
  if (failure) throw failure;
  return results;
}

export async function query(sql, signal, opts) {
  const results = await queryAll(sql, signal, opts);
  return results[results.length - 1];
}

// lookup and lookupAll are query and queryAll for a page's own lookups
// (see stream).
export function lookup(sql, signal) {
  return query(sql, signal, { lookup: true });
}

export function lookupAll(sql, signal) {
  return queryAll(sql, signal, { lookup: true });
}

// rowsAsObjects maps a result's rows to objects keyed by column name.
export function objects(result) {
  return result.rows.map((row) => Object.fromEntries(result.columns.map((c, i) => [c.name, row[i]])));
}

// download runs a query again and saves its whole result as CSV, streaming
// it to a file where the browser can, or else collecting it first.
// onProgress receives the bytes received so far.
export async function download(sql, { signal, onProgress = () => {} } = {}) {
  const name = `igloo-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.csv`;
  let file = null;
  if (window.showSaveFilePicker) {
    try {
      const handle = await window.showSaveFilePicker({
        suggestedName: name,
        types: [{ description: 'CSV', accept: { 'text/csv': ['.csv'] } }],
      });
      file = await handle.createWritable();
    } catch (e) {
      if (e.name === 'AbortError') return false;
      file = null;
    }
  }
  const res = await post('/api/download', { sql }, { signal, raw: true });
  const reader = res.body.getReader();
  const chunks = [];
  let bytes = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.length;
      onProgress(bytes);
      if (file) await file.write(value);
      else chunks.push(value);
    }
  } catch (e) {
    if (file) await file.abort().catch(() => {});
    throw e;
  }
  if (file) {
    await file.close();
    return true;
  }
  const url = URL.createObjectURL(new Blob(chunks, { type: 'text/csv' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
  return true;
}

// SQL quoting for statements the console builds from what users pick.

export function ident(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

export function literal(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}
