// Query and sign-in history, read with SQL from igloo.query_history and
// igloo.login_history as the signed-in user. Older versions of Igloo do not
// have them; the page says so instead of failing. Column names are matched
// from a list of likely ones, so the page adapts to what the views offer.

import * as api from './api.js';
import { h, clear, icon, button, fmtMs, fmtTime, plural, mark } from './dom.js';

// SQLSTATEs meaning the view is not there: no such table or schema, or a
// statement this version does not understand.
const MISSING = new Set(['42P01', '3F000', '42704', '0A000', '42601', '42883']);

const LIMIT = 500;

// LOOKUPS is the application name the console's own lookups run under
// (see internal/console/query.go): what its pages read to show
// themselves, such as a catalog page's reads of igloo.tables. The page
// leaves them out unless asked.
const LOOKUPS = 'Igloo console lookup';

const SOURCES = {
  queries: {
    table: 'igloo.query_history',
    title: 'Queries',
    heading: 'Query history',
    noun: 'query',
    fields: {
      time: ['start_time', 'started_at', 'query_start', 'start', 'time', 'timestamp', 'at', 'created_at', 'end_time'],
      user: ['user_name', 'username', 'user', 'usename'],
      outcome: ['outcome', 'status', 'execution_status', 'state', 'result'],
      text: ['query_text', 'query', 'statement', 'sql_text', 'sql', 'text'],
      app: ['application_name'],
      duration: ['duration_ms', 'elapsed_ms', 'total_elapsed_time', 'duration', 'elapsed'],
      rows: ['rows', 'rows_produced', 'row_count', 'rows_returned', 'rows_affected'],
      code: ['sqlstate', 'error_code', 'code'],
      error: ['error_message', 'error', 'message'],
    },
    list: ['time', 'user', 'outcome', 'duration', 'rows', 'text'],
  },
  logins: {
    table: 'igloo.login_history',
    title: 'Sign-ins',
    heading: 'Sign-in history',
    noun: 'sign-in',
    fields: {
      time: ['event_time', 'login_time', 'time', 'timestamp', 'at', 'created_at', 'start_time'],
      user: ['user_name', 'username', 'user', 'usename'],
      outcome: ['outcome', 'status', 'result', 'success', 'is_success'],
      client: ['client_address', 'client', 'client_ip', 'address', 'addr', 'remote_address', 'source'],
      error: ['reason', 'error_message', 'error', 'message'],
    },
    list: ['time', 'user', 'outcome', 'client', 'error'],
  },
};

const OUTCOMES = {
  ok: ['ok', 'success', 'succeeded', 'completed', 'true', 't'],
  error: ['error', 'failed', 'failure', 'fail', 'false', 'f'],
  denied: ['denied'],
  canceled: ['canceled', 'cancelled'],
};

const LABELS = { time: 'Time', user: 'User', outcome: 'Outcome', text: 'Query', duration: 'Duration', rows: 'Rows', client: 'Client', error: 'Reason' };

export function createHistory(app) {
  let source = 'queries';
  const shape = {}; // source -> {available, columns, map} once probed
  let rows = [];
  let columns = [];
  let selected = -1;

  const userInput = h('input', { placeholder: 'Any user', 'aria-label': 'User', autocapitalize: 'none', spellcheck: 'false' });
  const outcome = h('select', { 'aria-label': 'Outcome' },
    h('option', { value: '', text: 'Any outcome' }), h('option', { value: 'ok', text: 'Succeeded' }),
    h('option', { value: 'error', text: 'Failed' }), h('option', { value: 'denied', text: 'Denied' }),
    h('option', { value: 'canceled', text: 'Canceled' }));
  const range = h('select', { 'aria-label': 'Time range' },
    h('option', { value: '1', text: 'Last hour' }), h('option', { value: '24', text: 'Last 24 hours', selected: true }),
    h('option', { value: '168', text: 'Last 7 days' }), h('option', { value: '720', text: 'Last 30 days' }),
    h('option', { value: 'custom', text: 'Custom range' }));
  const from = h('input', { type: 'datetime-local', 'aria-label': 'From' });
  const to = h('input', { type: 'datetime-local', 'aria-label': 'To' });
  const search = h('input', { type: 'search', placeholder: 'Text in the query', 'aria-label': 'Search query text' });
  const customFields = [h('div', { class: 'field' }, h('label', { text: 'From' }), from), h('div', { class: 'field' }, h('label', { text: 'To' }), to)];
  const searchField = h('div', { class: 'field grow' }, h('label', { text: 'Search' }), search);
  const lookups = h('input', { type: 'checkbox', 'aria-label': 'Show the console\'s own lookups' });
  const lookupsField = h('label', {
    class: 'check', title: `Queries the console runs to show its pages, such as a catalog page's reads of igloo.tables (application_name '${LOOKUPS}')`,
  }, lookups, 'Console lookups');
  const outcomeField = h('div', { class: 'field' }, h('label', { text: 'Outcome' }), outcome);
  const userField = h('div', { class: 'field' }, h('label', { text: 'User' }), userInput);
  const filters = h('form', {
    class: 'filters',
    onsubmit: (e) => { e.preventDefault(); refresh(); },
  }, userField, outcomeField, h('div', { class: 'field' }, h('label', { text: 'Time' }), range), customFields, searchField,
  lookupsField, h('button', { class: 'btn primary', type: 'submit' }, icon('search'), 'Apply'));

  const toggle = h('div', { class: 'segmented', role: 'group', 'aria-label': 'History' },
    Object.entries(SOURCES).map(([key, s]) => h('button', {
      type: 'button', 'aria-pressed': String(key === source), dataset: { key }, text: s.title,
      onclick: () => { source = key; for (const b of toggle.children) b.setAttribute('aria-pressed', String(b.dataset.key === key)); refresh(); },
    })));
  const count = h('span', { class: 'sub' });
  const list = h('div', { class: 'history-list' });
  const detail = h('aside', { class: 'history-detail', 'aria-label': 'Details' });
  const layout = h('div', { class: 'history-layout' }, list, detail);
  const body = h('div', { class: 'page', hidden: false });
  const el = h('section', { class: 'page history', 'aria-label': 'Query history' },
    h('div', { class: 'page-head' }, h('h1', { text: 'History' }), toggle, count, h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => refresh() })),
    body);

  range.addEventListener('change', () => {
    for (const f of customFields) f.hidden = range.value !== 'custom';
    if (range.value !== 'custom') refresh();
  });
  outcome.addEventListener('change', () => refresh());
  lookups.addEventListener('change', () => refresh());
  for (const f of customFields) f.hidden = true;

  async function probe(key) {
    if (shape[key]) return shape[key];
    const src = SOURCES[key];
    try {
      const res = await api.lookup(`SELECT * FROM ${src.table} LIMIT 0`);
      const names = res.columns.map((c) => c.name);
      const map = {};
      for (const [field, candidates] of Object.entries(src.fields)) {
        map[field] = candidates.find((c) => names.includes(c)) || null;
      }
      shape[key] = { available: true, columns: res.columns, map };
    } catch (e) {
      if (MISSING.has(e.code)) shape[key] = { available: false, error: e };
      else throw e;
    }
    return shape[key];
  }

  function where(s) {
    const m = s.map;
    const parts = [];
    const user = userInput.value.trim();
    if (user && m.user) parts.push(`${api.ident(m.user)} = ${api.literal(user)}`);
    if (outcome.value && m.outcome) {
      parts.push(`lower(CAST(${api.ident(m.outcome)} AS VARCHAR)) IN (${OUTCOMES[outcome.value].map(api.literal).join(', ')})`);
    }
    if (m.time) {
      if (range.value === 'custom') {
        if (from.value) parts.push(`${api.ident(m.time)} >= CAST(${api.literal(new Date(from.value).toISOString())} AS TIMESTAMPTZ)`);
        if (to.value) parts.push(`${api.ident(m.time)} <= CAST(${api.literal(new Date(to.value).toISOString())} AS TIMESTAMPTZ)`);
      } else {
        parts.push(`${api.ident(m.time)} >= now() - INTERVAL ${Number(range.value)} HOUR`);
      }
    }
    if (m.app && !lookups.checked) parts.push(`coalesce(${api.ident(m.app)}, '') <> ${api.literal(LOOKUPS)}`);
    const text = search.value.trim();
    if (text && m.text) parts.push(`contains(lower(${api.ident(m.text)}), lower(${api.literal(text)}))`);
    return parts.length ? `\nWHERE ${parts.join('\n  AND ')}` : '';
  }

  async function refresh() {
    const src = SOURCES[source];
    clear(body, h('div', { class: 'result-message', text: 'Loading…' }));
    count.textContent = '';
    let s;
    try {
      s = await probe(source);
    } catch (e) {
      clear(body, errorView(e));
      return;
    }
    if (!s.available) {
      clear(body, h('div', { class: 'empty' },
        h('span', { class: 'mark' }, mark()),
        h('h2', { text: `${src.heading} is not available in this version of Igloo` }),
        h('p', {}, 'This page reads ', h('code', { text: src.table }), ', which this server does not have yet. It works as soon as the server is upgraded to a version that records history.')));
      return;
    }
    searchField.hidden = !s.map.text;
    lookupsField.hidden = !s.map.app;
    outcomeField.hidden = !s.map.outcome;
    userField.hidden = !s.map.user;
    const order = s.map.time ? `\nORDER BY ${api.ident(s.map.time)} DESC` : '';
    try {
      const res = await api.lookup(`SELECT * FROM ${src.table}${where(s)}${order}\nLIMIT ${LIMIT}`);
      columns = res.columns;
      rows = api.objects(res);
    } catch (e) {
      clear(body, filters, errorView(e));
      return;
    }
    selected = -1;
    count.textContent = rows.length === LIMIT ? `latest ${LIMIT} ${src.noun === 'query' ? 'queries' : `${src.noun}s`}` : plural(rows.length, src.noun, src.noun === 'query' ? 'queries' : `${src.noun}s`);
    clear(body, filters, layout);
    renderList(s);
  }

  function cellFor(field, s, row) {
    const col = s.map[field];
    const v = col ? row[col] : null;
    switch (field) {
      case 'time': return h('td', { class: 'nowrap', text: fmtTime(v) });
      case 'outcome': return h('td', {}, v === null || v === undefined ? '' : h('span', { class: `badge ${outcomeClass(v)}`, text: outcomeLabel(v) }));
      case 'duration': return h('td', { class: 'num nowrap', text: durationText(col, v) });
      case 'rows': return h('td', { class: 'num', text: v === null || v === undefined ? '' : Number(v).toLocaleString() });
      case 'text': return h('td', { class: 'fill' }, h('div', { class: 'truncate mono', text: (v || '').replace(/\s+/g, ' ') }));
      default: return h('td', { class: field === 'error' ? 'truncate' : '', text: v ?? '' });
    }
  }

  function renderList(s) {
    const src = SOURCES[source];
    const fields = src.list.filter((f) => s.map[f]);
    layout.classList.toggle('with-detail', selected >= 0);
    detail.hidden = selected < 0;
    if (!rows.length) {
      clear(list, h('div', { class: 'empty' }, h('h2', { text: 'Nothing found' }), h('p', { text: 'Nothing matches these filters.' })));
      return;
    }
    if (!fields.length) {
      // Unknown columns: show them all as they are.
      clear(list, h('table', { class: 'list' },
        h('thead', {}, h('tr', {}, columns.map((c) => h('th', { text: c.name })))),
        h('tbody', {}, rows.map((r, i) => h('tr', { class: `clickable${i === selected ? ' selected' : ''}`, onclick: () => pick(i, s) },
          columns.map((c) => h('td', { class: 'truncate', text: r[c.name] ?? '' })))))));
      return;
    }
    clear(list, h('table', { class: 'list' },
      h('thead', {}, h('tr', {}, fields.map((f) => h('th', { class: f === 'duration' || f === 'rows' ? 'num' : '', text: LABELS[f] })))),
      h('tbody', {}, rows.map((r, i) => h('tr', {
        class: `clickable${i === selected ? ' selected' : ''}`, tabindex: '0',
        onclick: () => pick(i, s),
        onkeydown: (e) => { if (e.key === 'Enter') pick(i, s); },
      }, fields.map((f) => cellFor(f, s, r)))))));
  }

  function pick(i, s) {
    selected = i;
    renderList(s);
    const row = rows[i];
    const textCol = s.map.text;
    clear(detail,
      h('div', { class: 'row' }, h('h2', { text: SOURCES[source].noun === 'query' ? 'Query' : 'Sign-in' }), h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost icon-only small', type: 'button', 'aria-label': 'Close details', onclick: () => { selected = -1; renderList(s); } }, icon('x'))),
      textCol && row[textCol] ? [h('pre', { class: 'mono', text: row[textCol] }),
        h('div', { class: 'row' }, button('Open in a worksheet', { icon: 'open', onclick: () => app.openWorksheet(row[textCol]) }))] : null,
      h('dl', {}, columns.filter((c) => c.name !== textCol).map((c) => [
        h('dt', { text: c.name }),
        h('dd', { class: c.type.startsWith('timestamp') ? '' : 'mono', text: c.type.startsWith('timestamp') ? fmtTime(row[c.name]) : row[c.name] ?? 'NULL' }),
      ])));
  }

  function errorView(e) {
    return h('div', { class: 'sql-error' },
      h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
      h('pre', { text: e.message }));
  }

  let loaded = false;
  return {
    el,
    shown() {
      if (!loaded) { loaded = true; refresh(); }
    },
  };
}

function outcomeLabel(v) {
  const s = String(v).toLowerCase();
  if (s === 'true' || s === 't') return 'ok';
  if (s === 'false' || s === 'f') return 'failed';
  return String(v);
}

function outcomeClass(v) {
  const s = String(v).toLowerCase();
  if (OUTCOMES.ok.includes(s)) return 'ok';
  if (OUTCOMES.error.includes(s)) return 'error';
  return 'warn';
}

function durationText(col, v) {
  if (v === null || v === undefined || v === '') return '';
  const n = Number(v);
  if (Number.isNaN(n)) return String(v); // an interval, as text
  if (/_ms$|elapsed_time$/.test(col)) return fmtMs(n);
  if (/_us$/.test(col)) return fmtMs(n / 1000);
  return fmtMs(n * 1000);
}
