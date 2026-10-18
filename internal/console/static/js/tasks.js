// Tasks, pipes, materialized views and alerts with their runs, and the
// load history of COPY INTO, read with SQL as the signed-in user: SHOW
// TASKS, SHOW PIPES, SHOW MATERIALIZED VIEWS, SHOW ALERTS,
// igloo.task_history, igloo.alert_history and igloo.load_history. Admins
// see everyone's; other users see their own, which the server decides.
// Older versions of Igloo have none of these; the pages say so instead of
// failing.

import * as api from './api.js';
import { h, clear, icon, button, fmtMs, fmtTime, fmtAgo, plural, mark, toast, confirmDialog } from './dom.js';

// SQLSTATEs meaning the statement or view is not there.
const MISSING = new Set(['42P01', '3F000', '42704', '0A000', '42601', '42883']);

const KINDS = {
  task: {
    show: 'SHOW TASKS', title: 'Tasks', noun: 'task', sql: 'TASK',
    run: { label: 'Run now', sql: (n) => `EXECUTE TASK ${n}`, done: 'Ran' },
  },
  pipe: { show: 'SHOW PIPES', title: 'Pipes', noun: 'pipe', sql: 'PIPE' },
  'materialized view': {
    show: 'SHOW MATERIALIZED VIEWS', title: 'Materialized views', noun: 'materialized view', sql: 'MATERIALIZED VIEW',
    run: { label: 'Refresh', sql: (n) => `REFRESH MATERIALIZED VIEW ${n}`, done: 'Refreshed' },
  },
  alert: {
    show: 'SHOW ALERTS', title: 'Alerts', noun: 'alert', sql: 'ALERT',
    run: { label: 'Run now', sql: (n) => `EXECUTE ALERT ${n}`, done: 'Ran' },
  },
};

const LIMIT = 500;

// sqlName quotes a job's name for SQL: a materialized view's is
// schema.table.
function sqlName(kind, name) {
  return kind === 'materialized view' ? name.split('.').map(api.ident).join('.') : api.ident(name);
}

function outcomeBadge(v) {
  if (v === null || v === undefined) return null;
  const s = String(v);
  const cls = {
    succeeded: 'ok', loaded: 'ok', failed: 'error', unknown: 'error', running: 'warn', loading: 'warn', skipped: '', 'partially loaded': 'warn',
    triggered: 'warn', condition_false: 'ok', condition_failed: 'error', action_failed: 'error',
  }[s] ?? '';
  return h('span', { class: `badge ${cls}`.trim(), text: s });
}

function errorView(e) {
  return h('div', { class: 'sql-error', role: 'alert' },
    h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
    h('pre', { text: e.message }));
}

function unavailable(what, source) {
  return h('div', { class: 'empty' },
    h('span', { class: 'mark' }, mark()),
    h('h2', { text: `${what} are not available in this version of Igloo` }),
    h('p', {}, 'This page reads ', h('code', { text: source }), ', which this server does not have yet.'));
}

function num(v) {
  return v === null || v === undefined ? '' : Number(v).toLocaleString();
}

// Tasks, pipes and materialized views.

export function createJobs(app) {
  let kind = 'task';
  let jobs = [];
  let selected = null;

  const toggle = h('div', { class: 'segmented', role: 'group', 'aria-label': 'Kind' },
    Object.entries(KINDS).map(([key, k]) => h('button', {
      type: 'button', 'aria-pressed': String(key === kind), dataset: { key }, text: k.title,
      onclick: () => {
        kind = key;
        selected = null;
        for (const b of toggle.children) b.setAttribute('aria-pressed', String(b.dataset.key === key));
        refresh();
      },
    })));
  const count = h('span', { class: 'sub' });
  const list = h('div', { class: 'history-list' });
  const detail = h('aside', { class: 'history-detail', 'aria-label': 'Details' });
  const layout = h('div', { class: 'history-layout' }, list, detail);
  const body = h('div', { class: 'page' });
  const el = h('section', { class: 'page history', 'aria-label': 'Tasks' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Tasks' }), toggle, count, h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => refresh() })),
    body);

  async function refresh() {
    const k = KINDS[kind];
    clear(body, h('div', { class: 'result-message', text: 'Loading…' }));
    count.textContent = '';
    try {
      jobs = api.objects(await api.lookup(k.show));
    } catch (e) {
      clear(body, MISSING.has(e.code) ? unavailable(k.title, k.show) : errorView(e));
      return;
    }
    count.textContent = plural(jobs.length, k.noun, `${k.noun}s`);
    if (selected && !jobs.some((j) => j.name === selected)) selected = null;
    clear(body, layout);
    renderList();
    if (selected) showDetail(jobs.find((j) => j.name === selected));
  }

  function renderList() {
    const k = KINDS[kind];
    layout.classList.toggle('with-detail', selected !== null);
    detail.hidden = selected === null;
    if (!jobs.length) {
      const hint = {
        task: 'CREATE TASK nightly SCHEDULE \'0 6 * * *\' AS INSERT INTO ...',
        pipe: 'CREATE PIPE drops AS COPY INTO sales FROM \'s3://landing/sales/\'',
        'materialized view': 'CREATE MATERIALIZED VIEW totals SCHEDULE \'1 hour\' AS SELECT ...',
        alert: 'CREATE ALERT pacing SCHEDULE \'0 9 * * *\' IF (EXISTS (SELECT ...)) THEN NOTIFY slack_ops',
      }[kind];
      clear(list, h('div', { class: 'empty' }, h('h2', { text: `No ${k.noun}s yet` }),
        h('p', {}, app.isAdmin ? 'Create one in a worksheet, such as ' : 'You have none. Create one in a worksheet, such as ', h('code', { text: hint }))));
      return;
    }
    const now = Date.now();
    clear(list, h('table', { class: 'list' },
      h('thead', {}, h('tr', {}, ['Name', 'Owner', 'State', 'Schedule', 'Next run', 'Last run'].map((t) => h('th', { text: t })))),
      h('tbody', {}, jobs.map((j) => h('tr', {
        class: `clickable${j.name === selected ? ' selected' : ''}`, tabindex: '0',
        onclick: () => pick(j),
        onkeydown: (e) => { if (e.key === 'Enter') pick(j); },
      },
      h('td', {}, h('span', { class: 'mono', text: j.name })),
      h('td', { class: 'mono muted', text: j.owner }),
      h('td', {}, h('span', { class: `badge ${j.state === 'started' ? 'ok' : ''}`.trim(), text: j.state })),
      h('td', { class: 'mono', text: j.schedule ? `${j.schedule}${j.timezone && j.timezone !== 'UTC' ? ` (${j.timezone})` : ''}` : 'on demand' }),
      h('td', { class: 'nowrap muted', text: j.next_run ? fmtTime(j.next_run) : '' }),
      h('td', { class: 'nowrap' }, j.last_run ? [outcomeBadge(j.last_outcome), ' ', h('span', { class: 'muted', text: fmtAgo(j.last_run, now) })] : ''))))));
  }

  function pick(j) {
    selected = j.name;
    renderList();
    showDetail(j);
  }

  async function act(sql, success) {
    try {
      await api.query(sql);
      toast(success);
    } catch (e) {
      toast(e.message, 'error');
    }
    refresh();
  }

  async function showDetail(j) {
    if (!j) return;
    const k = KINDS[kind];
    const name = sqlName(kind, j.name);
    const actions = [];
    if (k.run) {
      actions.push(button(k.run.label, {
        icon: 'play', kind: 'small',
        onclick: async () => {
          if (!(await confirmDialog(`${k.run.label}: ${j.name}`, `This runs the ${k.noun} now, as ${j.owner}, with ${j.owner}'s privileges.`, { confirm: k.run.label }))) return;
          toast(`Running ${j.name}…`);
          act(k.run.sql(name), `${k.run.done} ${j.name}`);
        },
      }));
    }
    if (j.schedule) {
      actions.push(j.state === 'started'
        ? button('Suspend', { icon: 'stop', kind: 'small', onclick: () => act(`ALTER ${k.sql} ${name} SUSPEND`, `Suspended ${j.name}`) })
        : button('Resume', { icon: 'play', kind: 'small', onclick: () => act(`ALTER ${k.sql} ${name} RESUME`, `Resumed ${j.name}`) }));
    }
    const runs = h('div', {}, h('p', { class: 'muted', text: 'Loading runs…' }));
    clear(detail,
      h('div', { class: 'row' }, h('h2', { text: j.name }), h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost icon-only small', type: 'button', 'aria-label': 'Close details', onclick: () => { selected = null; renderList(); } }, icon('x'))),
      h('div', { class: 'row' }, actions),
      h('pre', { class: 'mono', text: j.definition }),
      h('div', { class: 'row' }, button('Open in a worksheet', { icon: 'open', onclick: () => app.openWorksheet(j.definition) })),
      h('dl', {},
        h('dt', { text: 'Owner' }), h('dd', { class: 'mono', text: j.owner }),
        h('dt', { text: 'State' }), h('dd', { text: j.state }),
        h('dt', { text: 'Schedule' }), h('dd', { class: 'mono', text: j.schedule ? `${j.schedule} (${j.timezone || 'UTC'})` : 'on demand' }),
        h('dt', { text: 'Next run' }), h('dd', { text: j.next_run ? fmtTime(j.next_run) : '—' }),
        h('dt', { text: 'Created' }), h('dd', { text: fmtTime(j.created_at) })),
      h('h3', { text: kind === 'pipe' ? 'Loads' : 'Runs' }),
      runs);
    try {
      if (kind === 'pipe') {
        const res = api.objects(await api.lookup(`SELECT start_time, file, status, row_count, error_count, first_error
FROM igloo.load_history WHERE pipe_name = ${api.literal(j.name)}
ORDER BY start_time DESC LIMIT 50`));
        clear(runs, res.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Time', 'File', 'Status', 'Rows'].map((t) => h('th', { text: t })))),
          h('tbody', {}, res.map((r) => [
            h('tr', {},
              h('td', { class: 'nowrap muted', text: fmtTime(r.start_time) }),
              h('td', {}, h('div', { class: 'truncate mono', text: r.file.split('/').pop() })),
              h('td', {}, outcomeBadge(r.status)),
              h('td', { class: 'num', text: num(r.row_count) })),
            r.first_error ? h('tr', {}, h('td', { colspan: '4' }, h('pre', { class: 'mono', text: r.first_error }))) : null,
          ]))))
          : h('p', { class: 'muted', text: 'No files loaded yet. Checks that find nothing new are not recorded.' }));
      } else if (kind === 'alert') {
        const res = api.objects(await api.lookup(`SELECT start_time, trigger, state, duration_ms, row_count, integration, error
FROM igloo.alert_history WHERE alert_type = 'alert' AND alert_name = ${api.literal(j.name)}
ORDER BY start_time DESC LIMIT 50`));
        clear(runs, res.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Started', 'Trigger', 'State', 'Took', 'Rows'].map((t) => h('th', { text: t })))),
          h('tbody', {}, res.map((r) => [
            h('tr', {},
              h('td', { class: 'nowrap muted', text: fmtTime(r.start_time) }),
              h('td', { text: r.trigger }),
              h('td', {}, outcomeBadge(r.state)),
              h('td', { class: 'num nowrap', text: r.duration_ms === null ? '' : fmtMs(Number(r.duration_ms)) }),
              h('td', { class: 'num', text: num(r.row_count) })),
            r.error ? h('tr', {}, h('td', { colspan: '5' }, h('pre', { class: 'mono', text: r.error }))) : null,
          ]))))
          : h('p', { class: 'muted', text: 'No runs yet.' }));
      } else {
        const res = api.objects(await api.lookup(`SELECT start_time, trigger, outcome, duration_ms, row_count, error
FROM igloo.task_history WHERE kind = ${api.literal(kind)} AND name = ${api.literal(j.name)}
ORDER BY start_time DESC LIMIT 50`));
        clear(runs, res.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Started', 'Trigger', 'Outcome', 'Took', 'Rows'].map((t) => h('th', { text: t })))),
          h('tbody', {}, res.map((r) => [
            h('tr', {},
              h('td', { class: 'nowrap muted', text: fmtTime(r.start_time) }),
              h('td', { text: r.trigger }),
              h('td', {}, outcomeBadge(r.outcome)),
              h('td', { class: 'num nowrap', text: r.duration_ms === null ? '' : fmtMs(Number(r.duration_ms)) }),
              h('td', { class: 'num', text: num(r.row_count) })),
            r.error ? h('tr', {}, h('td', { colspan: '5' }, h('pre', { class: 'mono', text: r.error }))) : null,
          ]))))
          : h('p', { class: 'muted', text: 'No runs yet.' }));
      }
    } catch (e) {
      clear(runs, MISSING.has(e.code) ? h('p', { class: 'muted', text: 'Run history is not available in this version of Igloo.' }) : errorView(e));
    }
  }

  let loaded = false;
  return {
    el,
    shown() {
      if (!loaded) { loaded = true; refresh(); }
    },
  };
}

// Load history

export function createLoads(app) {
  let rows = [];
  let selected = -1;

  const table = h('input', { placeholder: 'Any table', 'aria-label': 'Table', autocapitalize: 'none', spellcheck: 'false' });
  const status = h('select', { 'aria-label': 'Status' },
    h('option', { value: '', text: 'Any status' }), h('option', { value: 'loaded', text: 'Loaded' }),
    h('option', { value: 'partially loaded', text: 'Partially loaded' }), h('option', { value: 'failed', text: 'Failed' }),
    h('option', { value: 'unknown', text: 'Unknown' }), h('option', { value: 'loading', text: 'Loading' }));
  const range = h('select', { 'aria-label': 'Time range' },
    h('option', { value: '24', text: 'Last 24 hours' }), h('option', { value: '168', text: 'Last 7 days', selected: true }),
    h('option', { value: '720', text: 'Last 30 days' }), h('option', { value: '', text: 'Any time' }));
  const search = h('input', { type: 'search', placeholder: 'Text in the file name', 'aria-label': 'Search file names' });
  const filters = h('form', { class: 'filters', onsubmit: (e) => { e.preventDefault(); refresh(); } },
    h('div', { class: 'field' }, h('label', { text: 'Table' }), table),
    h('div', { class: 'field' }, h('label', { text: 'Status' }), status),
    h('div', { class: 'field' }, h('label', { text: 'Time' }), range),
    h('div', { class: 'field grow' }, h('label', { text: 'File' }), search),
    h('button', { class: 'btn primary', type: 'submit' }, icon('search'), 'Apply'));
  for (const s of [status, range]) s.addEventListener('change', () => refresh());

  const count = h('span', { class: 'sub' });
  const list = h('div', { class: 'history-list' });
  const detail = h('aside', { class: 'history-detail', 'aria-label': 'Details' });
  const layout = h('div', { class: 'history-layout' }, list, detail);
  const body = h('div', { class: 'page' });
  const el = h('section', { class: 'page history', 'aria-label': 'Load history' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Load history' }), count, h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => refresh() })),
    body);

  function where() {
    const parts = [];
    const t = table.value.trim();
    if (t) parts.push(`contains(lower(table_name), lower(${api.literal(t)}))`);
    if (status.value) parts.push(`status = ${api.literal(status.value)}`);
    if (range.value) parts.push(`start_time >= now() - INTERVAL ${Number(range.value)} HOUR`);
    const f = search.value.trim();
    if (f) parts.push(`contains(lower(file), lower(${api.literal(f)}))`);
    return parts.length ? `\nWHERE ${parts.join('\n  AND ')}` : '';
  }

  async function refresh() {
    clear(body, h('div', { class: 'result-message', text: 'Loading…' }));
    count.textContent = '';
    try {
      rows = api.objects(await api.lookup(`SELECT * FROM igloo.load_history${where()}\nORDER BY start_time DESC\nLIMIT ${LIMIT}`));
    } catch (e) {
      clear(body, MISSING.has(e.code) ? unavailable('Load histories', 'igloo.load_history') : [filters, errorView(e)]);
      return;
    }
    selected = -1;
    count.textContent = rows.length === LIMIT ? `latest ${LIMIT} files` : plural(rows.length, 'file');
    clear(body, filters, layout);
    renderList();
  }

  function renderList() {
    layout.classList.toggle('with-detail', selected >= 0);
    detail.hidden = selected < 0;
    if (!rows.length) {
      clear(list, h('div', { class: 'empty' }, h('h2', { text: 'Nothing loaded' }),
        h('p', {}, 'Files loaded with ', h('code', { text: 'COPY INTO' }), ' and by pipes appear here.')));
      return;
    }
    clear(list, h('table', { class: 'list' },
      h('thead', {}, h('tr', {}, ['Time', 'Table', 'File', 'Status', 'Rows', 'Errors', 'By'].map((t) => h('th', { class: t === 'Rows' || t === 'Errors' ? 'num' : '', text: t })))),
      h('tbody', {}, rows.map((r, i) => h('tr', {
        class: `clickable${i === selected ? ' selected' : ''}`, tabindex: '0',
        onclick: () => pick(i),
        onkeydown: (e) => { if (e.key === 'Enter') pick(i); },
      },
      h('td', { class: 'nowrap muted', text: fmtTime(r.start_time) }),
      h('td', { class: 'mono', text: r.table_name }),
      h('td', {}, h('div', { class: 'truncate mono', text: r.file })),
      h('td', {}, outcomeBadge(r.status)),
      h('td', { class: 'num', text: num(r.row_count) }),
      h('td', { class: 'num', text: Number(r.error_count) ? num(r.error_count) : '' }),
      h('td', { class: 'mono muted', text: r.pipe_name ? `${r.user_name} (pipe ${r.pipe_name})` : r.user_name }))))));
  }

  function pick(i) {
    selected = i;
    renderList();
    const r = rows[i];
    clear(detail,
      h('div', { class: 'row' }, h('h2', { text: r.file.split('/').pop() }), h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost icon-only small', type: 'button', 'aria-label': 'Close details', onclick: () => { selected = -1; renderList(); } }, icon('x'))),
      r.first_error ? h('pre', { class: 'mono', text: r.first_error }) : null,
      h('dl', {}, Object.entries(r).filter(([k]) => k !== 'first_error').map(([k, v]) => [
        h('dt', { text: k }),
        h('dd', { class: /_time$|^last_modified$/.test(k) ? '' : 'mono', text: /_time$|^last_modified$/.test(k) ? fmtTime(v) : v ?? 'NULL' }),
      ])),
      h('div', { class: 'row' }, button('Query the table', {
        icon: 'open', onclick: () => app.openWorksheet(`SELECT * FROM ${r.table_name.split('.').map(api.ident).join('.')} LIMIT 100`),
      })));
  }

  let loaded = false;
  return {
    el,
    shown() {
      if (!loaded) { loaded = true; refresh(); }
    },
  };
}
