// Administration: users, roles and grants, and this replica's compute. The
// server refuses these pages' requests to anyone but an admin, and runs the
// same SQL statements an admin would type in psql.

import * as api from './api.js';
import { h, clear, icon, button, fmtTime, fmtAgo, fmtMs, plural, roleBadge, toast, dialog, confirmDialog, mark } from './dom.js';

const ROLES = ['none', 'reader', 'writer', 'admin'];
const NAME = /^[a-z_][a-z0-9_]{0,62}$/;

function errorView(e) {
  return h('div', { class: 'sql-error', role: 'alert' },
    h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
    h('pre', { text: e.message }));
}

function roleSelect(value, attrs = {}) {
  return h('select', attrs, ROLES.map((r) => h('option', { value: r, text: r, selected: r === value })));
}

function passwordFields() {
  const password = h('input', { type: 'password', autocomplete: 'new-password', minlength: '8', required: true });
  const again = h('input', { type: 'password', autocomplete: 'new-password', required: true });
  const error = h('p', { class: 'form-error', role: 'alert', hidden: true });
  const validate = () => {
    let msg = '';
    if (password.value.length < 8) msg = 'Use at least 8 characters.';
    else if (password.value !== again.value) msg = 'The passwords do not match.';
    error.textContent = msg;
    error.hidden = !msg;
    return !msg;
  };
  return {
    password, again, validate,
    fields: [
      h('div', { class: 'field' }, h('label', { text: 'Password' }), password),
      h('div', { class: 'field' }, h('label', { text: 'Repeat the password' }), again),
      error,
    ],
  };
}

// Users

export function createUsers(app) {
  const body = h('div', { class: 'page-body' });
  const el = h('section', { class: 'page', 'aria-label': 'Users' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Users' }),
      h('span', { class: 'sub', text: 'Users sign in to SQL clients and this console with the same name and password.' }),
      h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  const name = h('input', { placeholder: 'ana', autocomplete: 'off', autocapitalize: 'none', spellcheck: 'false', required: true, 'aria-label': 'User name' });
  const role = roleSelect('reader', { 'aria-label': 'Role' });
  const password = h('input', { type: 'password', autocomplete: 'new-password', placeholder: 'At least 8 characters', required: true, 'aria-label': 'Password' });
  const createError = h('p', { class: 'form-error', role: 'alert', hidden: true });
  const create = h('form', {
    class: 'card',
    onsubmit: async (e) => {
      e.preventDefault();
      createError.hidden = true;
      const n = name.value.trim().toLowerCase();
      let msg = '';
      if (!NAME.test(n)) msg = 'A user name is lowercase letters, digits and underscores, starting with a letter or underscore.';
      else if (password.value.length < 8) msg = 'Use a password of at least 8 characters.';
      if (msg) { createError.textContent = msg; createError.hidden = false; return; }
      try {
        await api.post('/api/admin/users', { action: 'create', name: n, password: password.value, role: role.value });
        toast(`Created user ${n}`);
        name.value = '';
        password.value = '';
        load();
      } catch (err) {
        createError.textContent = err.message;
        createError.hidden = false;
      }
    },
  },
  h('div', { class: 'card-head' }, h('h2', { text: 'New user' })),
  h('div', { class: 'card-body' },
    h('div', { class: 'form-grid' },
      h('div', { class: 'field' }, h('label', { text: 'Name' }), name),
      h('div', { class: 'field' }, h('label', { text: 'Password' }), password),
      h('div', { class: 'field' }, h('label', { text: 'Role' }), role),
      h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Create user')),
    createError));

  async function change(body, success) {
    try {
      await api.post('/api/admin/users', body);
      toast(success);
    } catch (err) {
      toast(err.message, 'error');
    }
    load();
  }

  async function load() {
    let res;
    try {
      res = await api.get('/api/admin/users');
    } catch (e) {
      clear(body, errorView(e));
      return;
    }
    const users = res.rows.map((r) => Object.fromEntries(res.columns.map((c, i) => [c, r[i]])));
    users.sort((a, b) => a.name.localeCompare(b.name));
    clear(body, h('div', { class: 'stack' }, create,
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', { text: plural(users.length, 'user') })),
        h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, h('th', { text: 'Name' }), h('th', { text: 'Role' }), h('th', { text: 'Created' }), h('th', { class: 'num', text: '' }))),
          h('tbody', {}, users.map((u) => {
            const select = roleSelect(u.role, { 'aria-label': `Role of ${u.name}` });
            select.addEventListener('change', async () => {
              if (u.name === app.user && select.value !== 'admin'
                && !(await confirmDialog('Change your own role', `You will lose the admin role and these pages. Make yourself a ${select.value}?`, { confirm: 'Change my role', danger: true }))) {
                select.value = u.role;
                return;
              }
              change({ action: 'role', name: u.name, role: select.value }, `${u.name} is now a ${select.value}`);
            });
            return h('tr', {},
              h('td', {}, h('span', { class: 'mono', text: u.name }), u.name === app.user ? h('span', { class: 'faint', text: ' (you)' }) : null),
              h('td', {}, select),
              h('td', { class: 'nowrap muted', text: fmtTime(u.created_at) }),
              h('td', { class: 'actions' },
                button('Reset password', { kind: 'small', onclick: () => resetPassword(u) }),
                button('Drop', { kind: 'small danger', onclick: () => drop(u) })));
          })))))));
  }

  async function resetPassword(u) {
    const p = passwordFields();
    const ok = await dialog({
      title: `New password for ${u.name}`,
      body: [h('p', { class: 'muted', text: 'Open sessions keep working; the new password is needed at the next sign-in. Console sessions of this user end.' }), p.fields],
      actions: [{ label: 'Cancel', value: false }, { label: 'Set password', value: true, primary: true, kind: 'primary', validate: p.validate }],
      onopen: () => p.password.focus(),
    });
    if (ok) change({ action: 'password', name: u.name, password: p.password.value }, `Changed the password of ${u.name}`);
  }

  async function drop(u) {
    const ok = await confirmDialog('Drop user', `Drop user ${u.name}? Their open sessions end within 30 seconds. Their tables stay.`, { confirm: 'Drop user', danger: true });
    if (ok) change({ action: 'drop', name: u.name }, `Dropped user ${u.name}`);
  }

  return { el, shown: load };
}

// Roles and grants

const GRANT_EXAMPLES = [
  'CREATE ROLE analyst',
  'GRANT SELECT ON TABLE lake.public.events TO ROLE analyst',
  'GRANT ROLE analyst TO USER ana',
  'REVOKE SELECT ON TABLE lake.public.events FROM ROLE analyst',
];

export function createRoles() {
  const body = h('div', { class: 'page-body' });
  const el = h('section', { class: 'page', 'aria-label': 'Roles and grants' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Roles and grants' }), h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  function resultTable(title, r) {
    if (!r || r.error) {
      return h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', { text: title })),
        h('div', { class: 'card-body' }, errorView({ message: r ? r.error : 'Not available', code: r && r.code })));
    }
    return h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', { text: title }), h('span', { class: 'muted', text: plural(r.rows.length, 'row') })),
      r.rows.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
        h('thead', {}, h('tr', {}, r.columns.map((c) => h('th', { text: c })))),
        h('tbody', {}, r.rows.map((row) => h('tr', {}, row.map((v) => h('td', { class: 'mono', text: v ?? 'NULL' })))))))
        : h('div', { class: 'card-body muted', text: 'None yet.' }));
  }

  async function load() {
    let res;
    try {
      res = await api.get('/api/admin/grants');
    } catch (e) {
      clear(body, errorView(e));
      return;
    }
    if (!res.available) {
      clear(body, h('div', { class: 'empty' },
        h('span', { class: 'mark' }, mark()),
        h('h2', { text: 'Custom roles and grants are not available in this version of Igloo' }),
        h('p', { text: 'Every user has one of three built-in roles: admin, writer or reader. Change them on the Users page. This page works as soon as the server supports SHOW ROLES and SHOW GRANTS.' })));
      return;
    }
    const box = h('textarea', { class: 'statement-box', spellcheck: 'false', 'aria-label': 'Statement', placeholder: GRANT_EXAMPLES[1] });
    const result = h('div');
    const form = h('form', {
      class: 'card',
      onsubmit: async (e) => {
        e.preventDefault();
        clear(result);
        if (!box.value.trim()) return;
        try {
          const r = await api.post('/api/admin/grants', { sql: box.value });
          toast(r.tag || 'Done');
          box.value = '';
          load();
        } catch (err) {
          clear(result, errorView(err));
        }
      },
    },
    h('div', { class: 'card-head' }, h('h2', { text: 'Change roles and grants' })),
    h('div', { class: 'card-body stack' },
      h('div', {}, box,
        h('ul', { class: 'examples' }, GRANT_EXAMPLES.map((x) => h('li', {}, h('code', { text: x }))))),
      h('div', { class: 'row' }, h('button', { class: 'btn primary', type: 'submit', text: 'Run' }),
        h('span', { class: 'muted', text: 'Runs GRANT, REVOKE, CREATE ROLE, ALTER ROLE or DROP ROLE as you.' })),
      result));
    clear(body, h('div', { class: 'stack' }, resultTable('Roles', res.roles), resultTable('Grants', res.grants), form));
  }

  return { el, shown: load };
}

// Compute

export function createCompute(app) {
  const body = h('div', { class: 'page-body' });
  const updated = h('span', { class: 'sub' });
  const el = h('section', { class: 'page', 'aria-label': 'Compute' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Compute' }), updated, h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  function stat(label, value, note) {
    return h('div', { class: 'card stat' }, h('div', { class: 'label', text: label }), h('div', { class: 'value', text: value }),
      note ? h('div', { class: 'note', text: note }) : null);
  }

  async function cancel(s, replica) {
    if (!(await confirmDialog('Cancel statement', `Cancel the statement ${s.user} is running? They get SQLSTATE 57014.`, { confirm: 'Cancel statement', danger: true }))) return;
    try {
      const r = await api.post('/api/admin/cancel', { pid: s.pid, replica });
      toast(r.canceled ? 'Canceled' : 'The statement had already finished');
    } catch (e) {
      toast(e.message, 'error');
    }
    load();
  }

  async function load() {
    let res;
    try {
      res = await api.get('/api/admin/compute');
    } catch (e) {
      clear(body, errorView(e));
      return;
    }
    const now = new Date(res.now).getTime();
    const st = res.status || {};
    const running = res.sessions.filter((s) => s.started);
    updated.textContent = `Replica ${res.replica} · updated ${fmtTime(res.now)}`;
    const m = st.maintenance;
    const gateway = st.gateway_seen
      ? `A scale-to-zero gateway last checked this replica ${fmtAgo(st.gateway_seen, now)}.`
      : 'No scale-to-zero gateway has checked this replica since it started. Gateways check after a quiet period (10 minutes by default), so a recent start may not show one yet.';
    clear(body, h('div', { class: 'stack' },
      h('div', { class: 'grid-cards' },
        stat('Version', st.version || 'unknown'),
        stat('Up for', st.started ? fmtAgo(st.started, now).replace(' ago', '') : 'unknown', st.started ? `since ${fmtTime(st.started)}` : null),
        stat('Sessions', String(res.sessions.length), 'SQL clients and console requests'),
        stat('Running', String(st.running ?? running.length), st.running ? 'statements and maintenance passes' : `idle for ${fmtMs((st.idle_seconds || 0) * 1000)}`)),
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', { text: 'Sessions on this replica' }),
          h('span', { class: 'muted', text: 'Behind a load balancer, each refresh may reach a different replica.' })),
        res.sessions.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Session', 'User', 'Client', 'Connected', 'State', 'Statement', ''].map((t) => h('th', { text: t })))),
          h('tbody', {}, res.sessions.map((s) => h('tr', {},
            h('td', { class: 'mono', text: String(s.pid) }),
            h('td', {}, h('span', { class: 'mono', text: s.user }), ' ', roleBadge(s.role)),
            h('td', { class: 'mono muted', text: s.client || '' }),
            h('td', { class: 'nowrap muted', text: fmtAgo(s.connected, now) }),
            h('td', { class: 'nowrap' }, s.started
              ? h('span', { class: 'badge warn', text: `running ${fmtMs(now - new Date(s.started).getTime())}` })
              : h('span', { class: 'badge', text: 'idle' })),
            h('td', {}, s.statement ? h('code', { class: 'statement', text: s.statement }) : null),
            h('td', { class: 'actions' }, s.started ? button('Cancel', { kind: 'small danger', onclick: () => cancel(s, res.replica) }) : null))))))
          : h('div', { class: 'card-body muted', text: 'No open sessions.' })),
      h('div', { class: 'grid-cards wide' },
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', { text: 'Table maintenance' })),
          h('div', { class: 'card-body' }, m
            ? h('dl', { class: 'kv' },
              h('dt', { text: 'Last pass' }), h('dd', { text: m.last_pass ? `${fmtTime(m.last_pass)} (${fmtAgo(m.last_pass, now)})` : 'not recorded yet' }),
              h('dt', { text: 'Interval' }), h('dd', { text: m.interval }),
              h('dt', { text: 'Snapshot retention' }), h('dd', { text: m.retention }))
            : h('p', { class: 'muted', text: 'This replica does not maintain tables: there is no catalog, or IGLOO_MAINTENANCE is false.' }))),
        h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', { text: 'Scale to zero' })),
          h('div', { class: 'card-body' }, h('p', { text: gateway }),
            h('p', { class: 'muted', text: 'The console sends nothing while you are not using it, so an open console never keeps Igloo awake.' }))))));
  }

  return { el, shown: load };
}
