// Access tokens, service accounts and single sign-on. Tokens are created,
// listed and dropped with SQL (CREATE TOKEN, SHOW TOKENS, DROP TOKEN) as the
// signed-in user, like any statement; the server decides who may do what.
// A new token is shown once, here, and kept only in this page's memory.

import * as api from './api.js';
import { h, clear, icon, button, fmtTime, plural, roleBadge, toast, dialog, confirmDialog, mark } from './dom.js';

const NAME = /^[a-z_][a-z0-9_]{0,62}$/;
const TOKEN_NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$/;
const ROLES = ['none', 'reader', 'writer', 'admin'];
const LIFETIMES = [['7d', '7 days'], ['30d', '30 days'], ['90d', '90 days'], ['180d', '180 days'], ['365d', '1 year']];

function errorView(e) {
  return h('div', { class: 'sql-error', role: 'alert' },
    h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
    h('pre', { text: e.message }));
}

const yes = (v) => v === true || v === 't' || v === 'true';

function lifetimeSelect(attrs = {}) {
  return h('select', attrs, LIFETIMES.map(([v, label]) => h('option', { value: v, text: label, selected: v === '90d' })));
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast('Copied');
  } catch {
    toast('Select the token and copy it', 'error');
  }
}

// newTokenView shows a token just created: the only time anyone sees it.
function newTokenView(row) {
  const box = h('pre', { class: 'secret mono', text: row.token, tabindex: '0' });
  return h('div', { class: 'stack' },
    h('div', { class: 'notice info' }, icon('token'),
      h('span', { text: `Copy the token now. Igloo shows it only once and keeps just a hash. It signs in as ${row.user_name} until ${fmtTime(row.expires_at)}, with all of that user's privileges.` })),
    box,
    h('div', { class: 'row' }, button('Copy token', { icon: 'copy', kind: 'primary', onclick: () => copy(row.token) })),
    h('p', { class: 'muted' }, 'Use it as the password of user ', h('code', { text: row.user_name }),
      ' in any PostgreSQL client, such as dbt, a BI tool or psql:'),
    h('pre', { class: 'mono example', text: `PGPASSWORD='<token>' psql "host=${location.hostname} user=${row.user_name} dbname=igloo${location.protocol === 'https:' ? ' sslmode=require' : ''}"` }));
}

// createToken runs CREATE TOKEN and resolves to the new token's row.
async function createToken(name, lifetime, user) {
  let sql = 'CREATE TOKEN';
  if (name) sql += ` ${api.ident(name)}`;
  if (user) sql += ` FOR USER ${api.ident(user)}`;
  sql += ` EXPIRES ${api.literal(lifetime)}`;
  const res = await api.query(sql);
  return api.objects(res)[0];
}

function tokenTable(tokens, { showUser = false, onDrop }) {
  const now = Date.now();
  return h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
    h('thead', {}, h('tr', {},
      showUser ? h('th', { text: 'User' }) : null,
      ['Name', 'Starts with', 'Created', 'Expires', 'Last used', ''].map((t) => h('th', { text: t })))),
    h('tbody', {}, tokens.map((t) => {
      const expired = yes(t.expired) || new Date(t.expires_at).getTime() <= now;
      return h('tr', {},
        showUser ? h('td', {}, h('span', { class: 'mono', text: t.user_name })) : null,
        h('td', {}, h('span', { class: 'mono', text: t.name })),
        h('td', {}, h('code', { text: `${t.prefix}…` })),
        h('td', { class: 'nowrap muted' }, fmtTime(t.created_at), t.created_by && t.created_by !== t.user_name ? h('span', { class: 'faint', text: ` by ${t.created_by}` }) : null),
        h('td', { class: 'nowrap' }, expired ? h('span', { class: 'badge error', text: 'expired' }) : h('span', { class: 'muted', text: fmtTime(t.expires_at) })),
        h('td', { class: 'nowrap muted', text: t.last_used_at ? fmtTime(t.last_used_at) : 'never' }),
        h('td', { class: 'actions' }, button(expired ? 'Remove' : 'Revoke', { kind: 'small danger', onclick: () => onDrop(t) })));
    }))));
}

async function dropToken(t, forUser) {
  const ok = await confirmDialog('Revoke token', `Revoke the token ${t.name}${forUser ? ` of ${t.user_name}` : ''}? Programs using it can no longer sign in, and its open sessions end.`,
    { confirm: 'Revoke token', danger: true });
  if (!ok) return false;
  try {
    await api.query(`DROP TOKEN ${api.ident(t.name)}${forUser ? ` FOR USER ${api.ident(t.user_name)}` : ''}`);
    toast(`Revoked ${t.name}`);
    return true;
  } catch (e) {
    toast(e.message, 'error');
    return false;
  }
}

// Access tokens: the signed-in user's own.

export function createTokens(app) {
  const body = h('div', { class: 'page-body' });
  const el = h('section', { class: 'page', 'aria-label': 'Access tokens' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Access tokens' }),
      h('span', { class: 'sub', text: 'SQL clients such as dbt, BI tools and scripts send a token where they would send a password.' }),
      h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  const name = h('input', { placeholder: 'dbt', autocomplete: 'off', autocapitalize: 'none', spellcheck: 'false', 'aria-label': 'Token name' });
  const lifetime = lifetimeSelect({ 'aria-label': 'Expires after' });
  const createError = h('p', { class: 'form-error', role: 'alert', hidden: true });
  const fresh = h('div');
  const create = h('form', {
    class: 'card',
    onsubmit: async (e) => {
      e.preventDefault();
      createError.hidden = true;
      const n = name.value.trim();
      if (n && !TOKEN_NAME.test(n)) {
        createError.textContent = 'A token name is up to 63 letters, digits, dots, dashes and underscores.';
        createError.hidden = false;
        return;
      }
      try {
        const row = await createToken(n, lifetime.value);
        name.value = '';
        clear(fresh, h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', { text: `New token ${row.name}` })),
          h('div', { class: 'card-body' }, newTokenView(row))));
        load();
      } catch (err) {
        createError.textContent = err.message;
        createError.hidden = false;
      }
    },
  },
  h('div', { class: 'card-head' }, h('h2', { text: 'New token' })),
  h('div', { class: 'card-body stack' },
    app.method === 'sso' ? h('p', { class: 'muted', text: 'You signed in with single sign-on, so SQL clients sign in with a token instead of a password.' }) : null,
    h('div', { class: 'form-grid' },
      h('div', { class: 'field' }, h('label', { text: 'Name (optional)' }), name),
      h('div', { class: 'field' }, h('label', { text: 'Expires after' }), lifetime),
      h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Create token')),
    createError));

  async function load() {
    let tokens;
    try {
      tokens = api.objects(await api.lookup(`SHOW TOKENS FOR USER ${api.ident(app.user)}`));
    } catch (e) {
      clear(body, h('div', { class: 'stack' }, create, errorView(e)));
      return;
    }
    clear(body, h('div', { class: 'stack' }, create, fresh,
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', { text: plural(tokens.length, 'token') })),
        tokens.length ? tokenTable(tokens, { onDrop: async (t) => { if (await dropToken(t, false)) load(); } })
          : h('div', { class: 'card-body muted', text: 'You have no access tokens.' }))));
  }

  return { el, shown: load };
}

// Service accounts (admins): users for programs, with tokens only.

export function createServiceAccounts(app) {
  const body = h('div', { class: 'page-body' });
  const el = h('section', { class: 'page', 'aria-label': 'Service accounts' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Service accounts' }),
      h('span', { class: 'sub', text: 'Users for programs such as dbt, Airflow and BI extracts. They have no password and sign in with access tokens only.' }),
      h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  const name = h('input', { placeholder: 'dbt_prod', autocomplete: 'off', autocapitalize: 'none', spellcheck: 'false', 'aria-label': 'Name' });
  const role = h('select', { 'aria-label': 'Role' }, ROLES.map((r) => h('option', { value: r, text: r, selected: r === 'none' })));
  const owner = h('input', { value: app.user, autocomplete: 'off', autocapitalize: 'none', spellcheck: 'false', 'aria-label': 'Owner' });
  const createError = h('p', { class: 'form-error', role: 'alert', hidden: true });
  const create = h('form', {
    class: 'card',
    onsubmit: async (e) => {
      e.preventDefault();
      createError.hidden = true;
      const n = name.value.trim().toLowerCase();
      if (!NAME.test(n)) {
        createError.textContent = 'A name is lowercase letters, digits and underscores, starting with a letter or underscore.';
        createError.hidden = false;
        return;
      }
      try {
        await api.post('/api/admin/users', { action: 'service', name: n, role: role.value, owner: owner.value.trim().toLowerCase() });
        toast(`Created service account ${n}`);
        name.value = '';
        load();
      } catch (err) {
        createError.textContent = err.message;
        createError.hidden = false;
      }
    },
  },
  h('div', { class: 'card-head' }, h('h2', { text: 'New service account' })),
  h('div', { class: 'card-body stack' },
    h('p', { class: 'muted', text: 'Give each program its own account with the least it needs: role none and grants on its schemas, or reader or writer. The owner, an admin, answers for it.' }),
    h('div', { class: 'form-grid' },
      h('div', { class: 'field' }, h('label', { text: 'Name' }), name),
      h('div', { class: 'field' }, h('label', { text: 'Role' }), role),
      h('div', { class: 'field' }, h('label', { text: 'Owner' }), owner),
      h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Create')),
    createError));

  async function change(req, success) {
    try {
      await api.post('/api/admin/users', req);
      toast(success);
    } catch (err) {
      toast(err.message, 'error');
    }
    load();
  }

  async function newToken(account) {
    const tname = h('input', { placeholder: 'ci', autocomplete: 'off', autocapitalize: 'none', spellcheck: 'false' });
    const lifetime = lifetimeSelect();
    const error = h('p', { class: 'form-error', role: 'alert', hidden: true });
    const ok = await dialog({
      title: `New token for ${account.name}`,
      body: [h('div', { class: 'field' }, h('label', { text: 'Name (optional)' }), tname),
        h('div', { class: 'field' }, h('label', { text: 'Expires after' }), lifetime), error],
      actions: [{ label: 'Cancel', value: false }, {
        label: 'Create token', value: true, primary: true, kind: 'primary',
        validate: () => {
          const bad = tname.value.trim() && !TOKEN_NAME.test(tname.value.trim());
          error.textContent = bad ? 'Up to 63 letters, digits, dots, dashes and underscores.' : '';
          error.hidden = !bad;
          return !bad;
        },
      }],
      onopen: () => tname.focus(),
    });
    if (!ok) return;
    let row;
    try {
      row = await createToken(tname.value.trim(), lifetime.value, account.name);
    } catch (err) {
      toast(err.message, 'error');
      return;
    }
    load();
    await dialog({ title: `Token ${row.name} for ${account.name}`, wide: true, body: [newTokenView(row)],
      actions: [{ label: 'Done', value: true, primary: true, kind: 'primary' }] });
  }

  async function load() {
    let users;
    let tokens;
    try {
      const res = await api.get('/api/admin/users');
      users = res.rows.map((r) => Object.fromEntries(res.columns.map((c, i) => [c, r[i]])));
      tokens = api.objects(await api.lookup('SHOW TOKENS'));
    } catch (e) {
      clear(body, h('div', { class: 'stack' }, create, errorView(e)));
      return;
    }
    const accounts = users.filter((u) => u.type === 'service').sort((a, b) => a.name.localeCompare(b.name));
    clear(body, h('div', { class: 'stack' }, create,
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', { text: plural(accounts.length, 'service account') })),
        accounts.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Name', 'Role', 'Owner', 'Tokens', 'Status', ''].map((t) => h('th', { text: t })))),
          h('tbody', {}, accounts.map((a) => {
            const enabled = yes(a.can_login);
            const live = tokens.filter((t) => t.user_name === a.name && !yes(t.expired)).length;
            return h('tr', {},
              h('td', {}, h('span', { class: 'mono', text: a.name })),
              h('td', {}, roleBadge(a.role)),
              h('td', { class: 'mono', text: a.owner || '' }),
              h('td', { class: 'muted', text: plural(live, 'active token') }),
              h('td', {}, enabled ? h('span', { class: 'badge ok', text: 'enabled' }) : h('span', { class: 'badge error', text: 'disabled' })),
              h('td', { class: 'actions' },
                button('New token', { kind: 'small', icon: 'token', onclick: () => newToken(a) }),
                button(enabled ? 'Disable' : 'Enable', { kind: 'small', onclick: () => change({ action: enabled ? 'disable' : 'enable', name: a.name }, `${a.name} ${enabled ? 'disabled' : 'enabled'}`) }),
                button('Drop', {
                  kind: 'small danger',
                  onclick: async () => {
                    if (await confirmDialog('Drop service account', `Drop ${a.name} and its tokens? Programs using them can no longer sign in.`, { confirm: 'Drop', danger: true })) {
                      change({ action: 'drop', name: a.name }, `Dropped ${a.name}`);
                    }
                  },
                })));
          })))) : h('div', { class: 'card-body muted', text: 'No service accounts yet.' })),
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', { text: `All tokens · ${plural(tokens.length, 'token')}` }),
          h('span', { class: 'muted', text: 'Every user\'s tokens. Revoke any that leaked or are no longer used.' })),
        tokens.length ? tokenTable(tokens, { showUser: true, onDrop: async (t) => { if (await dropToken(t, true)) load(); } })
          : h('div', { class: 'card-body muted', text: 'No one has access tokens.' }))));
  }

  return { el, shown: load };
}

// Single sign-on (admins): settings, group mappings and linked users.

export function createSSO() {
  const body = h('div', { class: 'page-body' });
  const el = h('section', { class: 'page', 'aria-label': 'Single sign-on' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Single sign-on' }), h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => load() })),
    body);

  const list = (v) => (v && v.length ? v.join(', ') : 'any');

  function settings(res) {
    const st = res.status;
    const reach = st.reachable ? h('span', { class: 'badge ok', text: 'reachable' }) : h('span', { class: 'badge error', text: 'unreachable' });
    return h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', { text: 'Settings' }), h('span', { class: 'muted', text: 'From IGLOO_OIDC_* settings; see docs/sso.md.' })),
      h('div', { class: 'card-body stack' },
        st.error ? h('div', { class: 'sql-error', role: 'alert' }, h('pre', { text: st.error })) : null,
        h('dl', { class: 'kv' },
          h('dt', { text: 'Identity provider' }), h('dd', {}, h('code', { text: st.issuer }), ' ', reach),
          h('dt', { text: 'Client ID' }), h('dd', {}, h('code', { text: st.client_id })),
          h('dt', { text: 'Client secret' }), h('dd', { text: st.client_secret ? 'set' : 'none (public client with PKCE)' }),
          h('dt', { text: 'Redirect URI' }), h('dd', {}, h('code', { text: res.callback }),
            st.redirect_url ? null : h('span', { class: 'faint', text: ' (from this request; register it at the provider)' })),
          h('dt', { text: 'Scopes' }), h('dd', { class: 'mono', text: (st.scopes || []).join(' ') }),
          h('dt', { text: 'User name claim' }), h('dd', { class: 'mono', text: st.username_claim }),
          h('dt', { text: 'Groups claim' }), h('dd', { class: 'mono', text: st.groups_claim }),
          h('dt', { text: 'Allowed domains' }), h('dd', { text: list(st.allowed_domains) }),
          h('dt', { text: 'Allowed groups' }), h('dd', { text: list(st.allowed_groups) }),
          h('dt', { text: 'New users' }), h('dd', { text: st.create_users ? 'created at their first sign-in, with role none' : 'admins create them; others are refused' }),
          h('dt', { text: 'Passwords of SSO users' }), h('dd', { text: st.password_sign_in ? 'allowed' : 'off: they use single sign-on and access tokens' }),
          h('dt', { text: 'Provider access tokens in SQL' }), h('dd', { text: st.access_token_audience ? `accepted over TLS, audience ${st.access_token_audience}` : 'off' }))));
  }

  function mappings(res, roles) {
    const m = res.mappings || {};
    const rows = (m.rows || []).map((r) => Object.fromEntries(m.columns.map((c, i) => [c, r[i]])));
    const group = h('input', { placeholder: 'Data Team', autocomplete: 'off', spellcheck: 'false', 'aria-label': 'Group' });
    const role = h('select', { 'aria-label': 'Role' },
      roles.map((r) => h('option', { value: r, text: r })),
      ROLES.map((r) => h('option', { value: r, text: `${r} (built-in)` })));
    const error = h('p', { class: 'form-error', role: 'alert', hidden: true });
    const run = async (sql, done) => {
      try {
        await api.post('/api/admin/grants', { sql });
        toast(done);
        load();
      } catch (e) {
        error.textContent = e.message;
        error.hidden = false;
      }
    };
    const builtin = rows.some((r) => r.role_type === 'built-in');
    return h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', { text: 'Group mappings' }),
        h('span', { class: 'muted', text: 'Applied at each sign-in: members of a group get its roles.' })),
      h('div', { class: 'card-body stack' },
        builtin ? h('div', { class: 'notice' }, h('span', { text: 'Groups map to built-in roles, so each single sign-on sets the user\'s built-in role: the highest their groups give, or none.' })) : null,
        m.error ? errorView({ message: m.error, code: m.code }) : null,
        rows.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
          h('thead', {}, h('tr', {}, ['Group', 'Role', 'Granted by', ''].map((t) => h('th', { text: t })))),
          h('tbody', {}, rows.map((r) => h('tr', {},
            h('td', {}, h('code', { text: r.group_name })),
            h('td', {}, r.role_type === 'built-in' ? roleBadge(r.role) : h('span', { class: 'mono', text: r.role })),
            h('td', { class: 'muted', text: `${r.granted_by} · ${fmtTime(r.granted_at)}` }),
            h('td', { class: 'actions' }, button('Remove', {
              kind: 'small danger',
              onclick: () => run(`REVOKE ${api.ident(r.role)} FROM SSO GROUP ${api.literal(r.group_name)}`, 'Mapping removed'),
            }))))))) : h('p', { class: 'muted', text: 'No groups are mapped yet.' }),
        h('form', {
          class: 'form-grid',
          onsubmit: (e) => {
            e.preventDefault();
            error.hidden = true;
            if (!group.value.trim()) return;
            run(`GRANT ${api.ident(role.value)} TO SSO GROUP ${api.literal(group.value.trim())}`, 'Mapping added');
          },
        },
        h('div', { class: 'field' }, h('label', { text: 'Identity provider group' }), group),
        h('div', { class: 'field' }, h('label', { text: 'Role' }), role),
        h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Map group')),
        error));
  }

  function users(res) {
    const linked = res.users || [];
    return h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', { text: plural(linked.length, 'user') + ' signed in with SSO' })),
      linked.length ? h('div', { class: 'table-wrap' }, h('table', { class: 'list' },
        h('thead', {}, h('tr', {}, ['User', 'Email', 'Groups at the last sign-in', 'Last sign-in'].map((t) => h('th', { text: t })))),
        h('tbody', {}, linked.map((u) => h('tr', {},
          h('td', {}, h('span', { class: 'mono', text: u.user })),
          h('td', { class: 'muted', text: u.email || '' }),
          h('td', { class: 'truncate', text: (u.groups || []).join(', ') }),
          h('td', { class: 'nowrap muted', text: fmtTime(u.last_sign_in) }))))))
        : h('div', { class: 'card-body muted', text: 'No one has signed in with single sign-on yet.' }));
  }

  async function load() {
    let res;
    let roles = [];
    try {
      res = await api.get('/api/admin/sso');
      const r = await api.get('/api/admin/grants');
      if (r.roles && r.roles.rows) roles = r.roles.rows.map((row) => row[0]);
    } catch (e) {
      clear(body, errorView(e));
      return;
    }
    if (!res.enabled) {
      clear(body, h('div', { class: 'empty' },
        h('span', { class: 'mark' }, mark()),
        h('h2', { text: 'Single sign-on is off' }),
        h('p', {}, 'Set ', h('code', { text: 'IGLOO_OIDC_ISSUER' }), ', ', h('code', { text: 'IGLOO_OIDC_CLIENT_ID' }), ' and ',
          h('code', { text: 'IGLOO_OIDC_CLIENT_SECRET_FILE' }), ' to let people sign in with Okta, Microsoft Entra ID, Google Workspace or another OpenID Connect provider. See docs/sso.md.')));
      return;
    }
    clear(body, h('div', { class: 'stack' }, settings(res), mappings(res, roles), users(res)));
  }

  return { el, shown: load };
}
