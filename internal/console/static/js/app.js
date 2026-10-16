// The console's entry point: sign-in, navigation and the theme.

import * as api from './api.js';
import { hydrate, icon, toast, h, clear, mark } from './dom.js';
import { createWorksheets } from './worksheets.js';
import { createData } from './data.js';
import { createCatalog } from './catalog.js';
import { createHistory } from './history.js';
import { createJobs, createLoads } from './tasks.js';
import { createUsers, createRoles, createCompute } from './admin.js';
import { createTokens, createServiceAccounts, createSSO } from './access.js';

const $ = (id) => document.getElementById(id);

// Theme: system, light or dark, remembered in this browser.

const THEMES = ['system', 'light', 'dark'];
const THEME_ICONS = { system: 'monitor', light: 'sun', dark: 'moon' };

function theme() {
  try { return localStorage.getItem('igloo.theme') || 'system'; } catch { return 'system'; }
}

function applyTheme(t) {
  if (t === 'system') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = t;
  const b = $('theme');
  clear(b, icon(THEME_ICONS[t]), `${t[0].toUpperCase()}${t.slice(1)} theme`);
}

function cycleTheme() {
  const next = THEMES[(THEMES.indexOf(theme()) + 1) % THEMES.length];
  try { localStorage.setItem('igloo.theme', next); } catch { /* ignore */ }
  applyTheme(next);
}

// Sign-in

// Why a single sign-on did not sign the user in, by the code the server
// puts in ?sso_error= (see internal/console/sso.go).
const SSO_ERRORS = {
  expired: 'The sign-in took too long, or was started in another tab. Try again.',
  denied: 'The identity provider did not sign you in.',
  failed: 'Igloo could not verify the identity provider\'s answer. Try again, or ask an admin to check Igloo\'s log.',
  unavailable: 'Igloo could not reach the identity provider. Try again later.',
  insecure: 'This connection is not encrypted, so single sign-on works only at localhost. Configure TLS to sign in from other machines.',
  unverified: 'The identity provider did not confirm who you are (no verified email or user name).',
  domain: 'Accounts of your email domain may not sign in to Igloo.',
  group: 'Your account is in none of the groups that may sign in to Igloo.',
  no_user: 'There is no Igloo user for your account. Ask an admin to create one.',
  conflict: 'Your account matches an Igloo user that belongs to someone else. Ask an admin.',
  service: 'That account is a service account, which cannot sign in to the console.',
  disabled: 'Your Igloo user is disabled. Ask an admin.',
};

// ssoError takes the code a failed single sign-on left in the address,
// and removes it.
function ssoError() {
  const params = new URLSearchParams(location.search);
  const code = params.get('sso_error');
  if (!code) return '';
  history.replaceState(null, '', location.pathname + location.hash);
  return SSO_ERRORS[code] || 'Single sign-on failed. Try again.';
}

function showSignIn(message, allowed = true, sso = null) {
  $('loading').hidden = true;
  $('app').hidden = true;
  $('signin').hidden = false;
  const error = $('signin-error');
  error.textContent = message || '';
  error.hidden = !message;
  $('signin-submit').disabled = !allowed;
  $('signin-sso').hidden = !sso;
  if (sso) $('signin-sso-label').textContent = `Sign in with ${sso.name}`;
  $('signin-user').focus();
}

async function signIn(e) {
  e.preventDefault();
  const user = $('signin-user').value.trim();
  const password = $('signin-password').value;
  if (!user || !password) {
    showSignIn('Enter your user name and password.');
    return;
  }
  const submit = $('signin-submit');
  submit.disabled = true;
  submit.textContent = 'Signing in…';
  try {
    const session = await api.post('/api/login', { user, password });
    $('signin-password').value = '';
    start(session);
  } catch (err) {
    showSignIn(err.message);
    $('signin-password').select();
  } finally {
    submit.textContent = 'Sign in';
    if (!$('signin').hidden) submit.disabled = false;
  }
}

function signedOut(body) {
  // Every page is gone with the session; start over from a clean page.
  if (!$('app').hidden) {
    sessionStorage.setItem('igloo.signedOut', (body && body.error) || 'Your session ended. Sign in again.');
    location.reload();
  }
}

// Navigation

const ROUTES = {
  worksheets: { make: createWorksheets, title: 'Worksheets' },
  data: { make: createData, title: 'Data' },
  catalog: { make: createCatalog, title: 'Catalog' },
  history: { make: createHistory, title: 'Query history' },
  tasks: { make: createJobs, title: 'Tasks' },
  loads: { make: createLoads, title: 'Load history' },
  tokens: { make: createTokens, title: 'Access tokens' },
  'admin/users': { make: createUsers, title: 'Users', admin: true },
  'admin/services': { make: createServiceAccounts, title: 'Service accounts', admin: true },
  'admin/roles': { make: createRoles, title: 'Roles and grants', admin: true },
  'admin/sso': { make: createSSO, title: 'Single sign-on', admin: true },
  'admin/compute': { make: createCompute, title: 'Compute', admin: true },
};

function start(session) {
  api.setSession(session.csrf, signedOut);
  const app = {
    user: session.user,
    role: session.role,
    method: session.method,
    isAdmin: session.role === 'admin',
    openWorksheet(sql, name) {
      location.hash = '#/worksheets';
      page('worksheets').open(sql, name);
    },
  };
  const pages = new Map();
  function page(name) {
    if (!pages.has(name)) {
      const p = ROUTES[name].make(app);
      p.el.hidden = true;
      $('main').append(p.el);
      pages.set(name, p);
    }
    return pages.get(name);
  }

  function route() {
    let name = location.hash.replace(/^#\/?/, '') || 'worksheets';
    // A table's catalog page is catalog/<catalog>/<schema>/<name>.
    let param = '';
    if (!ROUTES[name] && name.startsWith('catalog/')) [name, param] = ['catalog', name.slice('catalog/'.length)];
    if (!ROUTES[name] || (ROUTES[name].admin && !app.isAdmin)) name = 'worksheets';
    for (const [key, p] of pages) p.el.hidden = key !== name;
    const p = page(name);
    p.el.hidden = false;
    document.title = `${ROUTES[name].title} · Igloo`;
    for (const a of document.querySelectorAll('.nav a')) {
      if (a.dataset.route === name) a.setAttribute('aria-current', 'page');
      else a.removeAttribute('aria-current');
    }
    closeMenu();
    p.shown?.(param);
  }

  $('whoami-name').textContent = session.user;
  const role = $('whoami-role');
  role.textContent = session.role;
  role.className = `badge ${session.role}`;
  $('admin-nav').hidden = !app.isAdmin;
  $('loading').hidden = true;
  $('signin').hidden = true;
  $('app').hidden = false;
  window.addEventListener('hashchange', route);
  route();
}

function closeMenu() {
  $('sidebar').classList.remove('open');
  $('scrim').hidden = true;
  $('menu').setAttribute('aria-expanded', 'false');
}

async function main() {
  hydrate();
  applyTheme(theme());
  $('theme').addEventListener('click', cycleTheme);
  $('signin-form').addEventListener('submit', signIn);
  $('signout').addEventListener('click', async () => {
    try { await api.post('/api/logout', {}); } catch { /* signed out anyway */ }
    sessionStorage.setItem('igloo.signedOut', 'You signed out.');
    location.hash = '';
    location.reload();
  });
  $('menu').addEventListener('click', () => {
    const open = !$('sidebar').classList.contains('open');
    $('sidebar').classList.toggle('open', open);
    $('scrim').hidden = !open;
    $('menu').setAttribute('aria-expanded', String(open));
  });
  $('scrim').addEventListener('click', closeMenu);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeMenu(); });

  const note = sessionStorage.getItem('igloo.signedOut');
  sessionStorage.removeItem('igloo.signedOut');
  const failedSSO = ssoError();
  try {
    start(await api.get('/api/session'));
  } catch (e) {
    if (e.status === 401) {
      const sso = e.body.sso || null;
      if (e.body.signin === false) {
        showSignIn('This connection is not encrypted, so sign-in works only at localhost. Configure TLS to sign in from other machines.', false);
      } else {
        showSignIn(failedSSO || (note && note !== 'You signed out.' ? note : ''), true, sso);
        if (note === 'You signed out.') toast(note);
      }
      return;
    }
    $('loading').hidden = true;
    clear(document.body, h('div', { class: 'empty' }, h('span', { class: 'mark' }, mark()),
      h('h2', { text: 'The console could not reach Igloo' }), h('p', { text: e.message })));
  }
}

main();
