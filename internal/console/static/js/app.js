// The console's entry point: sign-in, navigation and the theme.

import * as api from './api.js';
import { hydrate, icon, toast, h, clear, mark } from './dom.js';
import { createWorksheets } from './worksheets.js';
import { createData } from './data.js';
import { createHistory } from './history.js';
import { createUsers, createRoles, createCompute } from './admin.js';

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

function showSignIn(message, allowed = true) {
  $('loading').hidden = true;
  $('app').hidden = true;
  $('signin').hidden = false;
  const error = $('signin-error');
  error.textContent = message || '';
  error.hidden = !message;
  $('signin-submit').disabled = !allowed;
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
  history: { make: createHistory, title: 'Query history' },
  'admin/users': { make: createUsers, title: 'Users', admin: true },
  'admin/roles': { make: createRoles, title: 'Roles and grants', admin: true },
  'admin/compute': { make: createCompute, title: 'Compute', admin: true },
};

function start(session) {
  api.setSession(session.csrf, signedOut);
  const app = {
    user: session.user,
    role: session.role,
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
    p.shown?.();
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
  try {
    start(await api.get('/api/session'));
  } catch (e) {
    if (e.status === 401) {
      if (e.body.signin === false) {
        showSignIn('This connection is not encrypted, so sign-in works only at localhost. Configure TLS to sign in from other machines.', false);
      } else {
        showSignIn(note && note !== 'You signed out.' ? note : '');
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
