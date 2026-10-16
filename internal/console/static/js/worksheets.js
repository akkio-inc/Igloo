// Worksheets: SQL scripts kept in this browser's local storage, run as the
// signed-in user. Results stay in memory only.

import * as api from './api.js';
import { h, clear, icon, button, fmtMs, plural, toast, confirmDialog } from './dom.js';
import { createEditor } from './editor.js';
import { createGrid } from './grid.js';

const STARTER = `-- Run with Ctrl+Enter (Cmd+Enter on a Mac), or select text to run just that.
SELECT current_user, version();
`;

export function createWorksheets(app) {
  const storeKey = `igloo.worksheets.${app.user}`;
  const heightKey = 'igloo.editorHeight';
  let state = load();
  const results = new Map(); // sheet id -> run
  const running = new Map(); // sheet id -> AbortController

  const tabs = h('div', { class: 'sheet-tabs', role: 'tablist', 'aria-label': 'Worksheets' });
  const runBtn = button('Run', { icon: 'play', kind: 'primary', onclick: () => run() });
  const cancelBtn = button('Cancel', { icon: 'stop', kind: 'danger', onclick: () => cancel() });
  const downloadBtn = button('Download CSV', { icon: 'download', onclick: () => downloadCSV() });
  const status = h('span', { class: 'status', 'aria-live': 'polite' });
  const editor = createEditor({
    onChange: (text) => { current().sql = text; save(); },
    onRun: () => run(),
    onEscape: () => runBtn.focus(),
    label: 'SQL worksheet',
  });
  const editorWrap = h('div', { class: 'editor-wrap' }, editor.el);
  const splitter = h('div', {
    class: 'splitter', role: 'separator', tabindex: '0',
    'aria-orientation': 'horizontal', 'aria-label': 'Resize the editor',
  });
  const resultTabs = h('div', { class: 'result-tabs', role: 'tablist', 'aria-label': 'Statements' });
  const resultStatus = h('div', { class: 'result-status' });
  const resultBody = h('div', { class: 'result-body' });
  const grid = createGrid();
  let gridShows = null; // the statement whose rows the grid shows
  const el = h('section', { class: 'page worksheets', 'aria-label': 'Worksheets' },
    tabs,
    h('div', { class: 'toolbar' },
      runBtn, cancelBtn,
      h('span', { class: 'hint' }, h('kbd', { text: navigator.platform.startsWith('Mac') ? '⌘' : 'Ctrl' }), h('kbd', { text: 'Enter' })),
      status, h('span', { class: 'spacer' }), downloadBtn),
    editorWrap, splitter,
    h('div', { class: 'results' }, resultTabs, resultStatus, resultBody));

  // Storage

  function load() {
    try {
      const s = JSON.parse(localStorage.getItem(storeKey));
      if (s && Array.isArray(s.sheets) && s.sheets.length) return s;
    } catch { /* start fresh */ }
    const id = newID();
    return { active: id, sheets: [{ id, name: 'Worksheet 1', sql: STARTER }] };
  }

  let saveTimer = 0;
  function save() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      try { localStorage.setItem(storeKey, JSON.stringify(state)); } catch { toast('Could not save worksheets in this browser', 'error'); }
    }, 300);
  }

  function newID() { return Math.random().toString(36).slice(2, 10); }
  function current() { return state.sheets.find((s) => s.id === state.active) || state.sheets[0]; }

  // Tabs

  function renderTabs() {
    clear(tabs, state.sheets.map((s) => {
      const selected = s.id === current().id;
      const tab = h('div', {
        class: 'sheet-tab', role: 'tab', tabindex: selected ? '0' : '-1', 'aria-selected': String(selected),
        title: 'Double-click to rename',
        onclick: () => activate(s.id),
        ondblclick: () => rename(s, tab),
        onkeydown: (e) => {
          if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activate(s.id); }
          if (e.key === 'F2') { e.preventDefault(); rename(s, tab); }
          if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
            const i = state.sheets.indexOf(s) + (e.key === 'ArrowRight' ? 1 : -1);
            const next = state.sheets[(i + state.sheets.length) % state.sheets.length];
            activate(next.id);
            tabs.querySelector('[aria-selected="true"]')?.focus();
          }
        },
      },
      running.has(s.id) ? h('span', { class: 'running', title: 'Running' }) : null,
      h('span', { class: 'name', text: s.name }),
      h('button', {
        class: 'btn ghost', type: 'button', 'aria-label': `Close ${s.name}`, tabindex: '-1',
        onclick: (e) => { e.stopPropagation(); close(s); },
      }, icon('x')));
      return tab;
    }), h('button', {
      class: 'btn ghost icon-only small sheet-add', type: 'button', 'aria-label': 'New worksheet', title: 'New worksheet',
      onclick: () => add(),
    }, icon('plus')));
  }

  function activate(id) {
    state.active = id;
    save();
    editor.value = current().sql;
    renderTabs();
    renderRunState();
    renderResults();
  }

  function add(sql = '', name) {
    const id = newID();
    let n = state.sheets.length + 1;
    while (state.sheets.some((s) => s.name === `Worksheet ${n}`)) n++;
    state.sheets.push({ id, name: name || `Worksheet ${n}`, sql });
    activate(id);
    editor.focus();
  }

  async function close(sheet) {
    if (sheet.sql.trim() && !(await confirmDialog('Close worksheet', `Close “${sheet.name}”? Its SQL will be deleted from this browser.`, { confirm: 'Close', danger: true }))) return;
    running.get(sheet.id)?.abort();
    results.delete(sheet.id);
    state.sheets = state.sheets.filter((s) => s !== sheet);
    if (!state.sheets.length) state.sheets.push({ id: newID(), name: 'Worksheet 1', sql: '' });
    if (state.active === sheet.id) state.active = state.sheets[0].id;
    activate(state.active);
  }

  function rename(sheet, tab) {
    const input = h('input', { value: sheet.name, 'aria-label': 'Worksheet name' });
    const done = (keep) => {
      if (keep && input.value.trim()) sheet.name = input.value.trim().slice(0, 60);
      save();
      renderTabs();
    };
    input.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter') done(true);
      if (e.key === 'Escape') done(false);
    });
    input.addEventListener('blur', () => done(true));
    input.addEventListener('click', (e) => e.stopPropagation());
    tab.replaceChildren(input);
    input.focus();
    input.select();
  }

  // Running

  function renderRunState() {
    const busy = running.has(current().id);
    runBtn.disabled = busy;
    cancelBtn.hidden = !busy;
    if (!busy && !results.get(current().id)) status.textContent = '';
  }

  async function run() {
    const sheet = current();
    if (running.has(sheet.id)) return;
    const { text, offset } = editor.selection();
    if (!text.trim()) { toast('Nothing to run'); return; }
    const controller = new AbortController();
    running.set(sheet.id, controller);
    const runState = { base: offset, statements: [], selected: -1, total: null, started: performance.now(), canceled: false };
    results.set(sheet.id, runState);
    renderTabs();
    renderRunState();
    renderResults();
    const timer = setInterval(() => {
      if (sheet === current()) status.textContent = `Running… ${fmtMs(performance.now() - runState.started)}`;
    }, 100);

    let pending = 0;
    const refresh = () => {
      if (pending) return;
      pending = requestAnimationFrame(() => { pending = 0; if (sheet === current()) renderResults(true); });
    };
    try {
      await api.stream(text, (ev) => {
        const cur = runState.statements[runState.statements.length - 1];
        if (ev.statement) {
          runState.statements.push({ ...ev.statement, columns: null, rows: [], tag: '', error: null, done: false });
          runState.selected = runState.statements.length - 1;
          if (sheet === current()) renderResults();
        } else if (ev.columns) {
          cur.columns = ev.columns;
          if (sheet === current()) renderResults();
        } else if (ev.rows) {
          for (const r of ev.rows) cur.rows.push(r);
          refresh();
        } else if (ev.complete) {
          Object.assign(cur, { tag: ev.complete.tag, truncated: ev.complete.truncated, shortened: ev.complete.shortened, ms: ev.complete.ms, done: true });
        } else if (ev.error) {
          Object.assign(cur, { error: ev.error, ms: ev.error.ms, done: true });
        } else if (ev.done) {
          runState.total = ev.done.ms;
        }
      }, controller.signal);
    } catch (e) {
      if (e.name === 'AbortError') {
        runState.canceled = true;
      } else {
        runState.failure = e;
      }
    } finally {
      clearInterval(timer);
      running.delete(sheet.id);
      // Show the last result with rows, or the statement that failed.
      const failed = runState.statements.findIndex((s) => s.error);
      if (failed >= 0) runState.selected = failed;
      else {
        for (let i = runState.statements.length - 1; i >= 0; i--) {
          if (runState.statements[i].columns) { runState.selected = i; break; }
        }
      }
      renderTabs();
      if (sheet === current()) {
        renderRunState();
        renderResults();
      }
    }
  }

  function cancel() {
    running.get(current().id)?.abort();
  }

  async function downloadCSV() {
    const r = results.get(current().id);
    const stmt = r && r.statements[r.selected];
    if (!stmt || !stmt.columns) return;
    downloadBtn.disabled = true;
    const label = downloadBtn.lastChild;
    try {
      const ok = await api.download(stmt.text, {
        onProgress: (bytes) => { label.textContent = `Downloading ${(bytes / 1e6).toFixed(1)} MB`; },
      });
      if (ok) toast('Download complete');
    } catch (e) {
      toast(`Download failed: ${e.message}`, 'error');
    } finally {
      label.textContent = 'Download CSV';
      renderResults();
    }
  }

  // Results

  function lineOf(offset) {
    return editor.value.slice(0, offset).split('\n').length;
  }

  function renderResults(rowsOnly = false) {
    const r = results.get(current().id);
    const stmt = r && r.statements[r.selected];
    if (rowsOnly && stmt && stmt.columns) {
      grid.more();
      renderStatus(r, stmt);
      return;
    }
    downloadBtn.disabled = !(stmt && stmt.columns && stmt.done && !stmt.error) || running.has(current().id);
    clear(resultTabs);
    resultTabs.hidden = !r || r.statements.length < 2;
    if (r && r.statements.length > 1) {
      r.statements.forEach((s, i) => {
        resultTabs.append(h('button', {
          class: `result-tab${s.error ? ' failed' : ''}`, type: 'button', role: 'tab',
          'aria-selected': String(i === r.selected),
          onclick: () => { r.selected = i; renderResults(); },
          text: `${i + 1} · ${s.error ? 'Error' : s.tag || '…'}`,
        }));
      });
    }
    if (!r) {
      clear(resultStatus, h('span', { text: 'Results appear here.' }));
      clear(resultBody, h('div', { class: 'result-message', text: 'Run a statement to see its result.' }));
      return;
    }
    renderStatus(r, stmt);
    if (r.failure) {
      clear(resultBody, errorBox({ code: r.failure.code, message: r.failure.message }, null));
      return;
    }
    if (!stmt) {
      clear(resultBody, h('div', { class: 'result-message', text: r.canceled ? 'Canceled.' : 'Running…' }));
      return;
    }
    if (stmt.error) {
      clear(resultBody, errorBox(stmt.error, r.base + stmt.offset));
      return;
    }
    if (r.canceled && !stmt.done && !stmt.rows.length) {
      clear(resultBody, h('div', { class: 'result-message', text: 'Canceled before any rows arrived.' }));
      return;
    }
    if (stmt.columns) {
      if (gridShows === stmt && grid.el.parentNode === resultBody) {
        grid.more();
      } else {
        clear(resultBody, grid.el);
        grid.set(stmt.columns, stmt.rows);
        gridShows = stmt;
      }
      return;
    }
    clear(resultBody, h('div', { class: 'result-message', text: stmt.done ? `${stmt.tag}` : 'Running…' }));
  }

  function renderStatus(r, stmt) {
    const parts = [];
    if (stmt) {
      if (stmt.tag) parts.push(h('span', { class: 'tag', text: stmt.tag }));
      if (stmt.columns) parts.push(h('span', { text: plural(stmt.rows.length, 'row') + (stmt.done || r.canceled ? '' : '…') }));
      if (stmt.ms !== undefined) parts.push(h('span', { text: fmtMs(stmt.ms) }));
      if (stmt.truncated) {
        parts.push(h('span', { class: 'badge warn', text: `First ${stmt.rows.length.toLocaleString()} rows shown` }),
          h('span', { class: 'muted', text: 'Download CSV for all of them.' }));
      }
      if (stmt.shortened) parts.push(h('span', { class: 'badge warn', text: 'Long values cut short' }));
      if (r.statements.length > 1) parts.push(h('span', { class: 'faint', text: `statement ${stmt.index + 1} of ${r.statements.length}` }));
    }
    if (r.canceled) parts.push(h('span', { class: 'badge warn', text: 'Canceled' }));
    if (r.total !== null && r.statements.length > 1) parts.push(h('span', { class: 'faint', text: `total ${fmtMs(r.total)}` }));
    clear(resultStatus, parts);
    if (!running.has(current().id)) {
      status.textContent = r.canceled ? 'Canceled' : r.total !== null ? `Finished in ${fmtMs(r.total)}` : '';
    }
  }

  function errorBox(e, offset) {
    return h('div', { class: 'sql-error', role: 'alert' },
      h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
      h('pre', { text: e.message }),
      e.detail ? h('div', { class: 'extra', text: `Detail: ${e.detail}` }) : null,
      e.hint ? h('div', { class: 'extra', text: `Hint: ${e.hint}` }) : null,
      offset === null ? null : h('div', { class: 'where' },
        'In the statement starting on ',
        h('a', { href: '#', text: `line ${lineOf(offset)}`, onclick: (ev) => { ev.preventDefault(); editor.goTo(offset); } }),
        '.'));
  }

  // Resizing the editor

  function setEditorHeight(px) {
    const max = Math.max(120, el.clientHeight - 220);
    const height = Math.round(Math.min(Math.max(px, 90), max));
    editorWrap.style.height = `${height}px`;
    return height;
  }
  let height = Number(localStorage.getItem(heightKey)) || 260;
  setEditorHeight(height);
  splitter.addEventListener('pointerdown', (e) => {
    e.preventDefault();
    splitter.setPointerCapture(e.pointerId);
    const startY = e.clientY;
    const start = editorWrap.getBoundingClientRect().height;
    const move = (ev) => { height = setEditorHeight(start + ev.clientY - startY); };
    const up = () => {
      splitter.removeEventListener('pointermove', move);
      splitter.removeEventListener('pointerup', up);
      try { localStorage.setItem(heightKey, String(height)); } catch { /* ignore */ }
    };
    splitter.addEventListener('pointermove', move);
    splitter.addEventListener('pointerup', up);
  });
  splitter.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
      e.preventDefault();
      height = setEditorHeight(height + (e.key === 'ArrowDown' ? 24 : -24));
      try { localStorage.setItem(heightKey, String(height)); } catch { /* ignore */ }
    }
  });

  editor.value = current().sql;
  renderTabs();
  renderRunState();
  renderResults();

  return {
    el,
    shown() {
      setEditorHeight(height);
      editor.focus();
    },
    // open adds a worksheet with sql, for other pages' "Query in a
    // worksheet" buttons.
    open(sql, name) { add(sql, name); },
  };
}
