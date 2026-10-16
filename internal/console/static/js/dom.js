// DOM helpers. Everything the console shows is built with these, from
// elements and text nodes: no HTML strings, so data can never become markup.

const SVG = 'http://www.w3.org/2000/svg';

// h builds an element. attrs: class, text, dataset, on<event> handlers,
// properties (value, checked, disabled, hidden, ...) and other attributes.
// Children are nodes, strings or arrays of them; null and false are skipped.
export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') el.className = value;
    else if (key === 'text') el.textContent = value;
    else if (key === 'dataset') Object.assign(el.dataset, value);
    else if (key.startsWith('on')) el.addEventListener(key.slice(2), value);
    else if (key in el && typeof value !== 'string') el[key] = value;
    else if (key === 'value') el.value = value;
    else el.setAttribute(key, value === true ? '' : value);
  }
  append(el, children);
  return el;
}

export function append(el, children) {
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

export function clear(el, ...children) {
  el.replaceChildren();
  return append(el, children);
}

function svg(viewBox, paths, cls) {
  const s = document.createElementNS(SVG, 'svg');
  s.setAttribute('viewBox', viewBox);
  s.setAttribute('aria-hidden', 'true');
  for (const [d, c] of paths) {
    const p = document.createElementNS(SVG, 'path');
    p.setAttribute('d', d);
    if (c || cls) p.setAttribute('class', c || cls);
    s.append(p);
  }
  return s;
}

// The igloo mark, styled by app.css so it follows the theme.
export function mark() {
  return svg('3 23 56 30', [
    ['M6 50A24 24 0 0 1 54 50Z', 'mk-dome'],
    ['M7.4 42H52.6M12.1 34H47.9M21.6 27.5H38.4M18 42V50M30 42V50M24 34V42M36 34V42M30 27.5V34', 'mk-joints'],
    ['M40 50V43a8 8 0 0 1 16 0V50Z', 'mk-tunnel'],
    ['M44 50V46a4 4 0 0 1 8 0V50Z', 'mk-door'],
  ]);
}

const ICONS = {
  sheet: 'M5 3h10l4 4v14H5zM14 3v5h5M8 12h8M8 16h6',
  data: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3',
  history: 'M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 7v5l3 3',
  users: 'M16 20v-1.5a3.5 3.5 0 0 0-3.5-3.5h-5A3.5 3.5 0 0 0 4 18.5V20M10 11a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7zM20 20v-1.5a3.5 3.5 0 0 0-2.6-3.4M15.5 4.2a3.5 3.5 0 0 1 0 6.6',
  key: 'M14.5 9.5a4.5 4.5 0 1 0-4.2 4.5L12 15.7V18h2.3v2.3H17V22h3v-3l-5.8-5.8a4.5 4.5 0 0 0 .3-3.7zM9 9h.01',
  cpu: 'M7 7h10v10H7zM10 10h4v4h-4zM9 3v4M15 3v4M9 17v4M15 17v4M3 9h4M3 15h4M17 9h4M17 15h4',
  play: 'M7 4.5v15l12-7.5z',
  stop: 'M6 6h12v12H6z',
  plus: 'M12 5v14M5 12h14',
  x: 'M6 6l12 12M18 6L6 18',
  download: 'M12 4v11M7 10l5 5 5-5M5 20h14',
  refresh: 'M20 11a8 8 0 0 0-14.3-4.9L4 8M4 4v4h4M4 13a8 8 0 0 0 14.3 4.9L20 16M20 20v-4h-4',
  table: 'M4 5h16v14H4zM4 10h16M10 10v9',
  view: 'M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12zM12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z',
  chevron: 'M6 9l6 6 6-6',
  schema: 'M3 7l9-4 9 4-9 4zM3 12l9 4 9-4M3 17l9 4 9-4',
  sun: 'M12 16a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4',
  moon: 'M20 14.5A8.5 8.5 0 0 1 9.5 4 8.5 8.5 0 1 0 20 14.5z',
  monitor: 'M3 4h18v12H3zM8 20h8M12 16v4',
  search: 'M11 18a7 7 0 1 0 0-14 7 7 0 0 0 0 14zM20 20l-4-4',
  open: 'M14 4h6v6M20 4l-9 9M18 14v6H4V6h6',
  clock: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM12 7v5l3 2',
  inbox: 'M3 13h5l1.5 3h5l1.5-3h5M5.5 5h13L21 13v6H3v-6z',
  token: 'M4 7h16v3a2 2 0 0 0 0 4v3H4v-3a2 2 0 0 0 0-4zM10 7v10',
  bot: 'M6 9h12v10H6zM12 5v4M9.5 14h.01M14.5 14h.01M3 13v3M21 13v3',
  shield: 'M12 3l7 3v6c0 4.5-3 7.5-7 9-4-1.5-7-4.5-7-9V6zM9 12l2 2 4-4',
  copy: 'M9 9h11v11H9zM5 15H4V4h11v1',
};

export function icon(name) {
  const span = h('span', { class: 'icon' });
  span.append(svg('0 0 24 24', [[ICONS[name] || '']]));
  return span;
}

// Fill [data-icon] and [data-mark] placeholders in static markup.
export function hydrate(root = document) {
  for (const el of root.querySelectorAll('[data-icon]')) el.replaceWith(icon(el.dataset.icon));
  for (const el of root.querySelectorAll('[data-mark]')) {
    el.removeAttribute('data-mark');
    el.replaceChildren(mark());
  }
}

export function button(label, { icon: name, kind = '', title, onclick, type = 'button', disabled } = {}) {
  return h('button', { class: `btn ${kind}`.trim(), type, title, onclick, disabled },
    name ? icon(name) : null, label);
}

// Formatting

export function fmtMs(ms) {
  if (ms === undefined || ms === null || Number.isNaN(ms)) return '';
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
  const s = Math.round(ms / 1000);
  if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
  return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min`;
}

export function fmtCount(n) {
  return Number(n).toLocaleString();
}

export function plural(n, one, many = `${one}s`) {
  return `${fmtCount(n)} ${n === 1 ? one : many}`;
}

export function fmtTime(value) {
  if (!value) return '';
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function fmtAgo(value, now = Date.now()) {
  if (!value) return '';
  const d = value instanceof Date ? value : new Date(value);
  const s = Math.max(0, Math.round((now - d.getTime()) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min ago`;
  return `${Math.floor(s / 86400)} d ago`;
}

export function roleBadge(role) {
  return h('span', { class: `badge ${role}`, text: role });
}

// Toasts

export function toast(message, kind = '') {
  const t = h('div', { class: `toast ${kind}`, role: kind === 'error' ? 'alert' : 'status', text: message });
  document.getElementById('toasts').append(t);
  setTimeout(() => t.remove(), kind === 'error' ? 7000 : 3500);
}

// Dialogs. Each resolves with the value of the action pressed, or null when
// dismissed.

export function dialog({ title, body = [], actions = [], wide = false, onopen }) {
  return new Promise((resolve) => {
    let result = null;
    const d = h('dialog', { class: wide ? 'wide' : '', 'aria-label': title });
    const form = h('form', {
      onsubmit: (e) => {
        e.preventDefault();
        const primary = actions.find((a) => a.primary);
        if (primary && (!primary.validate || primary.validate())) {
          result = primary.value;
          d.close();
        }
      },
    },
    h('div', { class: 'dialog-head' }, h('h2', { text: title })),
    h('div', { class: 'dialog-body' }, body),
    h('div', { class: 'dialog-foot' }, actions.map((a) => h('button', {
      class: `btn ${a.kind || ''}`,
      type: a.primary ? 'submit' : 'button',
      text: a.label,
      onclick: a.primary ? null : () => { result = a.value; d.close(); },
    }))));
    d.append(form);
    d.addEventListener('close', () => { d.remove(); resolve(result); });
    document.body.append(d);
    d.showModal();
    if (onopen) onopen(d);
  });
}

export function confirmDialog(title, message, { confirm = 'Confirm', danger = false } = {}) {
  return dialog({
    title,
    body: [h('p', { text: message })],
    actions: [
      { label: 'Cancel', value: false },
      { label: confirm, value: true, primary: true, kind: danger ? 'danger solid' : 'primary' },
    ],
  }).then((v) => v === true);
}
