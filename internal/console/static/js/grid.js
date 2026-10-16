// The results grid. It renders only the rows in view, so ten thousand rows
// scroll as smoothly as ten. Click a cell to select it, move with the arrow
// keys, copy it with Ctrl+C (Cmd+C), and open it with Enter or a double click.

import { h, dialog, toast } from './dom.js';

const NUMERIC = new Set(['smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision']);
// Rows rendered beyond the visible ones, above and below.
const OVERSCAN = 12;

export function createGrid() {
  const el = h('div', { class: 'grid', tabindex: '0', role: 'region', 'aria-label': 'Results' });
  let columns = [];
  let rows = [];
  let rowHeight = 28;
  let selected = null; // {r, c}
  let first = -1;
  let last = -1;
  let frame = 0;
  // measured is how many rows the column widths were worked out from.
  let measured = 0;
  let table, cols, tbody, top, bottom;

  function widths() {
    const sample = rows.slice(0, 200);
    measured = sample.length;
    return columns.map((col, i) => {
      let len = Math.max(col.name.length * 7.8, col.type.length * 6.8);
      for (const row of sample) {
        const v = row[i];
        len = Math.max(len, (v === null ? 4 : Math.min(String(v).length, 60)) * 7.6);
      }
      return Math.round(Math.min(Math.max(len + 26, 72), 440));
    });
  }

  function build() {
    el.replaceChildren();
    first = last = -1;
    if (!columns.length) return;
    const w = widths();
    cols = h('colgroup', {}, h('col'), w.map(() => h('col')));
    cols.children[0].style.width = '56px';
    w.forEach((px, i) => { cols.children[i + 1].style.width = `${px}px`; });
    const head = h('tr', {}, h('th', { class: 'rownum', scope: 'col', text: '#' }),
      columns.map((c) => h('th', { scope: 'col', title: `${c.name} (${c.type})` },
        h('span', { class: 'col-name', text: c.name }),
        h('span', { class: 'col-type', text: c.type }))));
    top = h('tr', { class: 'spacer' }, h('td', { colspan: String(columns.length + 1) }));
    bottom = h('tr', { class: 'spacer' }, h('td', { colspan: String(columns.length + 1) }));
    tbody = h('tbody', {}, top, bottom);
    table = h('table', {}, cols, h('thead', {}, head), tbody);
    table.style.width = `${56 + w.reduce((a, b) => a + b, 0)}px`;
    el.append(table);
    render(true);
  }

  function cell(r, c) {
    const v = rows[r][c];
    const td = h('td', { dataset: { r: String(r), c: String(c) } });
    if (v === null) {
      td.className = 'null';
      td.textContent = 'NULL';
    } else {
      td.textContent = v;
      if (NUMERIC.has(columns[c].type)) td.className = 'num';
      if (v.length > 30) td.title = v.length > 2000 ? `${v.slice(0, 2000)}…` : v;
    }
    if (selected && selected.r === r && selected.c === c) td.classList.add('selected');
    return td;
  }

  function render(force = false) {
    if (!tbody) return;
    const view = el.clientHeight || 400;
    const scroll = Math.max(0, el.scrollTop - 42);
    const from = Math.max(0, Math.floor(scroll / rowHeight) - OVERSCAN);
    const to = Math.min(rows.length, Math.ceil((scroll + view) / rowHeight) + OVERSCAN);
    if (!force && from === first && to === last) return;
    first = from;
    last = to;
    const frag = document.createDocumentFragment();
    for (let r = from; r < to; r++) {
      frag.append(h('tr', {}, h('td', { class: 'rownum', text: String(r + 1) }), columns.map((_, c) => cell(r, c))));
    }
    while (top.nextSibling !== bottom) top.nextSibling.remove();
    top.after(frag);
    top.firstChild.style.height = `${from * rowHeight}px`;
    bottom.firstChild.style.height = `${(rows.length - to) * rowHeight}px`;
    top.hidden = from === 0;
    bottom.hidden = to === rows.length;
    // Measure the real row height once rows are on screen.
    const sample = top.nextSibling;
    if (sample && sample !== bottom && sample.offsetHeight && Math.abs(sample.offsetHeight - rowHeight) > 0.5) {
      rowHeight = sample.offsetHeight;
      render(true);
    }
  }

  function schedule(force) {
    if (frame) return;
    frame = requestAnimationFrame(() => { frame = 0; render(force); });
  }

  function select(r, c, scroll = true) {
    if (!rows.length) return;
    r = Math.max(0, Math.min(rows.length - 1, r));
    c = Math.max(0, Math.min(columns.length - 1, c));
    selected = { r, c };
    if (scroll) {
      const y = 42 + r * rowHeight;
      if (y - 42 < el.scrollTop) el.scrollTop = y - 42;
      else if (y + rowHeight > el.scrollTop + el.clientHeight) el.scrollTop = y + rowHeight - el.clientHeight;
      const td = tbody.querySelector(`td[data-r="${r}"][data-c="${c}"]`);
      if (td) td.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
    render(true);
  }

  function open(r, c) {
    const col = columns[c];
    let v = rows[r][c];
    if (v !== null && col.type === 'json') {
      try { v = JSON.stringify(JSON.parse(v), null, 2); } catch { /* show as is */ }
    }
    dialog({
      title: `${col.name} · row ${r + 1}`,
      wide: true,
      body: [h('p', { class: 'muted', text: col.type }), h('pre', { class: 'value-view mono', text: v === null ? 'NULL' : v })],
      actions: [
        { label: 'Copy', value: 'copy' },
        { label: 'Close', value: null, primary: true, kind: 'primary' },
      ],
    }).then((action) => { if (action === 'copy') copy(rows[r][c]); });
  }

  function copy(v) {
    navigator.clipboard.writeText(v === null ? '' : v).then(
      () => toast('Copied'),
      () => toast('Could not copy to the clipboard', 'error'));
  }

  el.addEventListener('scroll', () => schedule(false));
  el.addEventListener('click', (e) => {
    const td = e.target.closest('td[data-r]');
    if (td) select(Number(td.dataset.r), Number(td.dataset.c), false);
  });
  el.addEventListener('dblclick', (e) => {
    const td = e.target.closest('td[data-r]');
    if (td) open(Number(td.dataset.r), Number(td.dataset.c));
  });
  el.addEventListener('keydown', (e) => {
    if (!selected) {
      if (['ArrowDown', 'ArrowRight', 'ArrowUp', 'ArrowLeft'].includes(e.key) && rows.length) {
        e.preventDefault();
        select(0, 0);
      }
      return;
    }
    const { r, c } = selected;
    const page = Math.max(1, Math.floor(el.clientHeight / rowHeight) - 2);
    const moves = {
      ArrowDown: [r + 1, c], ArrowUp: [r - 1, c], ArrowLeft: [r, c - 1], ArrowRight: [r, c + 1],
      PageDown: [r + page, c], PageUp: [r - page, c], Home: [r, 0], End: [r, columns.length - 1],
    };
    if (moves[e.key]) {
      e.preventDefault();
      select(...moves[e.key]);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      open(r, c);
    } else if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c' && !window.getSelection().toString()) {
      e.preventDefault();
      copy(rows[r][c]);
    }
  });
  new ResizeObserver(() => schedule(false)).observe(el);

  return {
    el,
    // set shows a new result. rows may keep growing; call more() after.
    set(newColumns, newRows) {
      columns = newColumns || [];
      rows = newRows || [];
      selected = null;
      el.scrollTop = 0;
      el.scrollLeft = 0;
      build();
    },
    // more redraws after rows were appended, re-measuring column widths
    // while the first rows arrive.
    more() {
      if (!table || measured < Math.min(rows.length, 200)) build();
      else schedule(true);
    },
  };
}
