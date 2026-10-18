// The catalog: search tables and views by name, description, column, tag
// and owner, and a page for each with what the catalog knows of it (owner,
// description, columns, size, freshness, usage), a sample of its rows and
// its lineage. The server reads it all with SQL as the signed-in user (see
// internal/console/catalog.go), so it shows only what they may see; the
// sample runs as they do, through their policies.

import * as api from './api.js';
import { h, clear, icon, button, plural, fmtTime, fmtAgo, fmtCount, toast, dialog } from './dom.js';
import { createGrid } from './grid.js';

const SVG = 'http://www.w3.org/2000/svg';

// svg builds an SVG element; text, like everywhere, is never markup.
function svg(tag, attrs = {}, ...children) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined) continue;
    if (k === 'text') el.textContent = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, String(v));
  }
  for (const c of children.flat()) if (c) el.append(c);
  return el;
}

function fmtBytes(n) {
  if (n === null || n === undefined || n === '') return '';
  let v = Number(n);
  for (const unit of ['bytes', 'KB', 'MB', 'GB', 'TB']) {
    if (v < 1024 || unit === 'TB') return unit === 'bytes' ? `${fmtCount(v)} bytes` : `${v.toFixed(v < 10 ? 1 : 0)} ${unit}`;
    v /= 1024;
  }
  return '';
}

function num(v) {
  return v === null || v === undefined ? '' : fmtCount(v);
}

// hashFor is the address of a table's page: in the Iceberg catalog, or a
// connection's.
function hashFor(catalog, schema, name) {
  return `#/catalog/${[catalog, schema, name].map(encodeURIComponent).join('/')}`;
}

// kindOf names a table type for people: external tables come from a Delta
// Sharing provider, and connections' tables (FOREIGN) from another
// database.
function kindOf(type) {
  return { VIEW: 'view', 'EXTERNAL TABLE': 'external table', FOREIGN: 'connection table' }[type] || 'table';
}

// titleOf names a table as people write it: a connection's in full.
function titleOf(type, catalog, schema, name) {
  return type === 'FOREIGN' ? `${catalog}.${schema}.${name}` : `${schema}.${name}`;
}

// balanceCards lays n stat cards out in as few rows as fit, at least
// CARD_MIN wide, with the rows as even as can be: seven cards that do not
// fit in one row make rows of four and three, not six and one.
const CARD_MIN = 140;
const CARD_GAP = 12;

function balanceCards(grid, n) {
  const fit = () => {
    if (!grid.isConnected) return;
    const perRow = Math.max(1, Math.floor((grid.clientWidth + CARD_GAP) / (CARD_MIN + CARD_GAP)));
    const rows = Math.ceil(n / perRow);
    grid.style.gridTemplateColumns = `repeat(${Math.ceil(n / rows)}, minmax(0, 1fr))`;
  };
  new ResizeObserver(fit).observe(grid);
}

// The lineage graph's geometry: node height, gaps between columns and
// between nodes, padding, the width of a character of a node's name
// (12.5px monospace), and the smallest the drawing shrinks to fit before
// it scrolls instead.
const NODE_H = 44;
const GAP_X = 56;
const GAP_Y = 18;
const PAD = 14;
const CHAR_W = 7.6;
const LINEAGE_MIN_SCALE = 0.75;

// lineageLayout places the lineage graph g (see internal/console/catalog.go)
// in columns: upstream tables left of the table, downstream ones right of
// it. A node's column is its longest path to or from the table, not the
// shortest, which the server's depths are, so every edge points right: a
// table that feeds another table as far from the table as itself, as
// raw.campaigns feeds media.advertisers when both feed media.campaigns,
// sits a column left of it, not in its column. Edges between neighbouring
// columns are curves; one that skips columns runs along a lane clear of
// the boxes it passes; one that points back, as a cycle in the lineage
// does, loops round the same way. In each column, nodes are ordered by
// where their neighbours nearer the table are, so edges cross less.
function lineageLayout(g) {
  const byId = new Map(g.nodes.map((n) => [n.id, n]));
  const root = byId.get(g.nodes.find((n) => n.depth === 0)?.id) || g.nodes[0];
  const edges = g.edges.filter((e) => e.from !== e.to && byId.has(e.from) && byId.has(e.to));
  const succ = new Map();
  const pred = new Map();
  for (const e of edges) {
    if (!succ.has(e.from)) succ.set(e.from, []);
    succ.get(e.from).push(e.to);
    if (!pred.has(e.to)) pred.set(e.to, []);
    pred.get(e.to).push(e.from);
  }

  // longest is the longest path from id to the table along next, over
  // nodes on side (-1 upstream, 1 downstream); a cycle ends a path.
  const memo = new Map();
  const visiting = new Set();
  const longest = (id, next, side) => {
    if (id === root.id) return 0;
    if (memo.has(id)) return memo.get(id);
    if (visiting.has(id)) return -Infinity;
    visiting.add(id);
    let best = -Infinity;
    for (const m of next.get(id) || []) {
      if (m === root.id || Math.sign(byId.get(m).depth) === side) best = Math.max(best, longest(m, next, side) + 1);
    }
    visiting.delete(id);
    if (best === -Infinity) best = Math.abs(byId.get(id).depth);
    memo.set(id, best);
    return best;
  };
  const nodes = g.nodes.map((n, index) => ({
    ...n, index,
    col: n.depth < 0 ? -longest(n.id, succ, -1) : n.depth > 0 ? longest(n.id, pred, 1) : 0,
  }));
  const pos = new Map(nodes.map((n) => [n.id, n]));

  // Boxes are as wide as the longest name needs, within limits.
  const chars = Math.max(...nodes.map((n) => {
    if (n.kind === 'table' && n.name) return Math.max(n.name.length, (n.label.length - n.name.length - 1) * 0.88);
    return n.kind === 'hidden' ? n.label.length * 0.8 : Math.min(n.label.length, 30);
  }));
  const W = Math.round(Math.min(260, Math.max(150, chars * CHAR_W + 26)));
  const maxChars = Math.floor((W - 26) / CHAR_W);
  const clip = (s) => (s.length > maxChars ? `${s.slice(0, maxChars - 1)}…` : s);

  const columns = new Map();
  for (const n of nodes) {
    if (!columns.has(n.col)) columns.set(n.col, []);
    columns.get(n.col).push(n);
  }
  const minCol = Math.min(...columns.keys());
  const maxCol = Math.max(...columns.keys());
  const tallest = Math.max(...[...columns.values()].map((c) => c.length));
  const height = tallest * (NODE_H + GAP_Y) - GAP_Y + 2 * PAD;
  // Edges that point back loop out past their columns' boxes.
  const loops = edges.some((e) => pos.get(e.to).col <= pos.get(e.from).col);
  const padX = loops ? GAP_X / 2 + 4 : PAD;
  const width = (maxCol - minCol + 1) * (W + GAP_X) - GAP_X + 2 * padX;
  const place = (col) => {
    const list = columns.get(col) || [];
    const offset = (tallest - list.length) * (NODE_H + GAP_Y) / 2;
    list.forEach((n, i) => {
      n.x = padX + (col - minCol) * (W + GAP_X);
      n.y = PAD + offset + i * (NODE_H + GAP_Y);
    });
  };
  const order = (col, nearer) => {
    const key = (n) => {
      const ys = (nearer.get(n.id) || []).map((id) => pos.get(id)).filter((m) => m.y !== undefined).map((m) => m.y);
      return ys.length ? ys.reduce((a, b) => a + b, 0) / ys.length : Infinity;
    };
    const keyed = (columns.get(col) || []).map((n) => [key(n), n]);
    keyed.sort((a, b) => (a[0] === b[0] ? 0 : a[0] < b[0] ? -1 : 1) || a[1].index - b[1].index);
    columns.set(col, keyed.map((k) => k[1]));
  };
  place(0);
  for (let c = -1; c >= minCol; c--) {
    order(c, succ);
    place(c);
  }
  for (let c = 1; c <= maxCol; c++) {
    order(c, pred);
    place(c);
  }

  // laneFor finds the height nearest ideal at which a line crosses the
  // columns cols without touching a box.
  const laneFor = (cols, ideal) => {
    const boxes = cols.flatMap((c) => columns.get(c) || []);
    const clear = (y) => boxes.every((n) => y < n.y - 3 || y > n.y + NODE_H + 3);
    let best = null;
    for (const y of [ideal, PAD / 2, height - PAD / 2, ...boxes.flatMap((n) => [n.y - GAP_Y / 2, n.y + NODE_H + GAP_Y / 2])]) {
      if (y >= PAD / 2 && y <= height - PAD / 2 && clear(y) && (best === null || Math.abs(y - ideal) < Math.abs(best - ideal))) best = y;
    }
    return best ?? PAD / 2;
  };
  const g2 = GAP_X / 2;
  const routed = edges.map((e) => {
    const a = pos.get(e.from);
    const b = pos.get(e.to);
    const x1 = a.x + W;
    const y1 = a.y + NODE_H / 2;
    const x2 = b.x;
    const y2 = b.y + NODE_H / 2;
    let d;
    if (b.col === a.col + 1) {
      const mid = (x1 + x2) / 2;
      d = `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2 - 6},${y2}`;
    } else if (b.col > a.col) {
      const cols = [];
      for (let c = a.col + 1; c < b.col; c++) cols.push(c);
      const lane = laneFor(cols, (y1 + y2) / 2);
      d = `M${x1},${y1} C${x1 + g2},${y1} ${x1 + g2},${lane} ${x1 + GAP_X},${lane} L${x2 - GAP_X},${lane} `
        + `C${x2 - g2},${lane} ${x2 - g2},${y2} ${x2 - 6},${y2}`;
    } else {
      const cols = [];
      for (let c = b.col; c <= a.col; c++) cols.push(c);
      const lane = laneFor(cols, Math.max(y1, y2) + NODE_H);
      d = `M${x1},${y1} C${x1 + g2},${y1} ${x1 + g2},${lane} ${x1},${lane} L${x2},${lane} `
        + `C${x2 - g2},${lane} ${x2 - g2},${y2} ${x2 - 6},${y2}`;
    }
    return { ...e, d };
  });
  return { nodes, edges: routed, W, H: NODE_H, width, height, root: pos.get(root.id), clip };
}

function errorView(e) {
  return h('div', { class: 'sql-error', role: 'alert' },
    h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
    h('pre', { text: e.message }));
}

export function createCatalog(app) {
  const input = h('input', {
    type: 'search', placeholder: 'Search tables by name, description, column, tag or owner',
    'aria-label': 'Search the catalog', autocomplete: 'off',
  });
  const count = h('span', { class: 'sub' });
  const results = h('div', { class: 'catalog-results' });
  const searchView = h('div', { class: 'page-body' }, results);
  const detail = h('div', { class: 'page-body catalog-object', hidden: true });
  const head = h('div', { class: 'page-head' }, h('h1', { text: 'Catalog' }),
    h('div', { class: 'catalog-search' }, icon('search'), input), count);
  const el = h('section', { class: 'page catalog', 'aria-label': 'Catalog' }, head, searchView, detail);

  let timer = 0;
  let run = null;
  input.addEventListener('input', () => {
    clearTimeout(timer);
    timer = setTimeout(() => search(input.value), 200);
  });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { clearTimeout(timer); search(input.value); }
  });

  async function search(q) {
    run?.abort();
    run = new AbortController();
    const signal = run.signal;
    clear(results, h('div', { class: 'result-message', text: 'Searching…' }));
    let res;
    try {
      res = await api.get(`/api/catalog/search?q=${encodeURIComponent(q)}`, { signal });
    } catch (e) {
      if (e.name !== 'AbortError') clear(results, errorView(e));
      return;
    }
    const found = res.results;
    count.textContent = found.length >= res.limit ? `the first ${res.limit}` : plural(found.length, 'table or view', 'tables and views');
    if (!found.length) {
      clear(results, h('div', { class: 'empty' }, h('h2', { text: q.trim() ? 'Nothing matches' : 'No tables yet' }),
        h('p', { text: q.trim()
          ? 'Search looks in the names, descriptions, columns, tags and owners of the tables and views you may see.'
          : 'There are no tables or views you may see. Ask an owner or an admin for a grant.' })));
      return;
    }
    clear(results, h('ul', { class: 'catalog-list' }, found.map((m) => h('li', {},
      h('a', { class: 'catalog-hit card', href: hashFor(m.catalog, m.schema, m.name) },
        h('div', { class: 'catalog-hit-title' },
          h('span', { class: `kind-${m.type === 'VIEW' ? 'view' : 'table'}` }, icon(m.type === 'VIEW' ? 'view' : 'table')),
          h('strong', { class: 'mono', text: titleOf(m.type, m.catalog, m.schema, m.name) }),
          kindOf(m.type) !== 'table' ? h('span', { class: 'badge', text: kindOf(m.type) }) : null,
          ...(m.tags || []).slice(0, 4).map((t) => h('span', { class: 'badge', text: t }))),
        m.comment ? h('p', { class: 'catalog-hit-comment', text: m.comment }) : null,
        h('div', { class: 'catalog-hit-meta muted' },
          m.provider ? h('span', {}, 'From ', h('span', { class: 'mono', text: m.provider })) : null,
          m.owner ? h('span', {}, 'Owner ', h('span', { class: 'mono', text: m.owner }))
            : m.type === 'FOREIGN' ? null : h('span', { class: 'faint', text: 'No owner' }),
          m.rows !== null && m.rows !== undefined ? h('span', { text: plural(Number(m.rows), 'row') }) : null,
          m.bytes ? h('span', { text: fmtBytes(m.bytes) }) : null,
          m.last_altered ? h('span', { title: fmtTime(m.last_altered), text: `changed ${fmtAgo(m.last_altered)}` }) : null,
          m.reads_30d ? h('span', { text: `${plural(Number(m.reads_30d), 'read')} in 30 days` }) : null),
        m.matched?.length ? h('div', { class: 'catalog-hit-matched faint', text: `Matches ${m.matched.join(', ')}` }) : null)))));
  }

  // The page of one table or view.

  async function showObject(catalog, schema, name) {
    searchView.hidden = true;
    head.hidden = true;
    detail.hidden = false;
    run?.abort();
    run = new AbortController();
    const signal = run.signal;
    clear(detail, h('div', { class: 'result-message', text: 'Loading…' }));
    let page;
    try {
      const q = new URLSearchParams({ catalog, schema, name });
      page = await api.get(`/api/catalog/object?${q}`, { signal });
    } catch (e) {
      if (e.name === 'AbortError') return;
      clear(detail, backLink(), e.status === 404
        ? h('div', { class: 'empty' }, h('h2', { text: 'Not found' }),
          h('p', { text: `There is no table or view ${catalog}.${schema}.${name} that you may see.` }))
        : errorView(e));
      return;
    }
    renderObject(page, signal);
  }

  function backLink() {
    return h('div', { class: 'catalog-back' }, h('a', { href: '#/catalog' }, icon('chevron'), 'Catalog'));
  }

  function editComment(title, current, change) {
    const area = h('textarea', { class: 'statement-box', rows: 5, 'aria-label': 'Description' });
    area.value = current || '';
    dialog({
      title,
      body: [area, h('p', { class: 'muted', text: 'Saved as the comment, which other engines that read the Iceberg tables see too. Leave it empty to remove it.' })],
      actions: [{ label: 'Cancel', value: null }, { label: 'Save', value: 'save', primary: true, kind: 'primary' }],
      onopen: () => area.focus(),
    }).then(async (v) => {
      if (v !== 'save') return;
      try {
        await api.post('/api/catalog/comment', { ...change, comment: area.value });
        toast('Saved');
        route();
      } catch (e) {
        toast(e.message, 'error');
      }
    });
  }

  function renderObject(page, signal) {
    const o = page.object;
    const catalog = o.table_catalog;
    const schema = o.table_schema;
    const name = o.table_name;
    const isView = o.table_type === 'VIEW';
    const foreign = o.table_type === 'FOREIGN';
    // Only the Iceberg catalog's own tables have files and snapshots.
    const own = !isView && !foreign && o.table_type !== 'EXTERNAL TABLE';
    const canComment = page.can_comment;
    const qualified = [...(foreign ? [catalog] : []), schema, name].map(api.ident).join('.');
    const u = page.usage || {};

    const stats = [
      ['Rows', own ? num(o.row_count) : ''],
      ['Size', own ? fmtBytes(o.bytes) : '', o.file_count ? plural(Number(o.file_count), 'file') : null],
      [isView ? 'Last replaced' : 'Data last changed', own || isView ? (o.last_altered ? fmtAgo(o.last_altered) : 'never') : '',
        o.last_altered ? fmtTime(o.last_altered) : null],
      ['Snapshots', own ? num(o.snapshot_count) : '', own ? 'kept for time travel' : null],
      ['Reads, 30 days', num(u.reads_30d) || '0', u.last_read ? `last ${fmtAgo(u.last_read)}` : null],
      ['Writes, 30 days', num(u.writes_30d) || '0', u.last_written ? `last ${fmtAgo(u.last_written)}` : null],
      ['Users, 30 days', num(u.users_30d) || '0'],
    ].filter((s) => s[1] !== '');

    const ownerText = o.table_owner ? `${o.table_owner}${o.owner_type === 'role' ? ' (role)' : ''}` : 'no owner';
    const top = h('div', { class: 'detail-head' },
      backLink(),
      h('div', { class: 'title' },
        h('span', { class: `kind-${isView ? 'view' : 'table'}` }, icon(isView ? 'view' : 'table')),
        h('h1', { text: titleOf(o.table_type, catalog, schema, name) }),
        h('span', { class: 'badge', text: kindOf(o.table_type) }),
        o.provider ? h('span', { class: 'badge', title: 'The Delta Sharing provider it is read from', text: `from ${o.provider}` }) : null,
        foreign ? null : h('span', { class: 'badge', title: 'The owner may set its descriptions and hand it on', text: `owner: ${ownerText}` }),
        h('span', { class: 'spacer' }),
        button('Query in a worksheet', { icon: 'open', onclick: () => app.openWorksheet(`SELECT *\nFROM ${qualified}\nLIMIT 100;\n`, name) })),
      h('div', { class: 'catalog-description' },
        h('p', { class: o.comment ? 'comment' : 'comment faint', text: o.comment || 'No description.' }),
        canComment ? button('Edit', {
          kind: 'small', onclick: () => editComment(`Describe ${schema}.${name}`, o.comment, { kind: 'table', schema, name }),
        }) : null),
      page.schema?.comment ? h('p', { class: 'muted', text: `Schema ${schema}: ${page.schema.comment}` }) : null);

    const statCards = h('div', { class: 'grid-cards catalog-stats' }, stats.map(([label, value, note]) =>
      h('div', { class: 'card stat' }, h('div', { class: 'label', text: label }), h('div', { class: 'value', text: value }),
        note ? h('div', { class: 'note', text: note }) : null)));
    balanceCards(statCards, stats.length);

    const panel = h('div', { class: 'tab-panel' });
    const tabs = h('div', { class: 'tabs', role: 'tablist' });
    const views = {
      columns: () => columnsView(page, canComment && !isView, schema, name),
      lineage: () => lineageView(page.lineage),
      sample: () => sampleView(qualified, signal),
      definition: () => h('pre', { class: 'definition mono', text: page.definition || '' }),
    };
    const names = [['columns', `Columns (${page.columns.length})`], ['lineage', 'Lineage'], ['sample', 'Sample rows']];
    if (isView && page.definition) names.push(['definition', 'Definition']);
    const buttons = [];
    const select = (key) => {
      for (const b of buttons) b.setAttribute('aria-selected', String(b.dataset.key === key));
      panel.classList.toggle('flush', key === 'sample');
      clear(panel, views[key]());
    };
    for (const [key, label] of names) {
      const b = h('button', { class: 'tab', type: 'button', role: 'tab', dataset: { key }, text: label, onclick: () => select(key) });
      buttons.push(b);
      tabs.append(b);
    }
    clear(detail, top, h('div', { class: 'catalog-body' }, statCards), tabs, panel);
    select('columns');
  }

  function columnsView(page, canComment, schema, name) {
    if (!page.columns.length) return h('div', { class: 'result-message', text: 'No columns.' });
    const same = (a, b) => (a || '').toLowerCase() === (b || '').toLowerCase();
    const badges = (c) => [
      ...page.policies.filter((p) => p.policy_kind === 'MASKING POLICY' && same(p.ref_column_name, c.column_name))
        .map((p) => h('span', { class: 'badge warn', text: `masked: ${p.policy_name}` })),
      ...page.tags.filter((t) => same(t.column_name, c.column_name))
        .map((t) => h('span', { class: 'badge', text: `${t.tag_name} = ${t.tag_value}` })),
    ];
    const rowPolicy = page.policies.find((p) => p.policy_kind === 'ROW ACCESS POLICY');
    const governed = page.columns.some((c) => badges(c).length);
    const headings = ['Column', 'Type', 'Description', ''];
    if (governed) headings.splice(3, 0, 'Policies and tags');
    return h('div', { class: 'stack' },
      rowPolicy ? h('p', { class: 'notice', text: `Rows are filtered by the row access policy ${rowPolicy.policy_name}.` }) : null,
      h('div', { class: 'card table-wrap' }, h('table', { class: 'list' },
        h('thead', {}, h('tr', {}, headings.map((t) => h('th', { text: t })))),
        h('tbody', {}, page.columns.map((c) => h('tr', {},
          h('td', { class: 'mono', text: c.column_name }),
          h('td', { class: 'mono muted', text: c.data_type }),
          h('td', { class: c.comment ? '' : 'faint', text: c.comment || '' }),
          governed ? h('td', {}, h('div', { class: 'badge-list' }, badges(c))) : null,
          h('td', { class: 'actions' }, canComment ? button('Edit', {
            kind: 'small ghost',
            onclick: () => editComment(`Describe ${name}.${c.column_name}`, c.comment, { kind: 'column', schema, name, column: c.column_name }),
          }) : null)))))));
  }

  function sampleView(qualified, signal) {
    const status = h('div', { class: 'result-status', text: 'Loading a sample…' });
    const body = h('div', { class: 'result-body' });
    const grid = createGrid();
    const sql = `SELECT * FROM ${qualified} LIMIT 100`;
    api.query(sql, signal).then((res) => {
      clear(status, h('span', { class: 'tag', text: sql }), h('span', { text: plural(res.rows.length, 'row') }),
        h('span', { class: 'muted', text: 'as you see them, through your policies' }));
      clear(body, grid.el);
      grid.set(res.columns, res.rows);
    }, (e) => {
      if (e.name === 'AbortError') return;
      clear(status);
      clear(body, errorView(e));
    });
    return h('div', { class: 'results flush-results' }, status, body);
  }

  // lineageView draws the lineage around a table: upstream to the left,
  // downstream to the right (see lineageLayout). The drawing shrinks to fit
  // the page down to LINEAGE_MIN_SCALE; below that it scrolls sideways,
  // starting at the table, with its edges faded and a hint that there is
  // more.
  function lineageView(g) {
    if (!g || g.nodes.length <= 1) {
      return h('div', { class: 'empty' }, h('h2', { text: 'No lineage recorded' }),
        h('p', { text: 'Lineage comes from the query history: statements that read tables or files and wrote this table, or read this table and wrote another, in the history\'s retention.' }));
    }
    const L = lineageLayout(g);
    const edges = L.edges.map((e) => {
      const tip = [e.statements ? plural(Number(e.statements), 'statement') : '', e.last_seen ? `last ${fmtTime(e.last_seen)}` : '',
        e.job ? `by ${e.job}` : ''].filter(Boolean).join(', ');
      return svg('path', { class: 'lineage-edge', d: e.d, 'marker-end': 'url(#lineage-arrow)' }, tip ? svg('title', { text: tip }) : null);
    });
    const nodes = L.nodes.map((n) => {
      const link = n.kind === 'table' && n.depth !== 0;
      const open = () => { location.hash = hashFor(n.catalog, n.schema, n.name); };
      // A table shows its schema small above its name; a location or a
      // count of hidden tables, one line.
      const lines = n.kind === 'table' && n.name
        ? [svg('text', { class: 'lineage-schema', x: 12, y: 17, text: L.clip(n.label.slice(0, -n.name.length - 1)) }),
          svg('text', { x: 12, y: 34, text: L.clip(n.name) })]
        : [svg('text', { x: 12, y: L.H / 2 + 4, text: n.kind === 'hidden' ? n.label : L.clip(n.label) })];
      return svg('g', {
        class: `lineage-node ${n.kind}${n.depth === 0 ? ' root' : ''}${link ? ' link' : ''}`,
        transform: `translate(${n.x},${n.y})`,
        tabindex: link ? 0 : null, role: link ? 'link' : null, 'aria-label': n.label,
        onclick: link ? open : null,
        onkeydown: link ? (e) => { if (e.key === 'Enter') open(); } : null,
      },
      svg('title', { text: n.kind === 'location' ? `Files loaded from ${n.label}` : n.label }),
      svg('rect', { width: L.W, height: L.H, rx: 8 }),
      lines);
    });
    const drawing = svg('svg', { class: 'lineage', width: L.width, height: L.height, viewBox: `0 0 ${L.width} ${L.height}`, role: 'img', 'aria-label': 'Lineage' },
      svg('defs', {}, svg('marker', { id: 'lineage-arrow', viewBox: '0 0 10 10', refX: 4, refY: 5, markerWidth: 8, markerHeight: 8, orient: 'auto-start-reverse' },
        svg('path', { class: 'lineage-arrow', d: 'M0,0 L10,5 L0,10 z' }))),
      edges, nodes);
    const wrap = h('div', { class: 'lineage-wrap' }, drawing);
    const card = h('div', { class: 'card lineage-card' }, wrap);
    const hint = h('p', { class: 'lineage-hint muted', hidden: true }, icon('chevron'), 'Scroll sideways for the rest of the graph');
    let centered = false;
    const edgesShown = () => {
      card.classList.toggle('more-left', wrap.scrollLeft > 2);
      card.classList.toggle('more-right', wrap.scrollLeft + wrap.clientWidth < wrap.scrollWidth - 2);
    };
    const fit = () => {
      if (!wrap.isConnected) return;
      const style = getComputedStyle(wrap);
      const room = wrap.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
      const scale = Math.max(LINEAGE_MIN_SCALE, Math.min(1, room / L.width));
      drawing.setAttribute('width', String(Math.round(L.width * scale)));
      drawing.setAttribute('height', String(Math.round(L.height * scale)));
      const scrolls = L.width * scale > room + 1;
      hint.hidden = !scrolls;
      if (scrolls && !centered) {
        // Start at the table, not at the far end of its upstream.
        centered = true;
        wrap.scrollLeft = Math.max(0, (L.root.x + L.W / 2) * scale - wrap.clientWidth / 2);
      }
      edgesShown();
    };
    wrap.addEventListener('scroll', edgesShown, { passive: true });
    new ResizeObserver(fit).observe(wrap);
    return h('div', { class: 'stack' },
      h('p', { class: 'muted', text: 'Where this table\'s data came from (left) and what was made from it (right), from the query history. Tables you may not see are counted, not named. Select a table to open it.' }),
      h('div', { class: 'lineage-block' }, card, hint));
  }

  // route shows what the address asks for: a table's page, or the search.
  let current = '';
  function route(param = current) {
    current = param;
    const parts = param ? param.split('/').map(decodeURIComponent) : [];
    if (parts.length === 3) {
      showObject(parts[0], parts[1], parts[2]);
      return;
    }
    detail.hidden = true;
    head.hidden = false;
    searchView.hidden = false;
    search(input.value);
  }

  return {
    el,
    shown(param = '') {
      route(param);
      if (!param) input.focus();
    },
  };
}
