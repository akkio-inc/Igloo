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

// hashFor is the address of a table's page.
function hashFor(schema, name) {
  return `#/catalog/${encodeURIComponent(schema)}/${encodeURIComponent(name)}`;
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
      h('a', { class: 'catalog-hit card', href: hashFor(m.schema, m.name) },
        h('div', { class: 'catalog-hit-title' },
          h('span', { class: `kind-${m.type === 'VIEW' ? 'view' : 'table'}` }, icon(m.type === 'VIEW' ? 'view' : 'table')),
          h('strong', { class: 'mono', text: `${m.schema}.${m.name}` }),
          m.type === 'VIEW' ? h('span', { class: 'badge', text: 'view' }) : null,
          ...(m.tags || []).slice(0, 4).map((t) => h('span', { class: 'badge', text: t }))),
        m.comment ? h('p', { class: 'catalog-hit-comment', text: m.comment }) : null,
        h('div', { class: 'catalog-hit-meta muted' },
          m.owner ? h('span', {}, 'Owner ', h('span', { class: 'mono', text: m.owner })) : h('span', { class: 'faint', text: 'No owner' }),
          m.rows !== null && m.rows !== undefined ? h('span', { text: plural(Number(m.rows), 'row') }) : null,
          m.bytes ? h('span', { text: fmtBytes(m.bytes) }) : null,
          m.last_altered ? h('span', { title: fmtTime(m.last_altered), text: `changed ${fmtAgo(m.last_altered)}` }) : null,
          m.reads_30d ? h('span', { text: `${plural(Number(m.reads_30d), 'read')} in 30 days` }) : null),
        m.matched?.length ? h('div', { class: 'catalog-hit-matched faint', text: `Matches ${m.matched.join(', ')}` }) : null)))));
  }

  // The page of one table or view.

  async function showObject(schema, name) {
    searchView.hidden = true;
    head.hidden = true;
    detail.hidden = false;
    run?.abort();
    run = new AbortController();
    const signal = run.signal;
    clear(detail, h('div', { class: 'result-message', text: 'Loading…' }));
    let page;
    try {
      page = await api.get(`/api/catalog/object?schema=${encodeURIComponent(schema)}&name=${encodeURIComponent(name)}`, { signal });
    } catch (e) {
      if (e.name === 'AbortError') return;
      clear(detail, backLink(), e.status === 404
        ? h('div', { class: 'empty' }, h('h2', { text: 'Not found' }), h('p', { text: `There is no table or view ${schema}.${name} that you may see.` }))
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
    const schema = o.table_schema;
    const name = o.table_name;
    const isView = o.table_type === 'VIEW';
    const canComment = page.can_comment;
    const qualified = `${api.ident(schema)}.${api.ident(name)}`;
    const u = page.usage || {};

    const stats = [
      ['Rows', isView ? '' : num(o.row_count)],
      ['Size', isView ? '' : fmtBytes(o.bytes), o.file_count ? plural(Number(o.file_count), 'file') : null],
      [isView ? 'Last replaced' : 'Data last changed', o.last_altered ? fmtAgo(o.last_altered) : 'never', o.last_altered ? fmtTime(o.last_altered) : null],
      ['Snapshots', isView ? '' : num(o.snapshot_count), isView ? null : 'kept for time travel'],
      ['Reads, 30 days', num(u.reads_30d) || '0', u.last_read ? `last ${fmtAgo(u.last_read)}` : null],
      ['Writes, 30 days', num(u.writes_30d) || '0', u.last_written ? `last ${fmtAgo(u.last_written)}` : null],
      ['Users, 30 days', num(u.users_30d) || '0'],
    ].filter((s) => s[1] !== '');

    const ownerText = o.table_owner ? `${o.table_owner}${o.owner_type === 'role' ? ' (role)' : ''}` : 'no owner';
    const top = h('div', { class: 'detail-head' },
      backLink(),
      h('div', { class: 'title' },
        h('span', { class: `kind-${isView ? 'view' : 'table'}` }, icon(isView ? 'view' : 'table')),
        h('h1', { text: `${schema}.${name}` }),
        h('span', { class: 'badge', text: isView ? 'view' : 'table' }),
        h('span', { class: 'badge', title: 'The owner may set its descriptions and hand it on', text: `owner: ${ownerText}` }),
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
  // downstream to the right, one column per step.
  function lineageView(g) {
    if (!g || g.nodes.length <= 1) {
      return h('div', { class: 'empty' }, h('h2', { text: 'No lineage recorded' }),
        h('p', { text: 'Lineage comes from the query history: statements that read tables or files and wrote this table, or read this table and wrote another, in the history\'s retention.' }));
    }
    const W = 210; const H = 38; const GAPX = 70; const GAPY = 16; const PAD = 12;
    const depths = g.nodes.map((n) => n.depth);
    const min = Math.min(...depths); const max = Math.max(...depths);
    const columns = new Map();
    for (const n of g.nodes) {
      if (!columns.has(n.depth)) columns.set(n.depth, []);
      columns.get(n.depth).push(n);
    }
    const tallest = Math.max(...[...columns.values()].map((c) => c.length));
    const width = (max - min + 1) * (W + GAPX) - GAPX + 2 * PAD;
    const height = tallest * (H + GAPY) - GAPY + 2 * PAD;
    const pos = new Map();
    for (const [depth, nodes] of columns) {
      const offset = (tallest - nodes.length) * (H + GAPY) / 2;
      nodes.forEach((n, i) => pos.set(n.id, { x: PAD + (depth - min) * (W + GAPX), y: PAD + offset + i * (H + GAPY) }));
    }
    const edges = g.edges.map((e) => {
      const a = pos.get(e.from); const b = pos.get(e.to);
      if (!a || !b) return null;
      const x1 = a.x + W; const y1 = a.y + H / 2; const x2 = b.x; const y2 = b.y + H / 2; const mid = (x1 + x2) / 2;
      const tip = [e.statements ? plural(Number(e.statements), 'statement') : '', e.last_seen ? `last ${fmtTime(e.last_seen)}` : '',
        e.job ? `by ${e.job}` : ''].filter(Boolean).join(', ');
      return svg('path', { class: 'lineage-edge', d: `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2 - 6},${y2}`, 'marker-end': 'url(#lineage-arrow)' },
        tip ? svg('title', { text: tip }) : null);
    });
    const label = (s) => (s.length > 28 ? `${s.slice(0, 27)}…` : s);
    const nodes = g.nodes.map((n) => {
      const p = pos.get(n.id);
      const link = n.kind === 'table' && n.depth !== 0;
      const open = () => { location.hash = hashFor(n.schema, n.name); };
      return svg('g', {
        class: `lineage-node ${n.kind}${n.depth === 0 ? ' root' : ''}${link ? ' link' : ''}`,
        transform: `translate(${p.x},${p.y})`,
        tabindex: link ? 0 : null, role: link ? 'link' : null, 'aria-label': n.label,
        onclick: link ? open : null,
        onkeydown: link ? (e) => { if (e.key === 'Enter') open(); } : null,
      },
      svg('title', { text: n.kind === 'location' ? `Files loaded from ${n.label}` : n.label }),
      svg('rect', { width: W, height: H, rx: 8 }),
      svg('text', { x: 12, y: H / 2 + 4, text: label(n.label) }));
    });
    const drawing = svg('svg', { class: 'lineage', width, height, viewBox: `0 0 ${width} ${height}`, role: 'img', 'aria-label': 'Lineage' },
      svg('defs', {}, svg('marker', { id: 'lineage-arrow', viewBox: '0 0 10 10', refX: 4, refY: 5, markerWidth: 8, markerHeight: 8, orient: 'auto-start-reverse' },
        svg('path', { class: 'lineage-arrow', d: 'M0,0 L10,5 L0,10 z' }))),
      edges, nodes);
    return h('div', { class: 'stack' },
      h('p', { class: 'muted', text: 'Where this table\'s data came from (left) and what was made from it (right), from the query history. Tables you may not see are counted, not named. Select a table to open it.' }),
      h('div', { class: 'card lineage-wrap' }, drawing));
  }

  // route shows what the address asks for: a table's page, or the search.
  let current = '';
  function route(param = current) {
    current = param;
    const parts = param ? param.split('/').map(decodeURIComponent) : [];
    if (parts.length === 2) {
      showObject(parts[0], parts[1]);
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
