// The data browser: schemas, tables and views with their columns, comments,
// policies, tags, definitions and a sample of rows. Everything is read with
// SQL as the signed-in user, so it shows what that user may see: masked
// values and only the rows that row access policies let through.

import * as api from './api.js';
import { h, clear, icon, button, plural, mark, fmtMs } from './dom.js';
import { createGrid } from './grid.js';

// Tables of the current database (the Iceberg catalog), and views: Igloo
// keeps a copy of each catalog view in DuckDB's own database.
const OBJECTS = `
SELECT schema_name, table_name AS name, 'table' AS kind, database_name, current_database() AS catalog FROM duckdb_tables()
  WHERE database_name = current_database() AND NOT temporary AND NOT internal
UNION ALL
SELECT schema_name, view_name, 'view', database_name, current_database() FROM duckdb_views()
  WHERE NOT temporary AND NOT internal AND schema_name NOT IN ('information_schema', 'pg_catalog')
UNION ALL
SELECT schema_name, NULL, 'schema', database_name, current_database() FROM duckdb_schemas()
  WHERE database_name = current_database() AND NOT internal AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY 1, 2`;

export function createData(app) {
  const filter = h('input', { type: 'search', placeholder: 'Filter tables and views', 'aria-label': 'Filter tables and views' });
  const tree = h('div', { class: 'tree', role: 'tree', 'aria-label': 'Schemas' });
  const detail = h('div', { class: 'detail' });
  const el = h('section', { class: 'page data', 'aria-label': 'Data' },
    h('div', { class: 'browser-layout' },
      h('div', { class: 'browser' },
        h('div', { class: 'browser-head' }, filter,
          h('button', { class: 'btn icon-only', type: 'button', title: 'Reload', 'aria-label': 'Reload', onclick: () => load() }, icon('refresh'))),
        tree),
      detail));

  let schemas = new Map(); // name -> [{name, kind, database}]
  const collapsed = new Set();
  let selected = null;
  let loaded = false;
  let detailRun = null;

  filter.addEventListener('input', () => renderTree());

  async function load() {
    clear(tree, h('div', { class: 'result-message', text: 'Loading…' }));
    try {
      const res = await api.query(OBJECTS);
      schemas = new Map();
      for (const o of api.objects(res)) {
        if (!schemas.has(o.schema_name)) schemas.set(o.schema_name, []);
        if (o.kind !== 'schema') schemas.get(o.schema_name).push({ name: o.name, kind: o.kind, database: o.database_name, catalog: o.catalog, schema: o.schema_name });
      }
      loaded = true;
      renderTree();
      if (!selected) renderEmpty();
    } catch (e) {
      clear(tree, h('div', { class: 'sql-error' }, h('pre', { text: e.message })));
    }
  }

  function renderTree() {
    const q = filter.value.trim().toLowerCase();
    const groups = [];
    for (const [schema, objects] of schemas) {
      const shown = q ? objects.filter((o) => o.name.toLowerCase().includes(q) || schema.toLowerCase().includes(q)) : objects;
      if (q && !shown.length) continue;
      const open = q || !collapsed.has(schema);
      groups.push(h('li', { class: 'schema', role: 'treeitem', 'aria-expanded': String(Boolean(open)) },
        h('button', {
          type: 'button',
          onclick: () => { if (collapsed.has(schema)) collapsed.delete(schema); else collapsed.add(schema); renderTree(); },
        }, h('span', { class: 'chev' }, icon('chevron')), icon('schema'), h('span', { class: 'name', text: schema }),
        h('span', { class: 'count', text: String(objects.length) })),
        open ? h('ul', { role: 'group' }, shown.map((o) => h('li', {},
          h('button', {
            class: 'object', type: 'button', role: 'treeitem',
            'aria-selected': String(selected === o || (selected && selected.schema === o.schema && selected.name === o.name)),
            title: `${o.kind} ${schema}.${o.name}`,
            onclick: () => show(o),
          }, h('span', { class: `kind-${o.kind}` }, icon(o.kind)), h('span', { class: 'name', text: o.name }))))) : null));
    }
    if (!groups.length) {
      clear(tree, h('div', { class: 'result-message', text: q ? 'Nothing matches.' : 'No schemas.' }));
      return;
    }
    clear(tree, h('ul', {}, groups));
  }

  function renderEmpty() {
    const count = [...schemas.values()].reduce((n, o) => n + o.length, 0);
    clear(detail, h('div', { class: 'empty' },
      h('span', { class: 'mark' }, mark()),
      h('h2', { text: count ? 'Pick a table or view' : 'No tables yet' }),
      h('p', { text: count
        ? `${plural(count, 'table or view', 'tables and views')} in ${plural(schemas.size, 'schema')}. Choose one to see its columns and a sample of its rows.`
        : 'Create one in a worksheet, for example: CREATE TABLE events (id INTEGER, kind VARCHAR, at_time TIMESTAMP);' })));
  }

  // qualified is the name a worksheet uses for the object. The schema
  // resolves in the catalog, and Igloo maps catalog views to their copies.
  function qualified(o) {
    return `${api.ident(o.schema)}.${api.ident(o.name)}`;
  }

  async function show(o) {
    selected = o;
    renderTree();
    detailRun?.abort();
    detailRun = new AbortController();
    const signal = detailRun.signal;
    const panel = h('div', { class: 'tab-panel' }, h('div', { class: 'result-message', text: 'Loading…' }));
    const tabButtons = [];
    const tabs = h('div', { class: 'tabs', role: 'tablist' });
    const head = h('div', { class: 'detail-head' },
      h('div', { class: 'title' },
        h('span', { class: `kind-${o.kind}` }, icon(o.kind)),
        h('h1', { text: `${o.schema}.${o.name}` }),
        h('span', { class: 'badge', text: o.kind }),
        h('span', { class: 'spacer' }),
        button('Catalog page', { icon: 'catalog', onclick: () => { location.hash = `#/catalog/${[o.catalog, o.schema, o.name].map(encodeURIComponent).join('/')}`; } }),
        button('Query in a worksheet', { icon: 'open', onclick: () => app.openWorksheet(`SELECT *\nFROM ${qualified(o)}\nLIMIT 100;\n`, o.name) })),
      h('p', { class: 'comment' }));
    clear(detail, head, tabs, panel);

    const s = api.literal(o.schema);
    const n = api.literal(o.name);
    const d = api.literal(o.database);
    let info;
    try {
      // One request: column details (with comments), the object's comment,
      // a view's definition, and the policies and tags of a table.
      const results = await api.queryAll(`
SELECT c.column_name, c.data_type, c.is_nullable, coalesce(m.comment, c.comment) AS comment
FROM duckdb_columns() c
LEFT JOIN igloo_comments() m ON m.table_schema = c.schema_name AND m.table_name = c.table_name
  AND lower(m.column_name) = lower(c.column_name)
WHERE c.database_name = ${d} AND c.schema_name = ${s} AND c.table_name = ${n}
ORDER BY c.column_index;
SELECT coalesce(
  (SELECT comment FROM igloo_comments() WHERE table_schema = ${s} AND table_name = ${n} AND column_name IS NULL LIMIT 1),
  (SELECT comment FROM duckdb_tables() WHERE database_name = ${d} AND schema_name = ${s} AND table_name = ${n}),
  (SELECT comment FROM duckdb_views() WHERE database_name = ${d} AND schema_name = ${s} AND view_name = ${n})) AS comment;
SELECT sql FROM duckdb_views() WHERE database_name = ${d} AND schema_name = ${s} AND view_name = ${n};
SELECT policy_name, policy_kind, ref_column_name, tag_name FROM igloo.policy_references
WHERE ref_database_name = ${d} AND lower(ref_schema_name) = lower(${s}) AND lower(ref_entity_name) = lower(${n});
SELECT column_name, tag_name, tag_value FROM igloo.tag_references
WHERE object_database = ${d} AND lower(object_schema) = lower(${s}) AND lower(object_name) = lower(${n});`, signal);
      info = {
        columns: api.objects(results[0]),
        comment: results[1].rows[0]?.[0] || '',
        definition: results[2].rows[0]?.[0] || '',
        policies: api.objects(results[3]),
        tags: api.objects(results[4]),
      };
    } catch (e) {
      if (e.name === 'AbortError') return;
      clear(panel, h('div', { class: 'sql-error' }, h('pre', { text: e.message })));
      return;
    }
    head.querySelector('.comment').textContent = info.comment;
    for (const p of info.policies.filter((p) => p.policy_kind === 'ROW ACCESS POLICY')) {
      head.querySelector('.title .spacer').before(h('span', {
        class: 'badge warn', text: `row access: ${p.policy_name}`,
        title: 'Rows are filtered by this row access policy for every user, as it decides',
      }));
    }

    const views = {
      columns: () => columnsView(info.columns, info.policies, info.tags),
      sample: () => sampleView(o, signal),
      definition: () => h('pre', { class: 'definition mono', text: info.definition }),
    };
    const names = [['columns', `Columns (${info.columns.length})`], ['sample', 'Sample rows']];
    if (o.kind === 'view') names.push(['definition', 'Definition']);
    const select = (key) => {
      for (const b of tabButtons) b.setAttribute('aria-selected', String(b.dataset.key === key));
      panel.classList.toggle('flush', key === 'sample');
      clear(panel, views[key]());
    };
    for (const [key, label] of names) {
      const b = h('button', { class: 'tab', type: 'button', role: 'tab', dataset: { key }, text: label, onclick: () => select(key) });
      tabButtons.push(b);
      tabs.append(b);
    }
    select('columns');
  }

  function columnsView(columns, policies = [], tags = []) {
    if (!columns.length) return h('div', { class: 'result-message', text: 'No columns.' });
    const same = (a, b) => (a || '').toLowerCase() === (b || '').toLowerCase();
    // Badges for a column: its masking policy, directly or through a tag,
    // and its tags.
    const badges = (c) => [
      ...policies.filter((p) => p.policy_kind === 'MASKING POLICY' && same(p.ref_column_name, c.column_name))
        .map((p) => h('span', {
          class: 'badge warn', text: `masked: ${p.policy_name}`,
          title: p.tag_name ? `Masked by ${p.policy_name}, through tag ${p.tag_name}` : `Masked by ${p.policy_name}`,
        })),
      ...tags.filter((t) => same(t.column_name, c.column_name))
        .map((t) => h('span', { class: 'badge', text: `${t.tag_name} = ${t.tag_value}`, title: `Tag ${t.tag_name}` })),
    ];
    const governed = columns.some((c) => badges(c).length);
    const headings = ['Column', 'Type', 'Nullable', 'Comment'];
    if (governed) headings.splice(3, 0, 'Policies and tags');
    return h('div', { class: 'card table-wrap' }, h('table', { class: 'list' },
      h('thead', {}, h('tr', {}, headings.map((t) => h('th', { text: t })))),
      h('tbody', {}, columns.map((c) => h('tr', {},
        h('td', { class: 'mono', text: c.column_name }),
        h('td', { class: 'mono muted', text: c.data_type }),
        h('td', { text: ['t', 'true'].includes(c.is_nullable) ? 'yes' : 'no' }),
        governed ? h('td', { class: 'badges' }, badges(c)) : null,
        h('td', { text: c.comment || '' }))))));
  }

  function sampleView(o, signal) {
    const status = h('div', { class: 'result-status', text: 'Loading a sample…' });
    const body = h('div', { class: 'result-body' });
    const wrap = h('div', { class: 'results flush-results' }, status, body);
    const grid = createGrid();
    const sql = `SELECT * FROM ${qualified(o)} LIMIT 100`;
    const started = performance.now();
    api.query(sql, signal).then((res) => {
      clear(status, h('span', { class: 'tag', text: sql }), h('span', { text: plural(res.rows.length, 'row') }),
        h('span', { text: fmtMs(performance.now() - started) }));
      clear(body, grid.el);
      grid.set(res.columns, res.rows);
    }, (e) => {
      if (e.name === 'AbortError') return;
      clear(status);
      clear(body, h('div', { class: 'sql-error' },
        h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
        h('pre', { text: e.message })));
    });
    return wrap;
  }

  return {
    el,
    shown() {
      if (!loaded) load();
    },
  };
}
