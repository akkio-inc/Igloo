// Clean rooms the signed-in user takes part in (admins: every room), read
// with SQL as the user: SHOW CLEAN ROOMS, and SHOW COLLABORATORS, DATASETS,
// TEMPLATES and RUNS IN CLEAN ROOM. Approving and running templates are
// ALTER CLEAN ROOM ... APPROVE TEMPLATE and RUN CLEAN ROOM; the server
// decides who may, and results come from the requester's result table,
// which only its party reads. Servers without clean rooms say so.

import * as api from './api.js';
import { h, clear, icon, button, fmtTime, plural, mark, toast, dialog } from './dom.js';
import { createGrid } from './grid.js';

// SQLSTATEs meaning the statement is not there or clean rooms are not set
// up.
const MISSING = new Set(['42601', '0A000']);

const yes = (v) => v === true || v === 't' || v === 'true';
const split = (v) => (v ? String(v).split(', ').filter(Boolean) : []);

function errorView(e) {
  return h('div', { class: 'sql-error', role: 'alert' },
    h('div', { class: 'head' }, h('span', { text: 'Error' }), e.code ? h('span', { class: 'badge code error', text: e.code }) : null),
    h('pre', { text: e.message }));
}

function statusBadge(s) {
  const cls = { succeeded: 'ok', failed: 'error', refused: 'warn', running: 'warn' }[s] ?? '';
  return h('span', { class: `badge ${cls}`.trim(), text: s });
}

function table(headings, rows) {
  return h('div', { class: 'card table-wrap' }, h('table', { class: 'list' },
    h('thead', {}, h('tr', {}, headings.map((t) => h('th', { text: t })))),
    h('tbody', {}, rows)));
}

export function createCleanRooms(app) {
  let rooms = [];
  let selected = null;
  let tab = 'templates';

  const count = h('span', { class: 'sub' });
  const roomList = h('div', { class: 'history-list' });
  const detail = h('aside', { class: 'history-detail', 'aria-label': 'Clean room' });
  const layout = h('div', { class: 'history-layout room-detail' }, roomList, detail);
  const body = h('div', { class: 'page' });
  const el = h('section', { class: 'page history', 'aria-label': 'Clean rooms' },
    h('div', { class: 'page-head' }, h('h1', { text: 'Clean rooms' }), count, h('span', { class: 'spacer' }),
      button('Refresh', { icon: 'refresh', onclick: () => refresh() })),
    body);

  async function refresh() {
    clear(body, h('div', { class: 'result-message', text: 'Loading…' }));
    try {
      rooms = api.objects(await api.query('SHOW CLEAN ROOMS'));
    } catch (e) {
      clear(body, MISSING.has(e.code) ? h('div', { class: 'empty' }, h('span', { class: 'mark' }, mark()),
        h('h2', { text: 'Clean rooms are not available here' }), h('p', { text: e.message })) : errorView(e));
      return;
    }
    count.textContent = plural(rooms.length, 'room');
    if (selected && !rooms.some((r) => r.name === selected)) selected = null;
    clear(body, layout);
    renderList();
    if (selected) show(selected);
  }

  function renderList() {
    layout.classList.toggle('with-detail', selected !== null);
    detail.hidden = selected === null;
    if (!rooms.length) {
      clear(roomList, h('div', { class: 'empty' }, h('h2', { text: 'No clean rooms' }),
        h('p', {}, app.isAdmin ? 'Create one in a worksheet: ' : 'You take part in none. An admin adds your role or user to one with ',
          h('code', { text: app.isAdmin ? "CREATE CLEAN ROOM tv COMMENT 'Agency and partner'" : 'ALTER CLEAN ROOM ... ADD COLLABORATOR' }))));
      return;
    }
    clear(roomList, table(['Room', 'Collaborators', 'You act for', 'Created'], rooms.map((r) => h('tr', {
      class: `clickable${r.name === selected ? ' selected' : ''}`, tabindex: '0',
      onclick: () => pick(r.name),
      onkeydown: (e) => { if (e.key === 'Enter') pick(r.name); },
    },
    h('td', {}, h('span', { class: 'mono', text: r.name }), r.comment ? h('div', { class: 'muted', text: r.comment }) : null),
    h('td', { class: 'mono muted', text: r.collaborators || '' }),
    h('td', { class: 'mono', text: r.yours || '' }),
    h('td', { class: 'nowrap muted', text: fmtTime(r.created_at) })))));
  }

  function pick(name) {
    selected = name;
    renderList();
    show(name);
  }

  async function show(name) {
    const room = api.ident(name);
    const panel = h('div', { class: 'tab-panel' }, h('div', { class: 'result-message', text: 'Loading…' }));
    const tabs = h('div', { class: 'tabs', role: 'tablist' });
    clear(detail,
      h('div', { class: 'row' }, h('h2', { text: name }), h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost icon-only small', type: 'button', 'aria-label': 'Close', onclick: () => { selected = null; renderList(); } }, icon('x'))),
      tabs, panel);
    let info;
    try {
      const [collaborators, datasets, templates, runs] = await api.queryAll(`SHOW COLLABORATORS IN CLEAN ROOM ${room};
SHOW DATASETS IN CLEAN ROOM ${room};
SHOW TEMPLATES IN CLEAN ROOM ${room};
SHOW RUNS IN CLEAN ROOM ${room};`);
      info = { collaborators: api.objects(collaborators), datasets: api.objects(datasets), templates: api.objects(templates), runs: api.objects(runs) };
    } catch (e) {
      clear(panel, errorView(e));
      return;
    }
    const mine = info.collaborators.filter((c) => yes(c.yours)).map((c) => c.name);
    const views = {
      templates: () => templatesView(name, info, mine),
      runs: () => runsView(name, info, mine),
      datasets: () => datasetsView(info),
      collaborators: () => collaboratorsView(info),
    };
    const names = [['templates', `Templates (${info.templates.length})`], ['runs', `Runs (${info.runs.length})`],
      ['datasets', `Datasets (${info.datasets.length})`], ['collaborators', `Collaborators (${info.collaborators.length})`]];
    const buttons = names.map(([key, label]) => h('button', {
      class: 'tab', type: 'button', role: 'tab', dataset: { key }, text: label,
      onclick: () => { tab = key; select(); },
    }));
    function select() {
      for (const b of buttons) b.setAttribute('aria-selected', String(b.dataset.key === tab));
      clear(panel, views[tab]());
    }
    tabs.append(...buttons);
    select();
  }

  function collaboratorsView(info) {
    return table(['Name', 'Kind', 'Who', 'Provider', 'You'], info.collaborators.map((c) => h('tr', {},
      h('td', { class: 'mono', text: c.name }), h('td', { text: c.kind }), h('td', { class: 'mono', text: c.principal }),
      h('td', { class: 'mono muted', text: c.provider || '' }), h('td', { text: yes(c.yours) ? 'yes' : '' }))));
  }

  function datasetsView(info) {
    if (!info.datasets.length) return h('p', { class: 'muted', text: 'No datasets yet.' });
    return table(['Dataset', 'Of', 'Identifiers (join only)', 'Dimensions', 'Aggregates', 'Threshold', 'Runs a day', 'Noise'],
      info.datasets.map((d) => h('tr', {},
        h('td', {}, h('span', { class: 'mono', text: d.name }), h('div', { class: 'muted mono', text: `${d.kind}: ${d.source}` })),
        h('td', { class: 'mono', text: d.collaborator }),
        h('td', { class: 'mono', text: d.join_on }),
        h('td', { class: 'mono', text: d.group_by || '' }),
        h('td', { class: 'mono' }, d.aggregates || '', d.bounds ? h('div', { class: 'muted', text: d.bounds }) : null),
        h('td', { class: 'num', text: d.threshold }),
        h('td', { class: 'num', text: d.runs_per_day }),
        h('td', { class: 'num', text: d.noise ? `ε ${d.noise}` : '' }))));
  }

  function templatesView(room, info, mine) {
    if (!info.templates.length) {
      return h('p', { class: 'muted' }, 'No templates yet. Add one in a worksheet, such as ',
        h('code', { text: 'ALTER CLEAN ROOM r ADD TEMPLATE overlap FROM BUILTIN audience_overlap (left => a.id, right => b.id) RUN BY (you)' }));
    }
    return h('div', { class: 'stack' }, info.templates.map((t) => {
      const needs = split(t.needs);
      const approved = split(t.approved);
      const toApprove = mine.filter((c) => needs.includes(c) && !approved.includes(c));
      const runners = mine.filter((c) => split(t.run_by).includes(c));
      const actions = [
        ...toApprove.map((c) => button(`Approve version ${t.version} as ${c}`, {
          icon: 'shield', kind: 'small',
          onclick: () => act(`ALTER CLEAN ROOM ${api.ident(room)} APPROVE TEMPLATE ${api.ident(t.name)} VERSION ${Number(t.version)} AS ${api.ident(c)}`,
            `Approved ${t.name} version ${t.version}`),
        })),
        ...runners.map((c) => button(runners.length > 1 ? `Run as ${c}` : 'Run', {
          icon: 'play', kind: 'small', disabled: !yes(t.runnable),
          title: yes(t.runnable) ? '' : 'Every collaborator whose data it reads must approve this version first',
          onclick: () => run(room, t, c),
        })),
      ];
      return h('div', { class: 'card card-body' },
        h('div', { class: 'row' }, h('strong', { class: 'mono', text: t.name }), h('span', { class: 'badge', text: `version ${t.version}` }),
          yes(t.runnable) ? h('span', { class: 'badge ok', text: 'approved' }) : h('span', { class: 'badge warn', text: 'awaiting approval' }),
          h('span', { class: 'spacer' }), actions),
        t.builtin ? h('p', { class: 'muted mono', text: `built-in: ${t.builtin}` }) : null,
        t.problem ? h('p', { class: 'form-error', text: t.problem }) : null,
        h('dl', {},
          h('dt', { text: 'Approvals' }), h('dd', {}, needs.map((c) => h('span', {
            class: `badge ${approved.includes(c) ? 'ok' : ''}`.trim(), text: `${c}${approved.includes(c) ? ' ✓' : ' pending'}`,
          }))),
          h('dt', { text: 'Run by' }), h('dd', { class: 'mono', text: t.run_by }),
          t.parameters ? [h('dt', { text: 'Parameters' }), h('dd', { class: 'mono', text: t.parameters })] : null),
        h('pre', { class: 'mono', text: t.definition }));
    }));
  }

  async function act(sql, success) {
    try {
      await api.query(sql);
      toast(success);
    } catch (e) {
      toast(e.message, 'error');
    }
    refresh();
  }

  async function run(room, t, as) {
    const params = split(t.parameters).map((p) => {
      const [name, ...type] = p.split(' ');
      return { name, type: type.join(' '), input: h('input', { 'aria-label': name, placeholder: type.join(' '), autocapitalize: 'none', spellcheck: 'false' }) };
    });
    if (params.length) {
      const ok = await dialog({
        title: `Run ${t.name}`,
        body: params.map((p) => h('div', { class: 'field' }, h('label', { text: `${p.name} (${p.type})` }), p.input)),
        actions: [{ label: 'Cancel', value: false }, { label: 'Run', value: true, primary: true, kind: 'primary' }],
        onopen: () => params[0].input.focus(),
      });
      if (!ok) return;
    }
    const args = params.length ? ` (${params.map((p) => `${api.ident(p.name)} => ${api.literal(p.input.value)}`).join(', ')})` : '';
    toast(`Running ${t.name}…`);
    try {
      const res = api.objects(await api.query(`RUN CLEAN ROOM ${api.ident(room)} TEMPLATE ${api.ident(t.name)}${args} AS ${api.ident(as)}`))[0];
      toast(`${t.name}: ${plural(Number(res.row_count), 'row')}`);
    } catch (e) {
      toast(e.message, 'error');
    }
    tab = 'runs';
    refresh();
  }

  function runsView(room, info, mine) {
    if (!info.runs.length) return h('p', { class: 'muted', text: 'No runs yet.' });
    const result = h('div', {});
    const rows = info.runs.map((r) => {
      const yours = mine.includes(r.requester) && r.result_table;
      return [
        h('tr', {},
          h('td', { class: 'num', text: r.id }),
          h('td', { class: 'nowrap muted', text: fmtTime(r.started_at) }),
          h('td', { class: 'mono', text: `${r.template} v${r.version}` }),
          h('td', { class: 'mono', text: `${r.requester} (${r.requested_by})` }),
          h('td', {}, statusBadge(r.status)),
          h('td', { class: 'num', text: r.row_count }),
          h('td', { class: 'mono muted', text: r.parameters === '{}' || r.parameters === 'null' ? '' : r.parameters }),
          h('td', {}, yours ? button('Latest result', { kind: 'small', onclick: () => showResult(result, r.result_table) }) : null)),
        r.error ? h('tr', {}, h('td', { colspan: '8' }, h('pre', { class: 'mono', text: r.error }))) : null,
      ];
    });
    return h('div', {},
      table(['Run', 'Started', 'Template', 'Requested by', 'Status', 'Rows', 'Parameters', ''], rows),
      h('p', { class: 'muted', text: 'Every collaborator of the room sees its runs; a result only its requester\'s party reads. Groups of too few people are left out of results without a trace.' }),
      result);
  }

  async function showResult(into, name) {
    const sql = `SELECT * FROM ${name.split('.').map(api.ident).join('.')}`;
    const grid = createGrid();
    clear(into, h('div', { class: 'row' }, h('strong', { class: 'mono', text: name }), h('span', { class: 'spacer' }),
      button('Open in a worksheet', { icon: 'open', onclick: () => app.openWorksheet(sql) })), grid.el);
    try {
      const res = await api.query(`${sql} LIMIT 10000`);
      grid.set(res.columns, res.rows);
    } catch (e) {
      clear(into, errorView(e));
    }
  }

  let loaded = false;
  return {
    el,
    shown() {
      if (!loaded) { loaded = true; refresh(); }
    },
  };
}
