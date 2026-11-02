// Benchmark: draws the two charts (seconds and dollars per run, by warehouse size) from the section's table,
// at each chart's own width, with Igloo's median lead under each size and a readout of every value on hover. The
// table stays in the page for screen readers; without JavaScript it shows instead of the charts.
const section = document.querySelector('.vx-benchmark');
const table = section?.querySelector('.vx-bm-data');

const SERIES = [
	{ name: 'Igloo', color: 'var(--bm-igloo)', cls: 'igloo' },
	{ name: 'Snowflake', color: 'var(--bm-snowflake)', cls: 'snowflake' },
	{ name: 'Databricks', color: 'var(--bm-databricks)', cls: 'databricks' },
];
const times = (x) => `${x >= 10 ? Math.round(x) : x.toFixed(1)}×`;
// Igloo's lead at a size: the median over the other two, Snowflake and Databricks.
const median = (xs) => {
	const s = [...xs].sort((a, b) => a - b);
	return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2;
};
const lead = (r) => times(median([r[1] / r[0], r[2] / r[0]]));
const METRICS = {
	seconds: {
		max: 140,
		ticks: [0, 50, 100],
		tick: (v) => `${v}s`,
		value: (v) => `${v} s`,
		lead: (r) => [lead(r), 'faster'],
	},
	dollars: {
		max: 0.85,
		ticks: [0, 0.25, 0.5, 0.75],
		tick: (v) => `$${v.toFixed(2)}`,
		value: (v) => `$${v < 0.01 ? v.toFixed(4) : v.toFixed(3)}`,
		lead: (r) => [lead(r), 'lower cost'],
	},
};

if (section && table) {
	const rows = [...table.tBodies[0].rows].map((tr) => {
		const v = [...tr.cells].slice(1).map((td) => parseFloat(td.textContent));
		return { size: tr.cells[0].textContent.trim(), seconds: v.slice(0, 3), dollars: v.slice(3, 6) };
	});
	section.querySelectorAll('.vx-bm-chart').forEach((figure) => {
		const plot = figure.querySelector('.vx-bm-plot');
		const metric = METRICS[figure.dataset.metric];
		if (!plot || !metric) return;
		let width = 0;
		new ResizeObserver(() => {
			if (plot.clientWidth !== width) draw(plot, metric, figure.dataset.metric, rows, (width = plot.clientWidth));
		}).observe(plot);
	});
	section.classList.add('is-charted');
}

function draw(plot, metric, key, rows, W) {
	const H = Math.round(Math.min(340, Math.max(250, W * 0.6)));
	const labels = W >= 460;
	const m = { l: 46, r: labels ? 88 : 12, t: 14, b: 72 };
	const step = (W - m.l - m.r) / rows.length;
	const x = (i) => m.l + (i + 0.5) * step;
	const y = (v) => m.t + (1 - v / metric.max) * (H - m.t - m.b);
	const NS = 'http://www.w3.org/2000/svg';
	const el = (name, attrs, text) => {
		const e = document.createElementNS(NS, name);
		for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
		if (text !== undefined) e.textContent = text;
		return e;
	};

	const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, height: H, 'aria-hidden': 'true' });
	for (const t of metric.ticks) {
		svg.append(el('line', { class: 'vx-bm-grid', x1: m.l, x2: W - m.r, y1: y(t), y2: y(t) }));
		svg.append(el('text', { class: 'vx-bm-axis', x: m.l - 10, y: y(t) + 4, 'text-anchor': 'end' }, metric.tick(t)));
	}
	// Under each size, Igloo's lead there over the faster (or cheaper) of the other two.
	rows.forEach((r, i) => {
		svg.append(el('text', { class: 'vx-bm-size', x: x(i), y: H - m.b + 22, 'text-anchor': 'middle' }, r.size));
		const [n, word] = metric.lead(r[key]);
		svg.append(el('text', { class: 'vx-bm-lead-at', x: x(i), y: H - m.b + 44, 'text-anchor': 'middle' }, n));
		svg.append(el('text', { class: 'vx-bm-axis', x: x(i), y: H - m.b + 60, 'text-anchor': 'middle' }, word));
	});
	const cross = el('line', { class: 'vx-bm-cross', y1: m.t, y2: H - m.b });
	svg.append(cross);
	// Igloo last, on top; each line ends in a direct label, nudged apart where two end close together.
	const ends = [];
	[1, 2, 0].forEach((s) => {
		const pts = rows.map((r, i) => [x(i), y(r[key][s])]);
		svg.append(el('path', { class: `vx-bm-line vx-bm-line--${SERIES[s].cls}`, stroke: SERIES[s].color, d: 'M' + pts.map((p) => p.join(',')).join('L') }));
		for (const [px, py] of pts) svg.append(el('circle', { class: 'vx-bm-dot-mark', cx: px, cy: py, r: 4, fill: SERIES[s].color }));
		ends.push({ s, y: pts[pts.length - 1][1] });
	});
	if (labels) {
		ends.sort((a, b) => a.y - b.y);
		for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + 16);
		for (const e of ends) svg.append(el('text', { class: 'vx-bm-label', x: x(rows.length - 1) + 14, y: e.y + 4 }, SERIES[e.s].name));
	}

	// Hover: a crosshair at the size under the pointer and every value there.
	const tip = document.createElement('div');
	tip.className = 'vx-bm-tip';
	tip.hidden = true;
	rows.forEach((r, i) => {
		const hit = el('rect', { class: 'vx-bm-hit', x: m.l + i * step, y: m.t, width: step, height: H - m.t - m.b + 50 });
		hit.addEventListener('pointerenter', () => {
			cross.setAttribute('x1', x(i));
			cross.setAttribute('x2', x(i));
			cross.style.opacity = '1';
			const [n, word] = metric.lead(r[key]);
			tip.innerHTML =
				`<b>${r.size}</b>` +
				SERIES.map((s, j) => `<span><i style="background:${s.color}"></i>${s.name}<em>${metric.value(r[key][j])}</em></span>`).join('') +
				`<span>Igloo: ${n} ${word}</span>`;
			tip.hidden = false;
			const left = Math.min(Math.max(x(i) + 14, 0), W - tip.offsetWidth);
			tip.style.left = `${x(i) + 14 + tip.offsetWidth > W ? x(i) - 14 - tip.offsetWidth : left}px`;
			tip.style.top = `${m.t}px`;
		});
		hit.addEventListener('pointerleave', () => {
			cross.style.opacity = '0';
			tip.hidden = true;
		});
		svg.append(hit);
	});
	plot.replaceChildren(svg, tip);
}
