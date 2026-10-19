// Screens tabs: one tab per screenshot panel. Panels use `hidden`; tabs carry aria-selected and a roving tabindex.
// Left/Right arrows move between tabs (wrapping), Home/End jump to the ends. Everything is queried inside the root.
export function initScreens(root) {
	const tabs = [...root.querySelectorAll('[role="tab"]')];
	if (!tabs.length || root.dataset.vxTabs === 'ready') return;
	root.dataset.vxTabs = 'ready';
	const panels = tabs.map((tab) => root.querySelector(`#${CSS.escape(tab.getAttribute('aria-controls'))}`));
	const list = root.querySelector('[role="tablist"]');

	// Keep the selected tab inside the tab track when the track scrolls (narrow screens).
	const reveal = (tab) => {
		const left = tab.offsetLeft;
		const right = left + tab.offsetWidth;
		if (left < list.scrollLeft) list.scrollLeft = left;
		else if (right > list.scrollLeft + list.clientWidth) list.scrollLeft = right - list.clientWidth;
	};

	const select = (index, focus = false) => {
		tabs.forEach((tab, i) => {
			const on = i === index;
			tab.setAttribute('aria-selected', String(on));
			tab.tabIndex = on ? 0 : -1;
			if (panels[i]) panels[i].hidden = !on;
		});
		reveal(tabs[index]);
		if (focus) tabs[index].focus();
	};

	tabs.forEach((tab, i) => {
		tab.addEventListener('click', () => select(i));
		tab.addEventListener('keydown', (event) => {
			const last = tabs.length - 1;
			const to = { ArrowRight: i === last ? 0 : i + 1, ArrowLeft: i === 0 ? last : i - 1, Home: 0, End: last }[event.key];
			if (to === undefined) return;
			event.preventDefault();
			select(to, true);
		});
	});
	const current = tabs.findIndex((tab) => tab.getAttribute('aria-selected') === 'true');
	select(current < 0 ? 0 : current);
}

for (const root of document.querySelectorAll('.vx-screens')) initScreens(root);
