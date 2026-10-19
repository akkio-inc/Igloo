// Tabs for the "In SQL" examples: click or arrow keys select a tab; panels are toggled with `hidden`.
// Runs once per .vx-features section and only queries inside it.
for (const root of document.querySelectorAll('.vx-features')) {
	const tabs = [...root.querySelectorAll('[role="tab"]')];
	const panels = tabs.map((tab) => root.querySelector(`#${tab.getAttribute('aria-controls')}`));
	const select = (index, focus = false) => {
		tabs.forEach((tab, i) => {
			const selected = i === index;
			tab.setAttribute('aria-selected', String(selected));
			tab.tabIndex = selected ? 0 : -1;
			if (panels[i]) panels[i].hidden = !selected;
		});
		if (focus) tabs[index].focus();
	};
	tabs.forEach((tab, i) => {
		tab.addEventListener('click', () => select(i));
		tab.addEventListener('keydown', (event) => {
			const last = tabs.length - 1;
			const next = { ArrowRight: i + 1, ArrowLeft: i - 1, Home: 0, End: last }[event.key];
			if (next === undefined) return;
			event.preventDefault();
			select((next + tabs.length) % tabs.length, true);
		});
	});
}
