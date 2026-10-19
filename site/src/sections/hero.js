// Site header: below 900 px the nav links collapse behind the menu button.
// The button toggles aria-expanded; Escape, a click outside, following a link or widening the window closes it.
const header = document.querySelector('.vx-header');
const button = header?.querySelector('.vx-header__menu');
const nav = header?.querySelector('.vx-header__nav');

if (header && button && nav) {
	header.classList.add('vx-js');
	const isOpen = () => button.getAttribute('aria-expanded') === 'true';
	const setOpen = (open) => {
		button.setAttribute('aria-expanded', String(open));
		button.setAttribute('aria-label', open ? 'Close menu' : 'Menu');
		header.classList.toggle('is-open', open);
	};

	button.addEventListener('click', () => setOpen(!isOpen()));
	nav.addEventListener('click', (event) => {
		if (event.target instanceof Element && event.target.closest('a')) setOpen(false);
	});
	document.addEventListener('keydown', (event) => {
		if (event.key !== 'Escape' || !isOpen()) return;
		setOpen(false);
		button.focus();
	});
	document.addEventListener('click', (event) => {
		if (isOpen() && event.target instanceof Node && !header.contains(event.target)) setOpen(false);
	});
	const wide = matchMedia('(min-width: 900px)');
	wide.addEventListener('change', (event) => {
		if (event.matches) setOpen(false);
	});
}
