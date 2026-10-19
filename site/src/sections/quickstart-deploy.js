// Copy buttons on the quick start's code blocks. Runs inside .vx-quickstart only.
for (const root of document.querySelectorAll('.vx-quickstart')) {
  for (const btn of root.querySelectorAll('.vx-qs-copy')) {
    const label = btn.getAttribute('aria-label');
    btn.addEventListener('click', async () => {
      const code = btn.parentElement.querySelector('pre code');
      if (!code) return;
      try {
        await navigator.clipboard.writeText(code.textContent);
        btn.dataset.copied = '';
        btn.setAttribute('aria-label', 'Copied');
        setTimeout(() => { delete btn.dataset.copied; btn.setAttribute('aria-label', label); }, 1600);
      } catch {
        const range = document.createRange(); range.selectNodeContents(code);
        const sel = getSelection(); sel.removeAllRanges(); sel.addRange(range);
      }
    });
  }
}
