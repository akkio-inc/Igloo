// Plays each of the AI section's cards once it comes into view. Without this script, or with reduced motion, they show as they end.
const root = document.querySelector('.vx-quickstart.vx-ai');
if (root && 'IntersectionObserver' in window && !matchMedia('(prefers-reduced-motion: reduce)').matches) {
  root.classList.add('is-armed');
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) {
      if (!e.isIntersecting) continue;
      requestAnimationFrame(() => e.target.classList.add('is-on'));
      io.unobserve(e.target);
    }
  }, { threshold: 0.35 });
  for (const card of root.querySelectorAll('.vx-ai-card')) io.observe(card);
}
