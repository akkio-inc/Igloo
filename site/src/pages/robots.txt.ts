import type { APIRoute } from 'astro';
import { SITE_PUBLIC } from '../site-config.mjs';

// Disallow everything until the site is public (SITE_PUBLIC=1).
export const GET: APIRoute = ({ site }) => {
	const lines = SITE_PUBLIC ? ['User-agent: *', 'Allow: /'] : ['User-agent: *', 'Disallow: /'];
	if (SITE_PUBLIC && site) lines.push('', `Sitemap: ${new URL('sitemap-index.xml', site)}`);
	return new Response(`${lines.join('\n')}\n`, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};
