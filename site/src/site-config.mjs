// Build-time settings, read from the environment.

/**
 * SITE_PUBLIC=1 makes the site indexable. Until launch it is unset, and every
 * page carries <meta name="robots" content="noindex">, robots.txt disallows
 * everything, and Cloudflare Pages sends `X-Robots-Tag: noindex` (dist/_headers).
 */
export const SITE_PUBLIC = process.env.SITE_PUBLIC === '1';

/**
 * The site's public origin, such as https://igloo.example.com. Optional: when
 * set, pages get canonical URLs and a sitemap is generated.
 */
export const SITE_URL = process.env.SITE_URL || undefined;

export const NOINDEX_META = { tag: 'meta', attrs: { name: 'robots', content: 'noindex, nofollow' } };
