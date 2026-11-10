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

/**
 * Page views and clicks on links that leave the site are counted by Plausible,
 * which sets no cookie, under the site's own domain. Only a build for that
 * domain carries the script: a preview on another origin counts nothing.
 */
const ANALYTICS_DOMAIN = 'igloodb.com';
export const ANALYTICS =
	SITE_URL && new URL(SITE_URL).hostname === ANALYTICS_DOMAIN
		? { domain: ANALYTICS_DOMAIN, src: 'https://plausible.io/js/script.tagged-events.outbound-links.js' }
		: null;

/**
 * A path on the site as the absolute URL that link previews (Open Graph,
 * Twitter cards) need. Without SITE_URL it stays a path, which some previews
 * cannot follow: set SITE_URL when building a site that will be shared.
 */
export const siteUrl = (path) => (SITE_URL ? new URL(path, SITE_URL).href : path);

/**
 * What the site says about itself where it is shared: link previews, search
 * results and the app manifest. The image is public/og.jpg, 1200 x 630.
 */
export const SHARE = {
	title: 'Igloo: the open source replacement for Snowflake and Databricks',
	description:
		'3× faster and 25× lower cost than Snowflake and Databricks: an open source SQL warehouse on Apache Iceberg, in your own cloud.',
	image: '/og.jpg',
	imageAlt: 'Igloo, the open source replacement for Snowflake and Databricks: 3× faster, 25× lower cost.',
};

/**
 * Link-preview tags and app icons for the docs pages, which bring their own
 * title and description; the landing page sets the same tags itself.
 */
export const SHARE_HEAD = [
	{ tag: 'meta', attrs: { property: 'og:image', content: siteUrl(SHARE.image) } },
	{ tag: 'meta', attrs: { property: 'og:image:width', content: '1200' } },
	{ tag: 'meta', attrs: { property: 'og:image:height', content: '630' } },
	{ tag: 'meta', attrs: { property: 'og:image:alt', content: SHARE.imageAlt } },
	{ tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
	{ tag: 'meta', attrs: { name: 'twitter:image', content: siteUrl(SHARE.image) } },
	{ tag: 'meta', attrs: { name: 'twitter:image:alt', content: SHARE.imageAlt } },
	{ tag: 'link', attrs: { rel: 'apple-touch-icon', href: '/apple-touch-icon.png' } },
	{ tag: 'link', attrs: { rel: 'manifest', href: '/site.webmanifest' } },
];

/** The analytics script for the docs pages; the landing page sets the same tag itself. */
export const ANALYTICS_HEAD = ANALYTICS
	? [{ tag: 'script', attrs: { defer: true, 'data-domain': ANALYTICS.domain, src: ANALYTICS.src } }]
	: [];
