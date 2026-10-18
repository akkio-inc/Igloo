// Which repository files become documentation pages, where each one lives on
// the site, and how the sidebar groups them. scripts/sync-docs.mjs reads this
// to copy and rewrite the markdown, and astro.config.mjs reads it to build the
// sidebar, so a page added here appears in both.
//
// `source` is relative to the repository root. `slug` defaults to the source
// path without its `.md` (docs/sql.md -> docs/sql, served at /docs/sql/).
// `title` defaults to the source's first `# Heading`. `label` is the sidebar
// text.
//
// Not included: docs/upstream/ (drafts of issues for upstream projects, not
// Igloo documentation). Links to it, and to any other repository file that is
// not a page here (code, scripts, compose files), go to GitHub.

export const REPO_URL = 'https://github.com/akkio-inc/Igloo';
export const REPO_BRANCH = 'main';

export const groups = [
	{
		label: 'Getting started',
		pages: [
			{ source: 'README.md', slug: 'docs', title: 'Overview', label: 'Overview' },
			{ source: 'docs/principles.md', label: 'Product principles' },
		],
	},
	{
		label: 'SQL',
		pages: [
			{ source: 'docs/sql.md', label: 'SQL and clients' },
			{ source: 'docs/media-analytics.md', label: 'Media analytics' },
		],
	},
	{
		label: 'Connecting',
		pages: [
			{ source: 'docs/integrations.md', label: 'Tools and integrations' },
			{ source: 'docs/console.md', label: 'Web console' },
			{ source: 'docs/flight-sql.md', label: 'Arrow Flight SQL' },
			{ source: 'docs/external-engines.md', label: 'Other engines' },
		],
	},
	{
		label: 'Security & governance',
		pages: [
			{ source: 'SECURITY.md', slug: 'docs/security', label: 'Security model' },
			{ source: 'docs/sso.md', label: 'Single sign-on and tokens' },
			{ source: 'docs/security-policies.md', label: 'Masking and row access' },
			{ source: 'docs/privacy.md', label: 'Erasing personal data' },
		],
	},
	{
		label: 'Data',
		pages: [
			{ source: 'docs/loading.md', label: 'Loading and scheduling' },
			{ source: 'docs/alerts.md', label: 'Alerts and notifications' },
			{ source: 'docs/catalog.md', label: 'Data catalog' },
			{ source: 'docs/sharing.md', label: 'Delta Sharing' },
			{ source: 'docs/federation.md', label: 'Connections' },
		],
	},
	{
		label: 'Operations',
		pages: [
			{ source: 'docs/operations.md', label: 'Operating Igloo' },
			{ source: 'docs/scale-to-zero.md', label: 'Scale to zero' },
			{ source: 'docs/workload-management.md', label: 'Workload management' },
			{ source: 'docs/distributed-execution.md', label: 'Distributed execution' },
		],
	},
	{
		label: 'Deploy',
		pages: [
			{ source: 'deploy/helm/igloo/README.md', slug: 'docs/deploy/helm', label: 'Kubernetes (Helm)' },
			{ source: 'deploy/aws/README.md', slug: 'docs/deploy/aws', label: 'AWS' },
			{ source: 'deploy/gcp/README.md', slug: 'docs/deploy/gcp', label: 'Google Cloud' },
			{ source: 'deploy/azure/README.md', slug: 'docs/deploy/azure', label: 'Azure' },
		],
	},
	{
		label: 'Reference',
		pages: [
			{ source: 'docs/configuration.md', label: 'Configuration' },
			{ source: 'bench/README.md', slug: 'docs/benchmark', label: 'Benchmark' },
			{ source: 'CHANGELOG.md', slug: 'docs/changelog', label: 'Changelog', toc: 2 },
			{ source: 'CONTRIBUTING.md', slug: 'docs/contributing', label: 'Contributing' },
		],
	},
	{
		label: 'Design notes',
		collapsed: true,
		pages: [{ source: 'docs/design/distributed-execution.md', label: 'Distributed execution' }],
	},
];

/** Every page, with its slug filled in. */
export const pages = groups.flatMap((group) =>
	group.pages.map((page) => ({ ...page, slug: page.slug ?? page.source.replace(/\.md$/, '') })),
);

/** The site URL of a page: docs/sql -> /docs/sql/. */
export const pageUrl = (page) => `/${page.slug}/`;

/** Starlight's sidebar configuration. */
export const sidebar = groups.map((group) => ({
	label: group.label,
	collapsed: group.collapsed ?? false,
	items: group.pages.map((page) => ({
		label: page.label,
		slug: page.slug ?? page.source.replace(/\.md$/, ''),
	})),
}));
