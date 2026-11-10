// @ts-check
import { writeFile } from 'node:fs/promises';
import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import { sourceFiles, syncDocs } from './scripts/sync-docs.mjs';
import { REPO_URL, sidebar } from './src/docs-manifest.mjs';
import { NOINDEX_META, SHARE, SHARE_HEAD, SITE_PUBLIC, SITE_URL } from './src/site-config.mjs';

/**
 * In `astro dev`, sync again when a repository file the site is built from
 * changes, so editing docs/sql.md updates the page in the browser.
 * @returns {import('astro').AstroIntegration}
 */
const resyncDocs = () => ({
	name: 'igloo-resync-docs',
	hooks: {
		'astro:server:setup': ({ server, logger }) => {
			const files = sourceFiles();
			server.watcher.add(files);
			server.watcher.on('change', async (file) => {
				if (!files.includes(file)) return;
				const { problems, changed } = await syncDocs();
				for (const problem of problems) logger.warn(problem);
				if (changed) logger.info(`synced ${changed} file(s) after a change to ${file}`);
			});
		},
	},
});

/**
 * Until the site is public, ask Cloudflare Pages to send X-Robots-Tag on every
 * response too, which covers files that have no <meta> (images, robots.txt).
 * @returns {import('astro').AstroIntegration}
 */
const noindexHeaders = () => ({
	name: 'igloo-noindex-headers',
	hooks: {
		'astro:build:done': async ({ dir }) => {
			if (SITE_PUBLIC) return;
			await writeFile(new URL('_headers', dir), '/*\n  X-Robots-Tag: noindex, nofollow\n');
		},
	},
});

export default defineConfig({
	site: SITE_URL,
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'Igloo',
			description: SHARE.description,
			logo: { light: './src/assets/logo-black.svg', dark: './src/assets/logo.svg' },
			favicon: '/favicon.svg',
			social: [{ icon: 'github', label: 'GitHub', href: REPO_URL }],
			sidebar,
			customCss: ['@fontsource-variable/inter/opsz.css', './src/styles/theme.css'],
			head: SITE_PUBLIC ? SHARE_HEAD : [...SHARE_HEAD, NOINDEX_META],
			tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
			// src/pages/404.astro instead.
			disable404Route: true,
			expressiveCode: {
				styleOverrides: { borderRadius: '0.75rem' },
			},
		}),
		noindexHeaders(),
		resyncDocs(),
	],
});
