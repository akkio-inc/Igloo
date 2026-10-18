#!/usr/bin/env node
// Checks the built site in dist/ for broken links. Runs after `astro build`.
//
// - Every link, image, stylesheet and script on every page must resolve to a
//   file in dist/, and every #anchor to an id on the target page.
// - Links to this repository on GitHub (blob, tree and edit URLs on main),
//   which the docs use for code and scripts, must name a path that exists in
//   the repository. The repository is private until 1.0, so these are checked
//   against the checkout rather than fetched.
// - Other external links are not fetched.
// - Until SITE_PUBLIC=1, every page must carry the noindex meta tag and
//   robots.txt must disallow everything.

import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { REPO_BRANCH, REPO_URL } from '../src/docs-manifest.mjs';
import { SITE_PUBLIC } from '../src/site-config.mjs';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoDir = path.resolve(siteDir, '..');
const distDir = path.join(siteDir, 'dist');
const ORIGIN = 'http://site.invalid';

function walk(dir) {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		const full = path.join(dir, entry.name);
		return entry.isDirectory() ? walk(full) : [full];
	});
}

/** HTML with code, scripts and styles removed, so text in them is not read as markup. */
function markupOnly(html) {
	return html
		.replace(/<script\b[\s\S]*?<\/script>/gi, '')
		.replace(/<style\b[\s\S]*?<\/style>/gi, '')
		.replace(/<pre\b[\s\S]*?<\/pre>/gi, '<pre></pre>')
		.replace(/<code\b[\s\S]*?<\/code>/gi, '<code></code>');
}

const decode = (value) =>
	value
		.replace(/&amp;/g, '&')
		.replace(/&quot;/g, '"')
		.replace(/&#39;/g, "'")
		.replace(/&lt;/g, '<')
		.replace(/&gt;/g, '>');

if (!existsSync(distDir)) {
	console.error('check-links: dist/ does not exist; run astro build first');
	process.exit(1);
}

const htmlFiles = walk(distDir).filter((file) => file.endsWith('.html'));
const idsCache = new Map();

function idsOf(file) {
	if (!idsCache.has(file)) {
		const html = readFileSync(file, 'utf8');
		const ids = new Set();
		for (const match of html.matchAll(/\sid="([^"]*)"/g)) ids.add(decode(match[1]));
		idsCache.set(file, ids);
	}
	return idsCache.get(file);
}

/** The file in dist/ that serves a pathname, or undefined. */
function fileFor(pathname) {
	const relative = decodeURIComponent(pathname).replace(/^\/+/, '');
	const candidates = pathname.endsWith('/')
		? [path.join(distDir, relative, 'index.html')]
		: [path.join(distDir, relative), path.join(distDir, relative, 'index.html')];
	return candidates.find((file) => existsSync(file) && statSync(file).isFile());
}

const repoLink = new RegExp(`^${REPO_URL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/(blob|tree|edit)/${REPO_BRANCH}/([^#?]+)`);

const problems = [];
let checked = 0;

for (const file of htmlFiles) {
	const page = `/${path.relative(distDir, file).split(path.sep).join('/')}`.replace(/index\.html$/, '');
	const html = readFileSync(file, 'utf8');
	const markup = markupOnly(html);

	if (!SITE_PUBLIC && !/<meta name="robots" content="noindex/.test(html)) {
		problems.push(`${page}: no <meta name="robots" content="noindex"> (SITE_PUBLIC is not 1)`);
	}

	const urls = [];
	for (const match of markup.matchAll(/<(a|link|img|source|script|iframe)\b[^>]*>/gi)) {
		const tag = match[0];
		for (const attr of tag.matchAll(/\s(href|src|srcset)="([^"]*)"/gi)) {
			const value = decode(attr[2]);
			if (attr[1].toLowerCase() === 'srcset') {
				for (const candidate of value.split(',')) urls.push(candidate.trim().split(/\s+/)[0]);
			} else {
				urls.push(value);
			}
		}
	}

	for (const raw of urls) {
		if (!raw || /^(mailto|tel|data|javascript):/i.test(raw)) continue;
		checked++;
		const github = raw.match(repoLink);
		if (github) {
			const target = decodeURIComponent(github[2]).replace(/\/+$/, '');
			if (!existsSync(path.join(repoDir, target))) {
				problems.push(`${page}: ${raw}: ${target} is not in the repository`);
			}
			continue;
		}
		if (/^[a-z][a-z0-9+.-]*:/i.test(raw) || raw.startsWith('//')) continue;

		const url = new URL(raw, ORIGIN + page);
		const target = url.pathname === page ? file : fileFor(url.pathname);
		if (!target) {
			problems.push(`${page}: ${raw}: no such file in dist/`);
			continue;
		}
		if (url.hash && url.hash !== '#' && target.endsWith('.html')) {
			const id = decodeURIComponent(url.hash.slice(1));
			if (!idsOf(target).has(id)) problems.push(`${page}: ${raw}: no element with id "${id}"`);
		}
	}
}

const robots = existsSync(path.join(distDir, 'robots.txt')) ? readFileSync(path.join(distDir, 'robots.txt'), 'utf8') : '';
if (!SITE_PUBLIC && !/^Disallow: \/$/m.test(robots)) {
	problems.push('robots.txt: does not disallow everything (SITE_PUBLIC is not 1)');
}
if (SITE_PUBLIC && /^Disallow: \/$/m.test(robots)) {
	problems.push('robots.txt: disallows everything, but SITE_PUBLIC is 1');
}

if (problems.length > 0) {
	console.error(`check-links: ${problems.length} problem(s) in ${htmlFiles.length} pages:`);
	for (const problem of problems) console.error(`  ${problem}`);
	process.exit(1);
}
console.log(`check-links: ${checked} links in ${htmlFiles.length} pages, all good`);
