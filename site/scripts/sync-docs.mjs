#!/usr/bin/env node
// Copies the repository's markdown into the site's content collection, so the
// documentation on the site is the documentation in the repository and cannot
// drift from it. Runs before every `astro build` and `astro dev`, and again in
// `astro dev` whenever a source file changes (see astro.config.mjs).
//
// For each page in src/docs-manifest.mjs it:
//   - takes the first `# Heading` as the page title (Starlight renders it) and
//     the first paragraph as the page description;
//   - rewrites links. A link to another page becomes that page's site URL, with
//     its #anchor; a link to an image is copied into public/files/; a link to
//     any other repository file or directory (code, scripts, compose files)
//     goes to GitHub on main. Links in raw HTML (<a href>, <img src>,
//     <source srcset>) are rewritten the same way;
//   - turns GitHub's <picture> with a prefers-color-scheme:dark <source> into
//     a light and a dark <img> that follow the site's theme switch;
//   - lets long code identifiers in table cells wrap at punctuation;
//   - fails if a relative link points at a file that does not exist.
//
// Edits are made at the source offsets the markdown parser reports, so the
// rest of each file is copied byte for byte.
//
// It also prepares the landing page's inputs in src/generated/: the
// screenshots, and the README's quick start and "How it works" sections. The
// hero's art is in src/assets/.
//
// Output goes to src/content/docs/, public/files/ and src/generated/, all
// ignored by git. Files are rewritten only when their content changes, and
// files no longer produced are removed.

import { existsSync, readdirSync, statSync } from 'node:fs';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { fromMarkdown } from 'mdast-util-from-markdown';
import { gfmFromMarkdown } from 'mdast-util-gfm';
import { gfm } from 'micromark-extension-gfm';
import sharp from 'sharp';
import { REPO_BRANCH, REPO_URL, pageUrl, pages } from '../src/docs-manifest.mjs';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoDir = path.resolve(siteDir, '..');
const contentDir = path.join(siteDir, 'src/content/docs');
const filesDir = path.join(siteDir, 'public/files');
const generatedDir = path.join(siteDir, 'src/generated');

const IMAGE_EXT = /\.(png|jpe?g|gif|webp|avif|svg)$/i;
// Raster images shown in pages are re-encoded as WebP; the PNG screenshots
// and banners are 0.3 to 2 MB each.
const WEBP_EXT = /\.(png|jpe?g)$/i;

const LANDING_SCREENSHOTS = [
	'console-worksheet',
	'console-worksheet-dark',
	'console-catalog',
	'console-catalog-dark',
	'metabase-dashboard',
	'notebook',
];

const pageBySource = new Map(pages.map((page) => [page.source, page]));
const posix = path.posix;

/** The repository files the site is built from, as absolute paths. */
export function sourceFiles() {
	const files = pages.map((page) => page.source);
	files.push('docs/assets/banner.jpg');
	files.push(...LANDING_SCREENSHOTS.map((name) => `docs/assets/screenshots/${name}.png`));
	return [...new Set(files)].map((file) => path.join(repoDir, file));
}

function parse(markdown) {
	return fromMarkdown(markdown, { extensions: [gfm()], mdastExtensions: [gfmFromMarkdown()] });
}

function githubUrl(repoPath, isDirectory, hash = '') {
	const encoded = repoPath.split('/').map(encodeURIComponent).join('/');
	return `${REPO_URL}/${isDirectory ? 'tree' : 'blob'}/${REPO_BRANCH}/${encoded}${hash}`;
}

/** True when `to` exists and is newer than `from`, so it need not be made again. */
function upToDate(from, to) {
	return existsSync(to) && statSync(to).mtimeMs >= statSync(from).mtimeMs;
}

function lineOf(source, offset) {
	return source.slice(0, offset).split('\n').length;
}

function getAttr(tag, name) {
	const match = tag.match(new RegExp(`\\s${name}\\s*=\\s*"([^"]*)"`, 'i'));
	return match ? match[1] : undefined;
}

function setAttr(tag, name, value) {
	const pattern = new RegExp(`(\\s${name}\\s*=\\s*)"[^"]*"`, 'i');
	if (pattern.test(tag)) return tag.replace(pattern, `$1"${value}"`);
	return tag.replace(/\s*(\/?)>$/, ` ${name}="${value}"$1>`);
}

/**
 * Inline code in a table cell as raw HTML with <wbr> after `.`, `/`, `_` and
 * `,`, so long identifiers (Helm keys, environment variables, URLs) can wrap
 * there instead of forcing the table wider than the page. The text is
 * unchanged. A `|` is written as an entity, since a raw one would end the cell.
 */
function tableCode(value) {
	const escaped = value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/\|/g, '&#124;');
	return `<code>${escaped.replace(/([./_,])(?=[^./_,])/g, '$1<wbr>')}</code>`;
}

/** Plain text of an mdast node. */
function toText(node) {
	if ('value' in node && node.type !== 'html') return node.value;
	return (node.children ?? []).map(toText).join('');
}

function truncate(text, max) {
	if (text.length <= max) return text;
	return `${text.slice(0, text.lastIndexOf(' ', max - 1))}…`;
}

/** One run of the sync: what it wrote and what it found wrong. */
class Sync {
	problems = [];
	/** Absolute paths of every file this run produced. */
	outputs = new Set();
	/** Repository path and mode -> { url, width, height } of each image copied. */
	images = new Map();
	changed = 0;

	async write(file, content) {
		this.outputs.add(file);
		if (existsSync(file) && (await readFile(file, 'utf8')) === content) return;
		await mkdir(path.dirname(file), { recursive: true });
		await writeFile(file, content);
		this.changed++;
	}

	/** Copies an image into public/files/ (as WebP when `embed`) and returns its URL and size. */
	async copyImage(repoPath, embed) {
		const key = `${repoPath}|${embed}`;
		if (this.images.has(key)) return this.images.get(key);
		const from = path.join(repoDir, repoPath);
		const published = embed ? repoPath.replace(WEBP_EXT, '.webp') : repoPath;
		const to = path.join(filesDir, published);
		this.outputs.add(to);
		if (!upToDate(from, to)) {
			await mkdir(path.dirname(to), { recursive: true });
			if (published === repoPath) await writeFile(to, await readFile(from));
			else await sharp(from).webp({ quality: 90, effort: 5 }).toFile(to);
			this.changed++;
		}
		const info = /\.svg$/i.test(published) ? undefined : await sharp(to).metadata();
		const result = {
			url: `/files/${published.split('/').map(encodeURIComponent).join('/')}`,
			width: info?.width,
			height: info?.height,
		};
		this.images.set(key, result);
		return result;
	}

	/**
	 * Resolves a link or image URL written in `fromSource` to its URL on the
	 * site. `embed` is true for images shown in the page (markdown images,
	 * <img>, <source>).
	 */
	async resolve(url, fromSource, embed, where) {
		const trimmed = url.trim();
		if (trimmed === '' || trimmed.startsWith('#')) return { url };
		// Absolute URLs (https:, mailto:) and protocol-relative ones stay as they are.
		if (/^[a-z][a-z0-9+.-]*:/i.test(trimmed) || trimmed.startsWith('//')) return { url };

		const [, rawPath, , hash = ''] = trimmed.match(/^([^?#]*)(\?[^#]*)?(#.*)?$/);
		let target = rawPath.startsWith('/')
			? posix.normalize(rawPath.slice(1))
			: posix.normalize(posix.join(posix.dirname(fromSource), rawPath));
		target = decodeURIComponent(target).replace(/\/+$/, '');
		if (target === '.') target = '';

		const page = pageBySource.get(target) ?? pageBySource.get(posix.join(target, 'README.md'));
		if (page) return { url: pageUrl(page) + hash };

		const absolute = path.join(repoDir, target);
		if (target.startsWith('..') || !existsSync(absolute)) {
			this.problems.push(`${where}: link to ${url}: ${target} does not exist in the repository`);
			return { url };
		}
		const isDirectory = statSync(absolute).isDirectory();
		if (!isDirectory && IMAGE_EXT.test(target)) {
			const image = await this.copyImage(target, embed);
			return { url: image.url + hash, width: image.width, height: image.height };
		}
		return { url: githubUrl(target, isDirectory, hash) };
	}

	/**
	 * Rewrites the URLs in a chunk of raw HTML, and maps <picture> to themed
	 * images. `eager` is for images at the top of a page.
	 */
	async rewriteHtml(html, fromSource, where, eager) {
		// GitHub shows the <source> whose media query matches the reader's OS
		// theme. The site has its own theme switch, so each variant becomes an
		// <img> that Starlight's light:/dark: utility classes show only in the
		// matching theme.
		html = html.replace(/<picture>\s*([\s\S]*?)\s*<\/picture>/gi, (whole, inner) => {
			const source = inner.match(/<source\b[^>]*>/i)?.[0];
			const img = inner.match(/<img\b[^>]*>/i)?.[0];
			if (!img) return whole;
			const media = source && getAttr(source, 'media');
			const darkSrc = media && /prefers-color-scheme:\s*dark/i.test(media) && getAttr(source, 'srcset');
			if (!darkSrc) return img;
			const light = setAttr(img, 'class', 'dark:sl-hidden');
			const dark = setAttr(setAttr(img, 'src', darkSrc), 'class', 'light:sl-hidden');
			return `${light}\n${dark}`;
		});

		let out = '';
		let last = 0;
		for (const match of html.matchAll(/<(a|img|source)\b[^>]*>/gi)) {
			let tag = match[0];
			const name = match[1].toLowerCase();
			if (name === 'a') {
				const href = getAttr(tag, 'href');
				if (href !== undefined) tag = setAttr(tag, 'href', (await this.resolve(href, fromSource, false, where)).url);
			} else if (name === 'img') {
				const src = getAttr(tag, 'src');
				if (src !== undefined) {
					const image = await this.resolve(src, fromSource, true, where);
					tag = setAttr(tag, 'src', image.url);
					// Real dimensions reserve the image's space before it loads; CSS
					// scales it to the column, as the source's width="100%" did.
					if (image.width && image.height) {
						tag = setAttr(setAttr(tag, 'width', String(image.width)), 'height', String(image.height));
					}
					if (!getAttr(tag, 'loading')) tag = setAttr(tag, 'loading', eager ? 'eager' : 'lazy');
					if (!getAttr(tag, 'decoding')) tag = setAttr(tag, 'decoding', 'async');
				}
			} else {
				const srcset = getAttr(tag, 'srcset');
				if (srcset !== undefined) {
					const candidates = [];
					for (const candidate of srcset.split(',')) {
						const [url, ...descriptor] = candidate.trim().split(/\s+/);
						candidates.push([(await this.resolve(url, fromSource, true, where)).url, ...descriptor].join(' '));
					}
					tag = setAttr(tag, 'srcset', candidates.join(', '));
				}
			}
			out += html.slice(last, match.index) + tag;
			last = match.index + match[0].length;
		}
		return out + html.slice(last);
	}

	/**
	 * Rewrites every link, image, link definition and raw HTML chunk in a
	 * markdown document, and optionally drops its first `# Heading`.
	 */
	async rewriteMarkdown(markdown, fromSource, { dropFirstHeading = false } = {}) {
		const tree = parse(markdown);
		const edits = [];
		let title;
		let description;

		if (dropFirstHeading) {
			const heading = tree.children.find((node) => node.type === 'heading' && node.depth === 1);
			if (heading) {
				title = toText(heading).trim();
				let end = heading.position.end.offset;
				while (markdown[end] === '\n') end++;
				edits.push({ start: heading.position.start.offset, end, text: '' });
				const index = tree.children.indexOf(heading);
				const paragraph = tree.children.slice(index + 1).find((node) => node.type === 'paragraph');
				if (paragraph) description = toText(paragraph).replace(/\s+/g, ' ').trim();
			}
		}

		const visit = async (node, inTable = false) => {
			const start = node.position?.start.offset;
			const end = node.position?.end.offset;
			const where = `${fromSource}:${node.position ? lineOf(markdown, start) : '?'}`;
			if (node.type === 'link' || node.type === 'image' || node.type === 'definition') {
				const slice = markdown.slice(start, end);
				// The destination follows the last "](" of an inline link, or the
				// "]:" of a definition. Autolinks (<https://...>) and bare URLs are
				// absolute and stay as they are.
				const marker = node.type === 'definition' ? slice.indexOf(']:') : slice.lastIndexOf('](');
				const resolved = await this.resolve(node.url, fromSource, node.type === 'image', where);
				if (resolved.url !== node.url) {
					const at = marker < 0 ? -1 : slice.indexOf(node.url, marker);
					if (at < 0) {
						this.problems.push(`${where}: could not find the URL ${node.url} in ${JSON.stringify(slice)}`);
					} else {
						edits.push({ start: start + at, end: start + at + node.url.length, text: resolved.url });
					}
				}
			} else if (node.type === 'html') {
				const eager = node === tree.children[0];
				const rewritten = await this.rewriteHtml(node.value, fromSource, where, eager);
				if (rewritten !== node.value) edits.push({ start, end, text: rewritten });
			} else if (node.type === 'inlineCode' && inTable && /[./_,]/.test(node.value)) {
				edits.push({ start, end, text: tableCode(node.value) });
			}
			for (const child of node.children ?? []) await visit(child, inTable || node.type === 'table');
		};
		await visit(tree);

		edits.sort((a, b) => b.start - a.start);
		let result = markdown;
		for (const edit of edits) result = result.slice(0, edit.start) + edit.text + result.slice(edit.end);
		return { body: result, title, description };
	}

	async page(page) {
		const markdown = await readFile(path.join(repoDir, page.source), 'utf8');
		const { body, title, description } = await this.rewriteMarkdown(markdown, page.source, {
			dropFirstHeading: true,
		});
		const frontmatter = {
			title: page.title ?? title ?? page.label,
			...(description ? { description: truncate(description, 200) } : {}),
			editUrl: `${REPO_URL}/edit/${REPO_BRANCH}/${page.source}`,
			...(page.toc ? { tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: page.toc } } : {}),
		};
		// JSON is valid YAML, and quotes every string safely.
		const yaml = Object.entries(frontmatter)
			.map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
			.join('\n');
		const file = path.join(contentDir, page.slug === 'docs' ? 'docs/index.md' : `${page.slug}.md`);
		const notice = `<!-- Generated from ${page.source} by site/scripts/sync-docs.mjs; edit that file instead. -->`;
		await this.write(file, `---\n${yaml}\n---\n\n${notice}\n\n${body}`);
	}

	/** The markdown of the README section under `## heading`, without the heading. */
	readmeSection(readme, heading) {
		const tree = parse(readme);
		const index = tree.children.findIndex(
			(node) => node.type === 'heading' && node.depth === 2 && toText(node).trim() === heading,
		);
		if (index < 0) {
			this.problems.push(`README.md: no "## ${heading}" section, which the landing page shows`);
			return '';
		}
		const next = tree.children.slice(index + 1).find((node) => node.type === 'heading' && node.depth <= 2);
		return readme.slice(tree.children[index].position.end.offset, next ? next.position.start.offset : undefined);
	}

	async landing() {
		for (const name of LANDING_SCREENSHOTS) {
			const from = path.join(repoDir, `docs/assets/screenshots/${name}.png`);
			const to = path.join(generatedDir, `screenshots/${name}.png`);
			this.outputs.add(to);
			if (upToDate(from, to)) continue;
			await mkdir(path.dirname(to), { recursive: true });
			await writeFile(to, await readFile(from));
			this.changed++;
		}

		const readme = await readFile(path.join(repoDir, 'README.md'), 'utf8');
		const quickStart = await this.rewriteMarkdown(this.readmeSection(readme, 'Quick start'), 'README.md');
		await this.write(path.join(generatedDir, 'quick-start.md'), `${quickStart.body.trim()}\n`);

		// "How it works" without its ASCII diagram, which the landing page draws.
		const howItWorks = this.readmeSection(readme, 'How it works').replace(/^```[^\n]*\n[\s\S]*?\n```\n/m, '');
		const rewritten = await this.rewriteMarkdown(howItWorks, 'README.md');
		await this.write(path.join(generatedDir, 'how-it-works.md'), `${rewritten.body.trim()}\n`);
	}

	/** Removes generated files that this run did not produce. */
	async prune() {
		const walk = (dir) =>
			existsSync(dir)
				? readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
						const full = path.join(dir, entry.name);
						return entry.isDirectory() ? walk(full) : [full];
					})
				: [];
		for (const file of [contentDir, filesDir, generatedDir].flatMap(walk)) {
			if (!this.outputs.has(file)) await rm(file);
		}
	}
}

/** Syncs everything. Returns the problems found; an empty list means success. */
export async function syncDocs() {
	const sync = new Sync();
	for (const page of pages) {
		if (!existsSync(path.join(repoDir, page.source))) {
			sync.problems.push(`src/docs-manifest.mjs: ${page.source} does not exist`);
			continue;
		}
		await sync.page(page);
	}
	await sync.landing();
	await sync.prune();
	return { problems: sync.problems, pages: pages.length, images: sync.images.size, changed: sync.changed };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	const result = await syncDocs();
	if (result.problems.length > 0) {
		console.error(`sync-docs: ${result.problems.length} problem(s) in the repository's markdown:`);
		for (const problem of result.problems) console.error(`  ${problem}`);
		process.exit(1);
	}
	console.log(`sync-docs: ${result.pages} pages and ${result.images} images from the repository`);
}
