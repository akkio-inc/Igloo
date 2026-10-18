import { defineCollection } from 'astro:content';
import { docsLoader, i18nLoader } from '@astrojs/starlight/loaders';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';

export const collections = {
	// Generated from the repository's markdown by scripts/sync-docs.mjs; see
	// src/docs-manifest.mjs for what goes in it.
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
	// Starlight's interface text, where ours differs (src/content/i18n/en.json).
	i18n: defineCollection({ loader: i18nLoader(), schema: i18nSchema() }),
};
