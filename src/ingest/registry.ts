import { NormalizedItem, SourceItem } from '../types';
import { sanitize } from '../utils/text';
import { canonicalizeUrl, getDomain, hashId } from '../utils/url';
import { parseUtc } from '../utils/time';
import { DEFAULT_LOGGER, Logger, normalizeSourceItems } from './broker';

const REGISTRY_ENABLED_DEFAULT = true;
const MAX_ITEMS_PER_FEED = 4;
const MAX_AGE_DAYS = 90;
const MAX_AGE_MS = MAX_AGE_DAYS * 86400000;

interface RegistryFeed {
	url: string;
	source: string;
}

const REGISTRY_FEEDS: RegistryFeed[] = [
	{ url: 'https://razorpay.com/blog/rss.xml', source: 'Razorpay Blog' },
	{ url: 'https://www.trai.gov.in/taxonomy/term/19/feed', source: 'TRAI' },
	{ url: 'https://www.npci.org.in/whats-new/press-releases/rss', source: 'NPCI' },
	{ url: 'https://www.meity.gov.in/news-rss', source: 'MeitY' },
	{ url: 'https://www.rbi.org.in/Rss/PressReleases.xml', source: 'RBI Press' },
	{ url: 'https://paytm.com/blog/feed/', source: 'Paytm Blog' },
	{ url: 'https://razorpay.com/blog/tag/policies/feed/', source: 'Razorpay Policies' },
	{ url: 'https://www.moneycontrol.com/rss/technology.xml', source: 'Moneycontrol Tech' },
	{ url: 'https://blog.google/products/google-cloud/rss/', source: 'Google Cloud Blog' },
	{ url: 'https://developer.apple.com/news/rss/news.rss', source: 'Apple Developer' },
];

export interface RegistryEnv {
	REGISTRY_ENABLED?: string;
}

export async function ingestRegistryFeeds(
	env: RegistryEnv,
	logger: Logger = DEFAULT_LOGGER
): Promise<NormalizedItem[]> {
	if (!registryEnabled(env)) return [];
	const collected: SourceItem[] = [];
	for (const feed of REGISTRY_FEEDS) {
		try {
			const res = await fetch(feed.url, {
				headers: {
					'User-Agent': 'PMNewsAgent/1.0 (+registry)'
				},
			});
			if (!res.ok) {
				logger('ingest_registry_feed_error', { feed: feed.url, status: res.status });
				continue;
			}
			const xml = await res.text();
			const entries = parseFeed(xml).slice(0, MAX_ITEMS_PER_FEED);
			for (const entry of entries) {
				if (!entry.link) continue;
				const normalizedDate = normalizeDate(entry.pubDate);
				if (!normalizedDate) continue;
				if (Date.now() - normalizedDate.getTime() > MAX_AGE_MS) continue;
				const publishedAt = normalizedDate.toISOString();
				collected.push({
					title: sanitize(entry.title || ''),
					url: entry.link,
					publishedAt,
					source: feed.source,
					description: sanitize(entry.description || ''),
					provider: 'registry',
				});
			}
		} catch (err) {
			logger('ingest_registry_feed_exception', { feed: feed.url, error: String(err) });
		}
	}

	const normalized = normalizeSourceItems(collected);

	logger('ingest_registry_complete', { total: normalized.length, raw: collected.length });
	return normalized;
}

function registryEnabled(env: RegistryEnv): boolean {
	if (env.REGISTRY_ENABLED === undefined) return REGISTRY_ENABLED_DEFAULT;
	return !['0', 'false', 'no', 'off'].includes(env.REGISTRY_ENABLED.toLowerCase());
}

interface FeedEntry {
	title?: string;
	link?: string;
	pubDate?: string;
	description?: string;
}

function parseFeed(xml: string): FeedEntry[] {
	const entries: FeedEntry[] = [];
	const rssBlocks = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
	for (const block of rssBlocks) entries.push(parseRssItem(block));
	const atomBlocks = xml.match(/<entry[\s\S]*?<\/entry>/gi) || [];
	for (const block of atomBlocks) entries.push(parseAtomEntry(block));
	return entries.filter((entry) => entry.link && entry.title);
}

function parseRssItem(block: string): FeedEntry {
	return {
		title: clean(pick(block, /<title[^>]*>([\s\S]*?)<\/title>/i)),
		link:
			clean(pick(block, /<link[^>]*>([\s\S]*?)<\/link>/i)) ||
			clean(pick(block, /<link[^>]*href="([^"]+)"/i)),
		pubDate:
			clean(pick(block, /<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i)) ||
			clean(pick(block, /<dc:date[^>]*>([\s\S]*?)<\/dc:date>/i)),
		description: clean(pick(block, /<description[^>]*>([\s\S]*?)<\/description>/i)),
	};
}

function parseAtomEntry(block: string): FeedEntry {
	return {
		title: clean(pick(block, /<title[^>]*>([\s\S]*?)<\/title>/i)),
		link:
			clean(pick(block, /<link[^>]*rel="alternate"[^>]*href="([^"]+)"/i)) ||
			clean(pick(block, /<link[^>]*href="([^"]+)"/i)),
		pubDate:
			clean(pick(block, /<updated[^>]*>([\s\S]*?)<\/updated>/i)) ||
			clean(pick(block, /<published[^>]*>([\s\S]*?)<\/published>/i)),
		description: clean(pick(block, /<summary[^>]*>([\s\S]*?)<\/summary>/i)),
	};
}

function pick(block: string, re: RegExp): string {
	const match = block.match(re);
	return match ? match[1] : '';
}

function clean(value: string): string {
	if (!value) return '';
	return value.replace(/<!\[CDATA\[/g, '').replace(/\]\]>/g, '').trim();
}

function normalizeDate(input?: string): Date | undefined {
	if (!input) return undefined;
	const parsed = parseUtc(input);
	return parsed || undefined;
}
