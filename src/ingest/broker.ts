import { NormalizedItem, SourceItem, SourceProvider } from '../types';
import { canonicalizeUrl, getDomain, hashId } from '../utils/url';
import { decodeHtmlEntities, sanitize } from '../utils/text';
import { nowUtc, subtractHours, toIsoString } from '../utils/time';
import { XMLParser } from 'fast-xml-parser';

export type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface BrokerEnv {
	NEWSAPI_KEY?: string;
	NEWSAPI_USER_AGENT?: string;
	NEWSAPI_QUERIES?: string;
	NEWSAPI_PAGE_SIZE?: string;
	GDELT_ENABLED?: string;
	GDELT_QUERIES?: string;
	GOOGLE_NEWS_ENABLED?: string;
	GOOGLE_NEWS_FEEDS?: string;
}

const WINDOW_HOURS = 48;
export const DEFAULT_LOGGER: Logger = () => {};
const DEFAULT_NEWSAPI_QUERIES = [
	'platform OR fintech OR payments OR marketplace OR "artificial intelligence"',
	'("India" OR Bharat) AND (fintech OR payments OR NPCI OR UPI OR RBI OR "Unified Payments Interface")',
	'("ONDC" OR "Open Network for Digital Commerce")',
	'("policy" OR regulation OR regulator OR compliance) AND (digital OR fintech OR payments OR pricing)'
];
const DEFAULT_GDELT_QUERIES = [
	'(technology OR business) AND (platform OR commerce OR fintech OR policy)',
	'(India OR Indian OR Bharat) AND (fintech OR policy OR payments OR platform OR regulator OR ONDC OR NPCI OR RBI)'
];
const DEFAULT_GOOGLE_NEWS_FEEDS = [
	'https://news.google.com/rss/search?q=(%22India%22%20AND%20(fintech%20OR%20payments%20OR%20NPCI%20OR%20UPI%20OR%20RBI))%20when:48h&hl=en-IN&gl=IN&ceid=IN:en',
	'https://news.google.com/rss/search?q=(ONDC%20OR%20%22Open%20Network%20for%20Digital%20Commerce%22)%20when:48h&hl=en-IN&gl=IN&ceid=IN:en',
	'https://news.google.com/rss/search?q=(%22India%22%20AND%20(policy%20OR%20regulation%20OR%20regulator)%20AND%20(digital%20OR%20fintech%20OR%20payments))%20when:48h&hl=en-IN&gl=IN&ceid=IN:en'
];
const DEFAULT_NEWSAPI_PAGE_SIZE = 30;

export async function ingestDynamicSources(env: BrokerEnv, logger: Logger = DEFAULT_LOGGER): Promise<NormalizedItem[]> {
	const since = subtractHours(nowUtc(), WINDOW_HOURS);
	const adapterPromises: Array<Promise<SourceItem[]>> = [];

	const newsQueries = getNewsApiQueries(env);
	for (const query of newsQueries) {
		adapterPromises.push(newsApiAdapter(env, since, logger, query));
	}

	if (env.GDELT_ENABLED !== '0') {
		const gdeltQueries = getGdeltQueries(env);
		for (const query of gdeltQueries) {
			adapterPromises.push(gdeltAdapter(env, since, logger, query));
		}
	}

	if (env.GOOGLE_NEWS_ENABLED !== '0') {
		const feeds = getGoogleFeeds(env);
		for (const feed of feeds) {
			adapterPromises.push(googleNewsRssAdapter(env, since, logger, feed));
		}
	}

	const settled = await Promise.allSettled(adapterPromises);
	const collected: SourceItem[] = [];

	for (const res of settled) {
		if (res.status === 'fulfilled') {
			collected.push(...res.value);
		} else {
			logger('ingest_dynamic_adapter_error', { error: res.reason ? String(res.reason) : 'unknown' });
		}
	}

	const normalized = normalizeSourceItems(collected);
	logger('ingest_dynamic_complete', { total: normalized.length, raw: collected.length });
	return normalized;
}

export function normalizeSourceItems(collected: SourceItem[]): NormalizedItem[] {
	return dedupeByCanonicalUrl(
		collected
			.map((item) => finalise(item))
			.filter((item): item is NormalizedItem => Boolean(item))
	);
}

function finalise(item: SourceItem): NormalizedItem | null {
	const title = sanitize(item.title);
	const url = canonicalizeUrl(item.url);
	if (!title || !url) return null;
	const domain = getDomain(url);
	const id = hashId(url, item.provider, item.publishedAt || title);
	return {
		id,
		provider: item.provider,
		title,
		description: sanitize(item.description),
		publishedAt: item.publishedAt,
		source: item.source,
		url,
		canonicalUrl: item.canonicalUrl || url,
		domain,
		language: item.language,
		authors: item.authors,
		country: item.country,
	};
}

export function dedupeByCanonicalUrl(items: NormalizedItem[]): NormalizedItem[] {
	const seen = new Map<string, NormalizedItem>();
	for (const item of items) {
		const key = item.canonicalUrl || item.url;
		const existing = seen.get(key);
		if (!existing) {
			seen.set(key, item);
			continue;
		}
		seen.set(key, preferNewer(existing, item));
	}
	return Array.from(seen.values());
}

export function preferNewer(a: NormalizedItem, b: NormalizedItem): NormalizedItem {
	const aTime = Date.parse(a.publishedAt || '') || 0;
	const bTime = Date.parse(b.publishedAt || '') || 0;
	return bTime > aTime ? b : a;
}

async function newsApiAdapter(env: BrokerEnv, since: Date, logger: Logger, query: string): Promise<SourceItem[]> {
	const key = env.NEWSAPI_KEY;
	if (!key) {
		logger('ingest_dynamic_newsapi_skip', { reason: 'missing_key', query });
		return [];
	}

	const url = new URL('https://newsapi.org/v2/everything');
	url.searchParams.set('language', 'en');
	url.searchParams.set('pageSize', String(clampPageSize(env.NEWSAPI_PAGE_SIZE)));
	url.searchParams.set('from', toIsoString(since));
	url.searchParams.set('sortBy', 'publishedAt');
	url.searchParams.set('q', query);

	const res = await fetch(url.toString(), {
		headers: {
			'X-Api-Key': key,
			'User-Agent': env.NEWSAPI_USER_AGENT || 'PMNewsAgent/1.0 (+newsapi)',
		},
	});

	if (!res.ok) {
		logger('ingest_dynamic_newsapi_error', { status: res.status, body: await safeSnippet(res), query });
		return [];
	}

	const data: any = await res.json();
	const articles = Array.isArray(data?.articles) ? data.articles : [];
	logger('ingest_dynamic_newsapi_ok', { query, count: articles.length });

	return articles
		.map((article: any) => mapNewsApiArticle(article))
		.filter((x: SourceItem | null): x is SourceItem => Boolean(x));
}

function mapNewsApiArticle(article: any): SourceItem | null {
	const title = sanitize(article?.title);
	const url = article?.url;
	const publishedAt = article?.publishedAt || article?.published_at;
	if (!title || !url || !publishedAt) return null;
	return {
		title,
		url,
		publishedAt,
		source: sanitize(article?.source?.name || 'NewsAPI'),
		description: sanitize(article?.description || article?.content),
		language: article?.language || 'en',
		provider: 'newsapi',
		authors: article?.author ? [article.author] : undefined,
 	};
}

async function gdeltAdapter(_env: BrokerEnv, since: Date, logger: Logger, query: string): Promise<SourceItem[]> {
	const url = new URL('https://api.gdeltproject.org/api/v2/doc/doc');
	url.searchParams.set('format', 'json');
	url.searchParams.set('mode', 'ArtList');
	url.searchParams.set('sort', 'datedesc');
	url.searchParams.set('timespan', `${WINDOW_HOURS}HRS`);
	url.searchParams.set('maxrecords', '75');
	url.searchParams.set('query', query);

	const res = await fetch(url.toString(), {
		headers: {
			'User-Agent': 'HotLaunchDiscovery/1.0 (+https://product.example)',
			Accept: 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
		},
	});
	if (!res.ok) {
		logger('ingest_dynamic_gdelt_error', { status: res.status, body: await safeSnippet(res), query });
		return [];
	}
	const raw = await res.text();
	let data: any;
	try {
		data = JSON.parse(raw);
	} catch (err) {
		logger('ingest_dynamic_gdelt_error', { status: res.status, body: raw.slice(0, 200), reason: String(err), query });
		return [];
	}
	const articles = Array.isArray(data?.articles) ? data.articles : [];
	logger('ingest_dynamic_gdelt_ok', { query, count: articles.length });

	return articles
		.map((article: any) => mapGdeltArticle(article))
		.filter((x: SourceItem | null): x is SourceItem => Boolean(x));
}

function mapGdeltArticle(article: any): SourceItem | null {
	const title = sanitize(article?.title);
	const url = article?.url || article?.sourceurl;
	const publishedAt = article?.seendate || article?.publishedAt;
	if (!title || !url || !publishedAt) return null;
	return {
		title,
		url,
		publishedAt,
		source: sanitize(article?.source || article?.domain || 'GDELT'),
		description: sanitize(article?.excerpt || article?.summary || article?.content),
		language: article?.language || 'en',
		country: article?.sourcecountry,
		provider: 'gdelt',
	};
}


async function googleNewsRssAdapter(_env: BrokerEnv, since: Date, logger: Logger, feedUrl: string): Promise<SourceItem[]> {
	try {
		const res = await fetch(feedUrl);
		if (!res.ok) {
			logger('ingest_dynamic_google_error', { status: res.status, body: await safeSnippet(res), feed: feedUrl });
			return [];
		}
		const xml = await res.text();
		const items = parseRss(xml);
		logger('ingest_dynamic_google_ok', { feed: feedUrl, count: items.length });
		const sinceMs = since.getTime();
		return items
			.filter((item) => Date.parse(item.publishedAt) >= sinceMs)
			.map((item) => ({ ...item, provider: 'google-rss' as SourceProvider }));
	} catch (err) {
		logger('ingest_dynamic_google_exception', { feed: feedUrl, error: String(err) });
		return [];
	}
}

const xmlParser = new XMLParser({ ignoreAttributes: false, trimValues: true });

function parseRss(xml: string): SourceItem[] {
	let parsed: any;
	try {
		parsed = xmlParser.parse(xml);
	} catch (err) {
		return [];
	}
	const channel = parsed?.rss?.channel || parsed?.channel;
	if (!channel) return [];
	const rawItems = channel.item || channel.items || [];
	const list = Array.isArray(rawItems) ? rawItems : [rawItems];
	const items: SourceItem[] = [];
	for (const rawItem of list) {
		if (!rawItem) continue;
		const title = sanitize(rawItem.title);
		const link = typeof rawItem.link === 'object' ? rawItem.link?.['@_href'] || rawItem.link?.['#text'] : rawItem.link;
		const pubDate = sanitize(rawItem.pubDate || rawItem.published || '');
		if (!title || !link || !pubDate) continue;
		const publishedAt = new Date(pubDate);
		if (Number.isNaN(publishedAt.getTime())) continue;
		const source = rawItem.source && typeof rawItem.source === 'object' ? rawItem.source['#text'] || rawItem.source['@_url'] : rawItem.source;
		const description = rawItem.description && typeof rawItem.description === 'object' ? rawItem.description['#text'] : rawItem.description;
		items.push({
			title: decodeHtmlEntities(title),
			url: decodeHtmlEntities(String(link)),
			publishedAt: publishedAt.toISOString(),
			source: decodeHtmlEntities(sanitize(source || 'Google News')),
			description: decodeHtmlEntities(sanitize(description || '')),
			provider: 'google-rss',
		});
	}
	return items;
}

async function safeSnippet(res: Response): Promise<string> {
	try {
		return (await res.text()).slice(0, 180);
	} catch {
		return 'unavailable';
	}
}

export const adapters = {
	newsApiAdapter,
	gdeltAdapter,
	googleNewsRssAdapter,
};

function getNewsApiQueries(env: BrokerEnv): string[] {
	const list = parseList(env.NEWSAPI_QUERIES) || DEFAULT_NEWSAPI_QUERIES;
	return list.slice(0, 6);
}

function getGdeltQueries(env: BrokerEnv): string[] {
	const list = parseList(env.GDELT_QUERIES) || DEFAULT_GDELT_QUERIES;
	return list.slice(0, 4);
}

function getGoogleFeeds(env: BrokerEnv): string[] {
	const list = parseList(env.GOOGLE_NEWS_FEEDS) || DEFAULT_GOOGLE_NEWS_FEEDS;
	return list.slice(0, 6);
}

function parseList(value?: string | null): string[] | null {
	if (!value) return null;
	const items = value
		.split(/[|;]/)
		.map((entry) => entry.trim())
		.filter(Boolean);
	return items.length ? items : null;
}

function clampPageSize(value?: string): number {
	const parsed = value ? parseInt(value, 10) : NaN;
	if (Number.isFinite(parsed) && parsed >= 10 && parsed <= 100) {
		return parsed;
	}
	return DEFAULT_NEWSAPI_PAGE_SIZE;
}
