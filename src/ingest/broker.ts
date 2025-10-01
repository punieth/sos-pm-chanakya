import { NormalizedItem, SourceItem, SourceProvider } from '../types';
import { canonicalizeUrl, getDomain, hashId } from '../utils/url';
import { decodeHtmlEntities, sanitize } from '../utils/text';
import { nowUtc, subtractHours, toIsoString } from '../utils/time';
import { XMLParser } from 'fast-xml-parser';

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface BrokerEnv {
	NEWSAPI_KEY?: string;
	NEWSAPI_USER_AGENT?: string;
	GDELT_ENABLED?: string;
	GOOGLE_NEWS_ENABLED?: string;
}

const WINDOW_HOURS = 48;
const DEFAULT_LOGGER: Logger = () => {};

export async function ingestDynamicSources(env: BrokerEnv, logger: Logger = DEFAULT_LOGGER): Promise<NormalizedItem[]> {
	const since = subtractHours(nowUtc(), WINDOW_HOURS);
	const adapterPromises: Array<Promise<SourceItem[]>> = [];

	adapterPromises.push(newsApiAdapter(env, since, logger));

	if (env.GDELT_ENABLED !== '0') {
		adapterPromises.push(gdeltAdapter(env, since, logger));
	}
	if (env.GOOGLE_NEWS_ENABLED !== '0') {
		adapterPromises.push(googleNewsRssAdapter(env, since, logger));
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

	const normalized = collected
		.map((item) => finalise(item))
		.filter((item): item is NormalizedItem => Boolean(item));

	const deduped = dedupeByCanonicalUrl(normalized);

	logger('ingest_dynamic_complete', { total: deduped.length, raw: collected.length });
	return deduped;
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

function dedupeByCanonicalUrl(items: NormalizedItem[]): NormalizedItem[] {
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

function preferNewer(a: NormalizedItem, b: NormalizedItem): NormalizedItem {
	const aTime = Date.parse(a.publishedAt || '') || 0;
	const bTime = Date.parse(b.publishedAt || '') || 0;
	return bTime > aTime ? b : a;
}

async function newsApiAdapter(env: BrokerEnv, since: Date, logger: Logger): Promise<SourceItem[]> {
	const key = env.NEWSAPI_KEY;
	if (!key) {
		logger('ingest_dynamic_newsapi_skip', { reason: 'missing_key' });
		return [];
	}

	const url = new URL('https://newsapi.org/v2/everything');
	url.searchParams.set('language', 'en');
	url.searchParams.set('pageSize', '50');
	url.searchParams.set('from', toIsoString(since));
	url.searchParams.set('sortBy', 'publishedAt');
	url.searchParams.set('q', 'platform OR fintech OR payments OR marketplace OR "artificial intelligence"');

	const res = await fetch(url.toString(), {
		headers: {
			'X-Api-Key': key,
			'User-Agent': env.NEWSAPI_USER_AGENT || 'HotLaunchDiscovery/1.0 (+https://product.example)',
		},
	});

	if (!res.ok) {
		logger('ingest_dynamic_newsapi_error', { status: res.status, body: await safeSnippet(res) });
		return [];
	}

	const data: any = await res.json();
	const articles = Array.isArray(data?.articles) ? data.articles : [];
	logger('ingest_dynamic_newsapi_ok', { count: articles.length });

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

async function gdeltAdapter(_env: BrokerEnv, since: Date, logger: Logger): Promise<SourceItem[]> {
	const url = new URL('https://api.gdeltproject.org/api/v2/doc/doc');
	url.searchParams.set('format', 'json');
	url.searchParams.set('mode', 'ArtList');
	url.searchParams.set('sort', 'datedesc');
	url.searchParams.set('timespan', `${WINDOW_HOURS}HRS`);
	url.searchParams.set('maxrecords', '75');
	url.searchParams.set('query', buildGdeltQuery());

	const res = await fetch(url.toString(), {
		headers: {
			'User-Agent': 'HotLaunchDiscovery/1.0 (+https://product.example)',
			Accept: 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
		},
	});
	if (!res.ok) {
		logger('ingest_dynamic_gdelt_error', { status: res.status, body: await safeSnippet(res) });
		return [];
	}
	const raw = await res.text();
	let data: any;
	try {
		data = JSON.parse(raw);
	} catch (err) {
		logger('ingest_dynamic_gdelt_error', { status: res.status, body: raw.slice(0, 200), reason: String(err) });
		return [];
	}
	const articles = Array.isArray(data?.articles) ? data.articles : [];
	logger('ingest_dynamic_gdelt_ok', { count: articles.length });

	return articles
		.map((article: any) => mapGdeltArticle(article))
		.filter((x: SourceItem | null): x is SourceItem => Boolean(x));
}

function buildGdeltQuery(): string {
	// Focus on technology and business signals without brand keywords
	return '(technology OR business) AND (platform OR commerce OR fintech OR policy)';
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


async function googleNewsRssAdapter(_env: BrokerEnv, since: Date, logger: Logger): Promise<SourceItem[]> {
	const url = new URL('https://news.google.com/rss/search');
	const query = '(platform OR fintech OR payments OR marketplace OR "artificial intelligence" OR "policy" OR "model launch")';
	url.searchParams.set('q', `${query} when:48h`);
	url.searchParams.set('hl', 'en-US');
	url.searchParams.set('gl', 'US');
	url.searchParams.set('ceid', 'US:en');


	const res = await fetch(url.toString());
	if (!res.ok) {
		logger('ingest_dynamic_google_error', { status: res.status, body: await safeSnippet(res) });
		return [];
	}

	const xml = await res.text();
	const items = parseRss(xml);
	logger('ingest_dynamic_google_ok', { count: items.length });
	const sinceMs = since.getTime();
	return items
		.filter((item) => Date.parse(item.publishedAt) >= sinceMs)
		.map((item) => ({ ...item, provider: 'google-rss' as SourceProvider }));
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
