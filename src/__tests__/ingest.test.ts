import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ingestDynamicSources } from '../ingest/broker';

const originalFetch = globalThis.fetch;

const nowIso = new Date().toISOString();

beforeEach(() => {
	vi.useFakeTimers();
	vi.setSystemTime(new Date(nowIso));
});

afterEach(() => {
	vi.useRealTimers();
	if (originalFetch) {
		globalThis.fetch = originalFetch;
	}
	vi.restoreAllMocks();
});

describe('dynamic ingest broker', () => {
	it('normalises and deduplicates sources across adapters', async () => {
		const newsPayload = {
			articles: [
				{
					title: 'CheckoutFlow integrates multi-wallet checkout',
					url: 'https://news.example.com/story/a',
					publishedAt: nowIso,
					source: { name: 'News Example' },
					description: 'Integrated payments update',
				},
			],
		};
		const googleRss = `<?xml version="1.0"?><rss><channel><item><title>Policy board sets new pricing terms</title><link>https://google.example.com/policy</link><pubDate>${new Date().toUTCString()}</pubDate><description>Policy change</description></item></channel></rss>`;

		const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
			const url = typeof input === 'string' ? input : input.toString();
			if (url.includes('newsapi.org')) {
				return new Response(JSON.stringify(newsPayload), {
					status: 200,
					headers: { 'Content-Type': 'application/json' },
				});
			}
			if (url.includes('news.google.com')) {
				return new Response(googleRss, {
					status: 200,
					headers: { 'Content-Type': 'application/rss+xml' },
				});
			}
			throw new Error(`Unexpected fetch: ${url}`);
		});

		globalThis.fetch = fetchMock as typeof globalThis.fetch;

		const env = {
			SEEN: {} as KVNamespace,
			NEWSAPI_KEY: 'demo',
			GDELT_ENABLED: '0',
			GOOGLE_NEWS_ENABLED: '1',
			NEWSAPI_QUERIES: 'checkout',
			GOOGLE_NEWS_FEEDS: 'https://news.google.com/rss/search?q=policy&hl=en-IN&gl=IN&ceid=IN:en',
		};

		const items = await ingestDynamicSources(env, () => {});

		expect(fetchMock).toHaveBeenCalledTimes(2);
		expect(items.length).toBe(2);
		const urls = items.map((i) => i.url);
		expect(new Set(urls).size).toBe(2);
		expect(items.every((i) => typeof i.id === 'string' && i.id.length >= 6)).toBe(true);
		expect(items.find((i) => i.provider === 'google-rss')).toBeTruthy();
	});
});
