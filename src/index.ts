export default {
	async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(run(env));
	},

	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === '/run') {
			const details = await run(env);
			return json(details);
		}

		if (url.pathname === '/score-new') {
			try {
				const limit = clamp(parseInt((env as any).SCORE_BATCH_SIZE || '15', 10), 1, 50);
				const pages = await notionFetchTodayBatch(env, limit);
				if (!pages.length) return json({ ok: true, message: "No rows from today's batch." });

				// Per-type caps so Coverage can't dominate
				const perTypeCap: Record<string, number> = { Platform: 20, Rails: 12, Marketplace: 8, Coverage: 4, 'Long-form': 4 };
				const counts: Record<string, number> = {};

				const results: Array<{ id: string; ok: boolean; error?: string }> = [];
				for (const p of pages) {
					const t = p.type || 'Coverage';
					if ((counts[t] || 0) >= (perTypeCap[t] || 0)) continue;
					try {
						const ctx = await getContextFor(env, p); // NEW
						// optional: store excerpt back to Notion
						try {
							await fetch(`https://api.notion.com/v1/pages/${p.id}`, {
								method: 'PATCH',
								headers: notionHeaders(env),
								body: JSON.stringify({
									properties: {
										'Source Excerpt': { rich_text: [{ text: { content: ctx.excerpt } }] },
										'Content Fetched': { checkbox: true },
									},
								}),
							});
						} catch (e) {
							console.warn('Failed to update excerpt for', p.id, e);
						}
						const shaped = shapeContentForLLM(p, env); // CHANGED
						const llmJson = await callLLM(env, shaped.prompt);
						const parsed = safeParseLLM(llmJson);
						const mapped = mapLLMToNotion(parsed, p);
						await notionUpdateScoring(env, p.id, mapped);
						// after: await notionUpdateScoring(env, p.id, mapped);

						try {
							const parsed = safeParseLLM(llmJson);
							const mapped = mapLLMToNotion(parsed, p);
							await notionUpdateScoring(env, p.id, mapped);

							// Compose only for strong signals
							if (mapped.signalScore >= 6 && (mapped.status === 'Keep' || mapped.status === 'Researching')) {
								const draft = composeLinkedInDraft(p, parsed);
								await fetch(`https://api.notion.com/v1/pages/${p.id}`, {
									method: 'PATCH',
									headers: notionHeaders(env),
									body: JSON.stringify({
										properties: {
											'Draft Title': { rich_text: [{ text: { content: draft.title.slice(0, 200) } }] },
											'Draft Body': { rich_text: [{ text: { content: draft.body.slice(0, 4000) } }] },
											'Draft Status': { select: { name: 'Proposed' } },
										},
									}),
								});
							}

							results.push({ id: p.id, ok: true });
						} catch (e: any) {
							results.push({ id: p.id, ok: false, error: String(e?.message || e) });
						}
						counts[t] = (counts[t] || 0) + 1;
						results.push({ id: p.id, ok: true });
					} catch (e: any) {
						results.push({ id: p.id, ok: false, error: String(e?.message || e) });
					}
					if (results.filter((r) => r.ok).length >= limit) break; // batch limit
				}
				return json({ ok: true, updated: results.filter((r) => r.ok).length, results, caps: counts });
			} catch (e: any) {
				return json({ ok: false, error: String(e?.message || e) }, 500);
			}
		}

		if (url.pathname === '/admin/kv/clear-all') {
			const deleted = await clearAllKV(env.SEEN);
			return json({ ok: true, deleted });
		}

		if (url.pathname === '/health') return new Response('OK');

		if (url.pathname === '/debug/gemini-models') {
			const key = (env as any).GEMINI_API_KEY;
			if (!key) return json({ ok: false, error: 'Missing GEMINI_API_KEY' }, 400);
			const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${key}`);
			const data = await res.json();
			return json({
				ok: true,
				cached: await env.SEEN.get('GEMINI_MODEL'),
				forced: (env as any).GEMINI_MODEL_FORCE || null,
				available: (data.models || []).map((m: any) => m.name?.replace(/^models\//, '')),
			});
		}

		if (url.pathname === '/compose') {
			try {
				const limit = clamp(parseInt((env as any).COMPOSE_BATCH_SIZE || '6', 10), 1, 20);
				const pages = await notionFetchForCompose(env, limit);
				if (!pages.length) return json({ ok: true, message: 'No rows qualify for compose.' });

				const results: Array<{ id: string; ok: boolean; error?: string }> = [];
				for (const p of pages) {
					try {
						const ctx = await getContextFor(env, p); // NEW
						const { prompt } = shapeComposePrompt(p, ctx.summary); // CHANGED
						const raw = await callLLM(env, prompt);
						const out = safeParseLLM(raw);
						await notionUpdateDraft(env, p.id, mapComposeToNotion(out, p));
						results.push({ id: p.id, ok: true });
					} catch (e: any) {
						results.push({ id: p.id, ok: false, error: String(e?.message || e) });
					}
				}
				return json({ ok: true, composed: results.filter((r) => r.ok).length, results });
			} catch (e: any) {
				return json({ ok: false, error: String(e?.message || e) }, 500);
			}
		}

		if (url.pathname === '/enrich') {
			try {
				const limit = 20;
				const pages = await notionFetchTodayBatch(env, limit); // reuse your today-batch selector
				const results: Array<{ id: string; ok: boolean; error?: string }> = [];
				for (const p of pages) {
					if (!p.url) {
						results.push({ id: p.id, ok: false, error: 'no URL' });
						continue;
					}
					try {
						const ctx = await fetchUrlContext(p.url);
						if (!ctx) {
							results.push({ id: p.id, ok: false, error: 'no ctx' });
							continue;
						}
						await notionPatchContext(env, p.id, ctx);
						results.push({ id: p.id, ok: true });
					} catch (e: any) {
						results.push({ id: p.id, ok: false, error: String(e?.message || e) });
					}
				}
				return json({ ok: true, enriched: results.filter((r) => r.ok).length, results });
			} catch (e: any) {
				return json({ ok: false, error: String(e?.message || e) }, 500);
			}
		}

		return new Response('OK\nUse /run then /score-new', { status: 200 });
	},
};

// ---------- Config ----------
const MAX_AGE_DAYS = 90;

// Per-Type targets (how many to ingest per run)
const TARGET_PER_TYPE: Record<TypeName, number> = {
	Platform: 3,
	Rails: 2,
	Marketplace: 2,
	Coverage: 1,
	'Long-form': 1,
};

// Typed registry of feeds
type FeedCfg = { url: string; type: TypeName; source?: string; cap?: number };
const REGISTRY: FeedCfg[] = [
	// Platform
	{ url: 'https://developer.apple.com/news/rss/news.rss', type: 'Platform', source: 'Apple Dev News', cap: 2 },
	{ url: 'https://android-developers.googleblog.com/feeds/posts/default?alt=rss', type: 'Platform', source: 'Android Dev Blog', cap: 2 },
	{ url: 'https://blog.youtube/news/rss/', type: 'Platform', source: 'YouTube Blog', cap: 1 },
	{ url: 'https://telegram.org/blog?rss', type: 'Platform', source: 'Telegram', cap: 1 },

	// Rails (IN policy/infra)
	{ url: 'https://www.trai.gov.in/taxonomy/term/19/feed', type: 'Rails', source: 'TRAI', cap: 2 },
	// (MeitY/MCA/CERT-In RSS is inconsistent; add later via HTML extractor if needed)
	{ url: 'https://www.npci.org.in/whats-new/press-releases/rss', type: 'Rails', source: 'NPCI', cap: 1 },

	// Marketplace
	{ url: 'https://sellercentral.amazon.in/forums/c/announcements/7.rss', type: 'Marketplace', source: 'Amazon IN', cap: 1 },
	// (Flipkart seller RSS is not always reachable publicly; add if stable)

	// Coverage
	{ url: 'https://www.medianama.com/feed/', type: 'Coverage', source: 'Medianama', cap: 1 },
	{ url: 'https://inc42.com/feed/', type: 'Coverage', source: 'Inc42', cap: 1 },

	// Long-form (kept tiny)
	// Add reliable long-form with RSS later; leaving empty avoids noise for now
];

// ---------- Types ----------
interface Env {
	SEEN: KVNamespace;
	NOTION_TOKEN: string;
	NOTION_DATABASE_ID: string;
	ADMIN_TOKEN?: string;

	// LLM env
	LLM_PROVIDER?: string;
	GEMINI_API_KEY?: string;
	GEMINI_MODEL_FORCE?: string;
	OPENAI_API_KEY?: string;

	SCORE_BATCH_SIZE?: string;
	DAILY_LLM_LIMIT?: string;
	LLM_MAX_OUTPUT_TOKENS?: string;
}

type TypeName = 'Platform' | 'Rails' | 'Marketplace' | 'Coverage' | 'Long-form';
type FormatName = 'Short-form' | 'Long-form';

type FeedItem = { title: string; link: string; pubDate?: string };

type FeedStats = {
	scanned: number;
	created: number;
	skippedSeen: number;
	skippedNoise: number;
	skippedOld: number;
	samples: string[];
};

type RunResult = {
	totals: { scanned: number; created: number; skippedSeen: number; skippedNoise: number; skippedOld: number };
	types: Record<TypeName, { created: number }>;
	feeds: Record<string, FeedStats>;
};

// ---------- RUNNER (Stage 1: ingest newest-per-type) ----------
async function run(env: Env): Promise<RunResult> {
	const today = new Date();
	const batchId = today.toISOString().slice(0, 10); // YYYY-MM-DD

	// init per-type counters
	const createdPerType: Record<TypeName, number> = {
		Platform: 0,
		Rails: 0,
		Marketplace: 0,
		Coverage: 0,
		'Long-form': 0,
	};

	const feedsOut: Record<string, FeedStats> = {};
	let T_scanned = 0,
		T_created = 0,
		T_seen = 0,
		T_noise = 0,
		T_old = 0;

	// Process feeds grouped by type to enforce per-type TARGETs
	const byType: Record<TypeName, FeedCfg[]> = {
		Platform: [],
		Rails: [],
		Marketplace: [],
		Coverage: [],
		'Long-form': [],
	};
	for (const f of REGISTRY) byType[f.type].push(f);

	for (const type of Object.keys(byType) as TypeName[]) {
		if (!byType[type].length) continue;
		const target = TARGET_PER_TYPE[type] || 0;
		if (target <= 0) continue;

		for (const cfg of byType[type]) {
			if (createdPerType[type] >= target) break;

			const feed = normalizeFeedUrl(cfg.url);
			let scanned = 0,
				created = 0,
				skippedSeen = 0,
				skippedNoise = 0,
				skippedOld = 0;
			const samples: string[] = [];

			const xml = await safeGet(feed);
			if (!xml) {
				feedsOut[feed] = { scanned, created, skippedSeen, skippedNoise, skippedOld, samples };
				continue;
			}

			const items = parseFeed(xml).sort((a, b) => (Date.parse(b.pubDate || '') || 0) - (Date.parse(a.pubDate || '') || 0));

			// take up to cfg.cap for this feed, but also stop if hitting per-type target
			const perFeedCap = Math.max(1, Math.min(cfg.cap || 2, 5));
			for (const item of items) {
				if (created >= perFeedCap) break;
				if (createdPerType[type] >= target) break;

				scanned++;
				if (samples.length < 3) samples.push(item.title || '(no title)');
				if (!item.link) continue;

				const cls = classifyItem(item.title);
				if (cls === 'Noise') {
					skippedNoise++;
					continue;
				}

				const publishedAt = normalizeDate(item.pubDate);
				if (isTooOld(publishedAt)) {
					skippedOld++;
					continue;
				}

				const key = `v3:${type}:${hash(item.link)}`;
				const seen = await env.SEEN.get(key);
				if (seen) {
					skippedSeen++;
					continue;
				}

				try {
					// use the declared feed type as baseline
					let type0: TypeName = cls === 'Long-form' ? 'Long-form' : cfg.type;

					// allow override if the title clearly signals regulator
					const type = coerceTypeByTitle(type0, item.title);
					await createNotionPage(env, {
						sourceName: cfg.source || guessSourceName(feed),
						url: item.link,
						type,
						format: cls === 'Long-form' ? 'Long-form' : 'Short-form',
						publishedAt,
						importedAt: today.toISOString(),
						batchId,
					});
					await env.SEEN.put(key, '1', { expirationTtl: 60 * 60 * 24 * 180 }); // 180 days
					created++;
					createdPerType[type]++;
					T_created++;
				} catch (e) {
					console.error('NOTION_ERR', feed, item.title, e);
				}
			}

			feedsOut[feed] = { scanned, created, skippedSeen, skippedNoise, skippedOld, samples };
			T_scanned += scanned;
			T_seen += skippedSeen;
			T_noise += skippedNoise;
			T_old += skippedOld;
			if (createdPerType[type] >= target) break;
		}
	}

	const total = { scanned: T_scanned, created: T_created, skippedSeen: T_seen, skippedNoise: T_noise, skippedOld: T_old };
	return { totals: total, types: createdPerType, feeds: feedsOut };
}

// ---------- Notion (Stage 2: score today's batch only) ----------
type NotionPageLite = {
	id: string;
	url?: string;
	title?: string; // Source Name
	sourceName?: string;
	type?: TypeName;
	format?: FormatName;
	excerpt?: string;
};

async function notionFetchTodayBatch(env: Env, limit: number): Promise<NotionPageLite[]> {
	const today = new Date().toISOString().slice(0, 10);

	const sorts = [
		{ property: 'Type', direction: 'ascending' as const }, // deterministic
		{ property: 'Published At', direction: 'descending' as const },
		{ property: 'Last Checked', direction: 'descending' as const },
	];

	// 1) Platform + Rails from today's batch, unscored
	const prioReq = {
		filter: {
			and: [
				{ property: 'Batch ID', rich_text: { contains: today } },
				{ property: 'Signal Score', number: { is_empty: true } },
				{
					or: [
						{ property: 'Type', select: { equals: 'Platform' } },
						{ property: 'Type', select: { equals: 'Rails' } },
					],
				},
			],
		},
		sorts,
		page_size: Math.min(limit, 25),
	};
	const top = await notionQuery(env, prioReq);
	if (top.length >= limit) return top.slice(0, limit);

	// 2) Everything else from today's batch, unscored
	const restReq = {
		filter: {
			and: [
				{ property: 'Batch ID', rich_text: { contains: today } },
				{ property: 'Signal Score', number: { is_empty: true } },
				{
					and: [
						{ property: 'Type', select: { does_not_equal: 'Platform' } },
						{ property: 'Type', select: { does_not_equal: 'Rails' } },
					],
				},
			],
		},
		sorts,
		page_size: Math.min(limit - top.length, 25),
	};
	const rest = await notionQuery(env, restReq);

	return [...top, ...rest].slice(0, limit);
}

async function notionQuery(env: Env, body: any): Promise<NotionPageLite[]> {
	const res = await fetch(`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`, {
		method: 'POST',
		headers: notionHeaders(env),
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`Notion query failed: ${res.status} ${await res.text()}`);
	const data = await res.json();
	const pages: NotionPageLite[] = [];
	for (const r of data.results || []) {
		const p = r.properties || {};
		const type = p['Type']?.select?.name as TypeName | undefined;
		const format = p['Format']?.select?.name as FormatName | undefined;
		const title = extractTitle(p['Source Name']);
		pages.push({
			id: r.id,
			url: p['URL']?.url,
			title,
			sourceName: title,
			type,
			format,
			excerpt:
				(p['Source Excerpt']?.rich_text || p['Source Excerpt']?.text || [])
					.map((t: any) => t.plain_text || t.text?.content)
					.filter(Boolean)
					.join(' ')
					.slice(0, 1200) || undefined,
		});
	}
	return pages;
}

// ---------- KV utilities ----------
async function clearAllKV(ns: KVNamespace): Promise<number> {
	let count = 0;
	let cursor: string | undefined = undefined;
	do {
		const list = await ns.list({ cursor, limit: 1000 });
		if (list.keys.length === 0) break;
		await Promise.all(list.keys.map((k) => ns.delete(k.name)));
		count += list.keys.length;
		cursor = list.list_complete ? undefined : list.cursor;
	} while (cursor);
	return count;
}

// ---------- HTTP helpers ----------
function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data, null, 2), {
		status,
		headers: { 'content-type': 'application/json' },
	});
}

// ---------- Networking ----------
async function safeGet(url: string): Promise<string | null> {
	try {
		const res = await fetch(url, {
			headers: {
				Accept: 'application/rss+xml, application/atom+xml, text/xml',
				'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0)',
			},
		});
		if (!res.ok) return null;
		const txt = await res.text();
		return txt;
	} catch {
		return null;
	}
}

// ---------- Parsing (RSS + Atom) ----------
function parseFeed(xml: string): FeedItem[] {
	const items: FeedItem[] = [];

	// RSS
	const itemBlocks = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
	for (const b of itemBlocks) {
		const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
		const link = pick(b, /<link[^>]*>([\s\S]*?)<\/link>/i) || pick(b, /<link[^>]*href="([^"]+)"/i);
		const pub = pick(b, /<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i) || pick(b, /<dc:date[^>]*>([\s\S]*?)<\/dc:date>/i);
		items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}

	// Atom
	const entryBlocks = xml.match(/<entry[\s\S]*?<\/entry>/gi) || [];
	for (const b of entryBlocks) {
		const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
		const link =
			pick(b, /<link[^>]*rel="alternate"[^>]*href="([^"]+)"/i) ||
			pick(b, /<link[^>]*href="([^"]+)"[^>]*rel="alternate"[^>]*>/i) ||
			pick(b, /<link[^>]*href="([^"]+)"/i);
		const pub = pick(b, /<updated[^>]*>([\s\S]*?)<\/updated>/i) || pick(b, /<published[^>]*>([\s\S]*?)<\/published>/i);
		items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}

	// Dedup by link
	const seen = new Set<string>();
	return items.filter((x) => {
		if (!x.link) return false;
		if (seen.has(x.link)) return false;
		seen.add(x.link);
		return true;
	});
}
function pick(text: string, re: RegExp): string {
	const m = text.match(re);
	return m ? m[1] : '';
}
function clean(s: string): string {
	return (s || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
}

// ---------- Classifier & mapping ----------
function classifyItem(title = ''): 'Noise' | 'Long-form' | 'OK' {
	const t = title.toLowerCase();
	const junk = ['hiring', 'careers', 'job opening', "we're hiring", 'award', 'funding round'];
	if (junk.some((w) => t.includes(w))) return 'Noise';
	if (t.includes('podcast') || t.includes('webinar') || t.includes('livestream')) return 'Long-form';
	return 'OK';
}

function coerceTypeByTitle(original: TypeName, title = ''): TypeName {
	if (original !== 'Coverage') return original;
	const t = title.toLowerCase();
	const railsHints = [
		'rbi',
		'trai',
		'npci',
		'meity',
		'sebi',
		'ed ',
		'enforcement directorate',
		'pss act',
		'it rules',
		'guidelines',
		'circular',
		'notification',
	];
	if (railsHints.some((h) => t.includes(h))) return 'Rails';
	return original;
}

function guessSourceName(feedUrl: string): string {
	try {
		const h = new URL(feedUrl).hostname;
		if (h.includes('googleblog')) return 'Android Dev Blog';
		if (h.includes('apple.com') && feedUrl.includes('/news/')) return 'Apple Developer News';
		if (h.includes('google.com') && feedUrl.includes('/search/')) return 'Google Search Central';
		if (h.includes('medianama')) return 'Medianama';
		if (h.includes('inc42')) return 'Inc42';
		if (h.includes('npci')) return 'NPCI';
		if (h.includes('trai')) return 'TRAI';
		if (h.includes('amazon')) return 'Amazon Seller Announcements';
		return h;
	} catch {
		return feedUrl;
	}
}
function normalizeDate(s?: string): string | undefined {
	if (!s) return undefined;
	const d = new Date(s);
	return isNaN(d.getTime()) ? undefined : d.toISOString();
}
function isTooOld(publishedAt?: string): boolean {
	if (!publishedAt) return false;
	const d = new Date(publishedAt);
	if (isNaN(d.getTime())) return false;
	const ageMs = Date.now() - d.getTime();
	return ageMs > MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
}

// ---------- Notion write ----------
async function createNotionPage(
	env: Env,
	{
		sourceName,
		url,
		type,
		format,
		publishedAt,
		importedAt,
		batchId,
	}: { sourceName: string; url: string; type: TypeName; format: FormatName; publishedAt?: string; importedAt: string; batchId: string }
) {
	const props: Record<string, any> = {
		'Source Name': { title: [{ text: { content: sourceName } }] },
		URL: { url },
		Type: { select: { name: type } },
		Format: { select: { name: format } },
		'Imported By': { rich_text: [{ text: { content: 'Cloudflare Worker' } }] },
		'Imported At': { date: { start: importedAt } },
		'Batch ID': { rich_text: [{ text: { content: batchId } }] },
		'Last Checked': { date: { start: new Date().toISOString() } },
	};
	if (publishedAt) props['Published At'] = { date: { start: publishedAt } };

	const payload = { parent: { database_id: env.NOTION_DATABASE_ID }, properties: props };
	const res = await fetch('https://api.notion.com/v1/pages', {
		method: 'POST',
		headers: notionHeaders(env),
		body: JSON.stringify(payload),
	});
	if (!res.ok) throw new Error(`Notion error (${res.status}): ${await res.text()}`);
}

function extractTitle(titleProp: any): string | undefined {
	const arr = titleProp?.title || [];
	if (!Array.isArray(arr) || arr.length === 0) return undefined;
	return arr
		.map((t: any) => t.plain_text || t.text?.content)
		.filter(Boolean)
		.join(' ')
		.trim();
}
function notionHeaders(env: Env): Record<string, string> {
	return {
		Authorization: `Bearer ${env.NOTION_TOKEN}`,
		'Notion-Version': '2022-06-28',
		'Content-Type': 'application/json',
	};
}

// ---------- LLM (Gemini-first; cheapest stable) ----------
type GeminiModel = { name: string };

async function listGeminiModels(key: string): Promise<GeminiModel[]> {
	const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${key}`);
	if (!res.ok) throw new Error(`Gemini listModels error ${res.status}: ${await res.text()}`);
	const data: any = await res.json();
	return (data.models || []).map((m: any) => ({ name: (m.name || '').replace(/^models\//, '') }));
}
function pickBestGeminiModel(models: GeminiModel[], forced?: string): string {
	if (forced) return forced;
	const names = models.map((m) => m.name).filter(Boolean);
	const nonPreview = names.filter((n) => !/preview/i.test(n));
	const prefs = [
		/gemini-1\.5-flash-002/i,
		/gemini-1\.5-flash-latest/i,
		/gemini-1\.5-flash/i,
		/gemini-1\.5-pro-002/i,
		/gemini-1\.5-pro-latest/i,
		/gemini-1\.5-pro/i,
	];
	for (const re of prefs) {
		const hit = nonPreview.find((n) => re.test(n));
		if (hit) return hit;
	}
	const fb = nonPreview.find((n) => /(flash|pro)/i.test(n));
	if (fb) return fb;
	if (!names.length) throw new Error('Gemini: no models available');
	return names[0];
}

async function callLLM(env: Env, prompt: string): Promise<string> {
	const provider = (env as any).LLM_PROVIDER || 'gemini';

	// simple daily call cap
	const dailyCap = parseInt((env as any).DAILY_LLM_LIMIT || '200', 10);
	const todayKey = `llm_calls:${new Date().toISOString().slice(0, 10)}`;
	const used = parseInt((await env.SEEN.get(todayKey)) || '0', 10);
	if (used >= dailyCap) throw new Error('Daily LLM limit reached');
	await env.SEEN.put(todayKey, String(used + 1), { expirationTtl: 60 * 60 * 26 });

	if (provider === 'gemini') {
		const key = (env as any).GEMINI_API_KEY;
		if (!key) throw new Error('Missing GEMINI_API_KEY');

		let model = (env as any).GEMINI_MODEL_FORCE || (await env.SEEN.get('GEMINI_MODEL'));
		if (!model) {
			const models = await listGeminiModels(key);
			model = pickBestGeminiModel(models, (env as any).GEMINI_MODEL_FORCE);
			await env.SEEN.put('GEMINI_MODEL', model);
		}

		const maxOut = clamp(parseInt((env as any).LLM_MAX_OUTPUT_TOKENS || '512', 10), 64, 2048);
		const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
		const body = {
			contents: [{ role: 'user', parts: [{ text: prompt }] }],
			generationConfig: { temperature: 0.2, maxOutputTokens: maxOut },
		};

		let res = await fetch(url, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(body),
		});

		if (res.status === 404) {
			await env.SEEN.delete('GEMINI_MODEL');
			const models = await listGeminiModels(key);
			model = pickBestGeminiModel(models, (env as any).GEMINI_MODEL_FORCE);
			await env.SEEN.put('GEMINI_MODEL', model);
			const retryUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
			res = await fetch(retryUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
		}
		if (!res.ok) throw new Error(`Gemini error ${res.status}: ${await res.text()}`);

		const data: any = await res.json();
		const text = data?.candidates?.[0]?.content?.parts
			?.map((p: any) => p?.text)
			.filter(Boolean)
			.join('\n');
		if (!text) throw new Error('Gemini returned no text');
		return text;
	}

	// OpenAI fallback (requires API key; Plus sub is not the API)
	const key = (env as any).OPENAI_API_KEY;
	if (!key) throw new Error('Missing OPENAI_API_KEY');
	const res = await fetch('https://api.openai.com/v1/chat/completions', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
		body: JSON.stringify({
			model: 'gpt-4o-mini',
			temperature: 0.2,
			messages: [
				{ role: 'system', content: 'You are a pragmatic product manager for India-focused consumer apps. Return STRICT JSON only.' },
				{ role: 'user', content: prompt },
			],
		}),
	});
	if (!res.ok) throw new Error(`OpenAI error ${res.status}: ${await res.text()}`);
	const data: any = await res.json();
	const text = data?.choices?.[0]?.message?.content;
	if (!text) throw new Error('OpenAI returned no text');
	return text;
}

// ---------- LLM prompt shaping & mapping ----------
function shapeContentForLLM(p: NotionPageLite, env?: Env): { prompt: string } {
	const title = p.title || '(no title)';
	const url = p.url || '';
	const type = p.type || '';
	const format = p.format || '';
	const excerpt = (p.excerpt || '').slice(0, 1200);
	const companyProfile = (env as any)?.COMPANY_PROFILE || 'vertical=ecommerce; scale=mid; payments=UPI; top_kpis=Conversion,Churn';

	const prompt = `
  You are a ruthless PM editor for Indian consumer apps. Read the ITEM and return STRICT JSON.
  
  ITEM:
  - Title: ${title}
  - URL: ${url}
  - Type: ${type}
  - Excerpt: ${excerpt}
  - CompanyProfile: ${companyProfile}
  
  Return JSON with ALL keys (no extras):
  {
	"signal_score": 0-10,
	"role_tag": "IC"|"Lead"|"VP",
	"decision_window": "<7d"|"7–30d"|">30d",
	"what_changed": "Quote one concrete change with date/threshold.",
	"quick_actions": [
	  {"owner":"IC|Lead|VP","task":"imperative, ≤14 words","due_days":7},
	  {"owner":"IC|Lead|VP","task":"...","due_days":30}
	],
	"affected_steps": ["Search","Browse","Signup","Checkout","Payment","Refund","Returns","Notifications","Pricing","Policy"],
	"kpi_impact": [{"kpi":"Conversion|Churn|CAC|NPS|Approval|CTR|ASO|SEO","dir":"+|-"}],
	"risks": ["one-liner risk or unknown"],
	"status": "Keep"|"Archive"|"Researching"|"Draft",
	"citations": [{"source":"${url}","note":"anchor or section"}]
  }
  
  Rules:
  - If Type="Coverage" and no rule/policy/pricing/ranking change is stated → status="Archive", signal_score≤3.
  - Quick actions must be shippable. No verbs like “monitor/consider/explore”.
  - Prefer Platform/Rails. If enforcement/deadline mentioned → decision_window "<7d" or "7–30d".
  - Tailor actions to CompanyProfile. If ecommerce, talk checkout/returns/COD; if fintech, talk KYC/limits.
  - Always fill "what_changed" with a concrete clause/date. If unknown, set status="Researching" and add a risk like "source vague".
  `.trim();

	return { prompt };
}

function safeParseLLM(raw: string): any {
	const cleaned = raw.replace(/```json|```/g, '').trim();
	try {
		return JSON.parse(cleaned);
	} catch {
		const m = cleaned.match(/\{[\s\S]*\}/);
		if (!m) throw new Error('LLM did not return JSON');
		return JSON.parse(m[0]);
	}
}

type NotionScorePayload = {
	signalScore: number;
	roleTag: 'IC' | 'Lead' | 'VP';
	quickAction: string;
	why: string;
	decisionWindow: '<7d' | '7–30d' | '>30d';
	affectedSteps: string[];
	kpiImpact: string[];
	status: 'Keep' | 'Archive' | 'Researching' | 'Draft';
};

async function notionUpdateScoring(env: Env, pageId: string, s: NotionScorePayload): Promise<void> {
	const props: any = {
		'Signal Score': { number: clamp(+s.signalScore, 0, 10) },
		'Role Tag': { select: { name: s.roleTag } },
		'Quick Action': { rich_text: [{ text: { content: s.quickAction } }] },
		Why: { rich_text: [{ text: { content: s.why } }] },
		'Decision Window': { select: { name: s.decisionWindow } },
		'Affected Steps': { multi_select: s.affectedSteps.map((n) => ({ name: n })) },
		'KPI Impact': { multi_select: s.kpiImpact.map((n) => ({ name: n })) },
		Status: { select: { name: s.status } },
	};

	const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
		method: 'PATCH',
		headers: notionHeaders(env),
		body: JSON.stringify({ properties: props }),
	});
	if (!res.ok) throw new Error(`Notion update failed: ${res.status} ${await res.text()}`);
}

// ---------- tiny non-crypto hash ----------
function hash(s: string): string {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = (h << 5) + h + s.charCodeAt(i);
	return (h >>> 0).toString(36);
}
function clamp(n: number, lo: number, hi: number) {
	return Math.max(lo, Math.min(hi, n));
}
function pickEnum<T extends string>(val: any, allowed: T[], fallback: T): T {
	if (typeof val === 'string' && allowed.includes(val as T)) return val as T;
	return fallback;
}
function normalizeFeedUrl(u: string): string {
	try {
		const url = new URL(u);
		if (url.hostname.includes('android-developers.googleblog.com'))
			return 'https://android-developers.googleblog.com/feeds/posts/default?alt=rss';
		return url.toString();
	} catch {
		return u.trim();
	}
}

type ComposePage = NotionPageLite & {
	signal?: number;
	status?: string; // Keep/Researching/...
	why?: string;
	quick?: string;
};

async function notionFetchForCompose(env: Env, limit: number): Promise<ComposePage[]> {
	const today = new Date().toISOString().slice(0, 10);

	const sorts = [
		{ property: 'Type', direction: 'ascending' as const },
		{ property: 'Signal Score', direction: 'descending' as const },
		{ property: 'Published At', direction: 'descending' as const },
	];

	// Pass 1: Platform/Rails high-signal, today's batch
	const pass1 = await notionQueryCompose(env, {
		filter: {
			and: [
				{ property: 'Batch ID', rich_text: { contains: today } },
				{ property: 'Signal Score', number: { greater_than_or_equal_to: 6 } },
				{ property: 'Status', select: { does_not_equal: 'Archive' } },
				{
					or: [
						{ property: 'Type', select: { equals: 'Platform' } },
						{ property: 'Type', select: { equals: 'Rails' } },
					],
				},
			],
		},
		sorts,
		page_size: Math.min(limit, 15),
	});

	if (pass1.length >= limit) return pass1.slice(0, limit);

	// Pass 2: Other types with signal ≥7 to keep quality high
	const pass2 = await notionQueryCompose(env, {
		filter: {
			and: [
				{ property: 'Batch ID', rich_text: { contains: today } },
				{ property: 'Signal Score', number: { greater_than_or_equal_to: 7 } },
				{ property: 'Status', select: { does_not_equal: 'Archive' } },
				{
					and: [
						{ property: 'Type', select: { does_not_equal: 'Platform' } },
						{ property: 'Type', select: { does_not_equal: 'Rails' } },
					],
				},
			],
		},
		sorts,
		page_size: Math.min(limit - pass1.length, 10),
	});

	return [...pass1, ...pass2].slice(0, limit);
}

async function notionQueryCompose(env: Env, body: any): Promise<ComposePage[]> {
	const res = await fetch(`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`, {
		method: 'POST',
		headers: notionHeaders(env),
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`Notion query failed: ${res.status} ${await res.text()}`);
	const data = await res.json();
	const out: ComposePage[] = [];
	for (const r of data.results || []) {
		const p = r.properties || {};
		out.push({
			id: r.id,
			url: p['URL']?.url,
			title: extractTitle(p['Source Name']),
			sourceName: extractTitle(p['Source Name']),
			type: p['Type']?.select?.name,
			format: p['Format']?.select?.name,
			signal: p['Signal Score']?.number ?? undefined,
			status: p['Status']?.select?.name,
			why: (p['Why']?.rich_text || [])
				.map((x: any) => x.plain_text)
				.join(' ')
				.trim(),
			quick: (p['Quick Action']?.rich_text || [])
				.map((x: any) => x.plain_text)
				.join(' ')
				.trim(),
		});
	}
	return out;
}

function shapeComposePrompt(p: any, summary: string): { prompt: string } {
	const title = p.title || '(no title)';
	const url = p.url || '';
	const type = p.type || '';

	const prompt = `
  Return STRICT JSON for a LinkedIn post draft for Indian Product Managers.
  
  INPUT:
  - TYPE: ${type}
  - TITLE: ${title}
  - URL: ${url}
  
  SUMMARY (factual, extracted from article):
  ${summary}
  
  CONSTRAINTS:
  - Plain English. No jargon. No hedging.
  - Must include India context (UPI/NPCI/TRAI/ONDC, Android share, local pricing/compliance).
  - Concrete action tied to a surface and KPI (e.g., "Update Play listing keywords for Hindi queries to defend CTR").
  - Max 1200 chars total.
  
  OUTPUT SHAPE:
  {
	"draft_title": "≤ 80 chars, crisp, no clickbait",
	"draft_body": "Sections: What changed (1–2 lines). Why it matters for India (2–4 lines, concrete surfaces like Checkout/SEO/Notifications/Payments). Quick action (≤100 chars, imperative). Open question (1 line).",
	"audience_tier": "IC" | "Lead" | "VP",
	"post_angle": ["Compliance","Growth","ASO/SEO","Checkout/Payments","Marketplace Ops"]
  }
	`.trim();

	return { prompt };
}

type ComposeOut = {
	draft_title: string;
	draft_body: string;
	audience_tier: 'IC' | 'Lead' | 'VP';
	post_angle: string[];
};

function mapComposeToNotion(j: any, p: ComposePage) {
	const title = (j?.draft_title || '').toString().slice(0, 80);
	const body = (j?.draft_body || '').toString().slice(0, 1200);
	const tier = pickEnum(j?.audience_tier, ['IC', 'Lead', 'VP'], p.type === 'Platform' || p.type === 'Rails' ? 'Lead' : 'IC');
	const anglesAll = ['Compliance', 'Growth', 'ASO/SEO', 'Checkout/Payments', 'Marketplace Ops'];
	const angles = Array.isArray(j?.post_angle) ? j.post_angle.filter((x: string) => anglesAll.includes(x)) : [];

	return {
		draftTitle: title,
		draftBody: body,
		audienceTier: tier,
		postAngle: angles,
	};
}

async function notionUpdateDraft(env: Env, pageId: string, d: ReturnType<typeof mapComposeToNotion>) {
	const props: any = {
		'Draft Status': { select: { name: 'Proposed' } },
		'Audience Tier': { select: { name: d.audienceTier } },
		'Post Angle': { multi_select: d.postAngle.map((n) => ({ name: n })) },
	};
	if (d.draftTitle) props['Draft Title'] = { rich_text: [{ text: { content: d.draftTitle } }] };
	if (d.draftBody) props['Draft Body'] = { rich_text: [{ text: { content: d.draftBody } }] };

	const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
		method: 'PATCH',
		headers: notionHeaders(env),
		body: JSON.stringify({ properties: props }),
	});
	if (!res.ok) throw new Error(`Notion draft update failed: ${res.status} ${await res.text()}`);
}

// ---------- Article content fetch & cache ----------

async function fetchArticleText(env: Env, url: string): Promise<string | null> {
	const cacheKey = `content:${hash(url)}`;
	const cached = await env.SEEN.get(cacheKey);
	if (cached) return cached;

	try {
		const res = await fetch(url, {
			headers: {
				'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0; +https://storyofstrategy.com/)',
				Accept: 'text/html,application/xhtml+xml',
			},
		});
		if (!res.ok) return null;

		const ctype = res.headers.get('content-type') || '';
		if (!/html/i.test(ctype)) {
			// Non-HTML (PDF, image, etc.) → we skip full extraction
			return null;
		}

		const html = await res.text();
		const text = extractMainText(html);
		if (text) {
			// keep ~50KB to be safe
			const clipped = text.slice(0, 50_000);
			await env.SEEN.put(cacheKey, clipped, { expirationTtl: 60 * 60 * 24 * 7 }); // 7 days
			return clipped;
		}
		return null;
	} catch {
		return null;
	}
}

// basic text extractor that works in Workers (no DOM)
// It favors <article> or long paragraphs, strips scripts/styles/nav
function extractMainText(html: string): string {
	// remove scripts/styles/comments
	let h = html
		.replace(/<script[\s\S]*?<\/script>/gi, ' ')
		.replace(/<style[\s\S]*?<\/style>/gi, ' ')
		.replace(/<!--[\s\S]*?-->/g, ' ');

	// try <article> first
	const articleMatch = h.match(/<article[\s\S]*?<\/article>/i);
	let core = articleMatch ? articleMatch[0] : h;

	// strip boilerplate tags
	core = core.replace(/<(header|footer|nav|aside|form|button|svg)[\s\S]*?<\/\1>/gi, ' ');

	// kill tags, keep text
	core = core.replace(/<\/?[^>]+>/g, ' ');

	// collapse whitespace
	core = core.replace(/\s+/g, ' ').trim();

	// heuristic: keep longest slice around sentences
	// (in practice the above is enough; we just return)
	return core;
}

async function getContextFor(env: Env, p: { url?: string; title?: string }): Promise<{ summary: string; excerpt: string }> {
	const url = (p.url || '').trim();
	const title = (p.title || '').trim();

	// Try fetching the page text
	let text = url ? await fetchArticleText(env, url) : null;

	// If nothing, fall back to title only
	if (!text || text.length < 400) {
		const minimal = title ? `TITLE: ${title}\nURL: ${url}` : '';
		return { summary: minimal, excerpt: title || '' };
	}

	// Light summarization pass (cheap, short output)
	const sumPrompt = `
  Summarize the following article content in 6–10 bullet points.
  Focus on concrete changes, dates/deadlines, policy/enforcement, pricing/ranking/distribution.
  Keep it neutral and factual; no fluff.
  
  CONTENT:
  ${text.slice(0, 8000)}
	`.trim();

	const summary = await callLLM(env, sumPrompt);
	const excerpt = summary.split('\n').slice(0, 3).join(' ').slice(0, 500);
	return { summary, excerpt };
}

function validateOutput(j: any): { ok: boolean; reason?: string } {
	// 1) quick_actions present and imperative (no banned verbs)
	const banned = /(monitor|consider|explore|keep an eye|stay informed)/i;
	if (!Array.isArray(j.quick_actions) || j.quick_actions.length === 0) {
		return { ok: false, reason: 'no quick_actions' };
	}
	for (const qa of j.quick_actions) {
		const task = (qa?.task || '').toString();
		const owner = (qa?.owner || '').toString();
		const due = +qa?.due_days;
		if (!task || banned.test(task)) return { ok: false, reason: 'banned verb in quick_actions' };
		if (!owner || !/^(IC|Lead|VP)$/.test(owner)) return { ok: false, reason: 'owner missing/invalid' };
		if (!(due >= 1)) return { ok: false, reason: 'due_days invalid' };
		if (task.split(' ').length > 14) return { ok: false, reason: 'task too long' };
	}

	// 2) what_changed must include a date/number/threshold cue
	const wc = (j.what_changed || '').toString();
	if (!wc || !/(\d{4}|\d{1,3}%|₹|\$|\bJan|\bFeb|\bMar|\bApr|\bMay|\bJun|\bJul|\bAug|\bSep|\bOct|\bNov|\bDec|\b20\d{2})/i.test(wc)) {
		return { ok: false, reason: 'what_changed lacks concrete detail/date' };
	}

	// 3) KPI with direction
	if (!Array.isArray(j.kpi_impact) || j.kpi_impact.length === 0) {
		return { ok: false, reason: 'kpi_impact missing' };
	}
	const hasDir = j.kpi_impact.some(
		(x: any) =>
			x && typeof x.kpi === 'string' && /^(Conversion|Churn|CAC|NPS|Approval|CTR|ASO|SEO)$/.test(x.kpi) && /^(?:\+|-)$/.test(x.dir)
	);
	if (!hasDir) return { ok: false, reason: 'kpi_impact lacks +/-' };

	return { ok: true };
}

function mapLLMToNotion(j: any, p: NotionPageLite): NotionScorePayload {
	const stepsAll = ['Search', 'Browse', 'Signup', 'Checkout', 'Payment', 'Refund', 'Returns', 'Notifications', 'Pricing', 'Policy'];
	const kpiAll = ['Conversion', 'Churn', 'CAC', 'NPS', 'Approval', 'CTR', 'ASO', 'SEO'];

	// base parse
	let status = pickEnum(j.status, ['Keep', 'Archive', 'Researching', 'Draft'], 'Keep');
	let score = typeof j.signal_score === 'number' ? j.signal_score : 0;
	let decision = pickEnum(j.decision_window, ['<7d', '7–30d', '>30d'], '>30d');

	// Coverage rule
	if (p.type === 'Coverage' && decision === '>30d') {
		status = 'Archive';
		if (score > 3) score = 3;
	}

	// Validate; if fails → downgrade to Researching and force one IC action
	const v = validateOutput(j);
	let quicks = Array.isArray(j.quick_actions) ? j.quick_actions : [];
	let why = (j.why || '').toString().slice(0, 4000);

	if (!v.ok) {
		status = 'Researching';
		if (score > 5) score = 5;
		// inject a concrete IC task
		const icTask = { owner: 'IC', task: 'Open source and capture exact clause/date into Citations', due_days: 3 };
		quicks = [icTask];
		// clarify the why
		why = `Draft failed validator (${v.reason}). Need exact clause/date and KPI direction grounded on source. ${why}`;
	}

	// Collapse quick_actions -> single line (for Notion "Quick Action")
	const qaLine = quicks
		.slice(0, 2)
		.map((q: any) => {
			const own = /^(IC|Lead|VP)$/.test(q?.owner) ? q.owner : 'IC';
			const task = (q?.task || '').toString().slice(0, 100);
			const due = q?.due_days && +q.due_days > 0 ? ` (due ${q.due_days}d)` : '';
			return `[${own}] ${task}${due}`;
		})
		.join(' • ');

	const steps = Array.isArray(j.affected_steps) ? j.affected_steps.filter((x: string) => stepsAll.includes(x)) : [];
	const kpis = Array.isArray(j.kpi_impact)
		? (j.kpi_impact
				.map((x: any) => x && `${x.kpi}${x.dir}`)
				.filter((s: any) => typeof s === 'string')
				.map((s: string) => {
					// Turn "Conversion+" → "Conversion" with direction retained in Why
					const k = s.replace(/[+-]$/, '');
					return kpiAll.includes(k) ? k : null;
				})
				.filter(Boolean) as string[])
		: [];

	return {
		signalScore: clamp(score, 0, 10),
		roleTag: pickEnum(j.role_tag, ['IC', 'Lead', 'VP'], 'IC'),
		quickAction: qaLine || 'IC: Fill missing clause/date (3d)',
		why,
		decisionWindow: decision,
		affectedSteps: steps,
		kpiImpact: kpis,
		status,
	};
}

function composeLinkedInDraft(p: NotionPageLite, j: any): { title: string; body: string } {
	const title = (p.title || '').trim();
	const url = p.url || '';
	const wc = (j.what_changed || '').toString();
	const qa = Array.isArray(j.quick_actions) ? j.quick_actions.slice(0, 2) : [];
	const kpi = Array.isArray(j.kpi_impact) && j.kpi_impact[0] ? `${j.kpi_impact[0].kpi} ${j.kpi_impact[0].dir}` : 'Conversion +';
	const hook = (title || wc).slice(0, 90);

	const bullets = qa
		.map((q: any) => {
			const own = q?.owner || 'IC';
			const task = (q?.task || '').toString();
			const due = q?.due_days ? ` — ${q.due_days}d` : '';
			return `- [${own}] ${task}${due}`;
		})
		.join('\n');

	const body = `**${hook}**
  What changed: ${wc}
  
  Why it matters (India PM): ${(j.why || '').toString()}
  
  Do this this week:
  ${bullets || '- [IC] Capture exact clause/date into Citations — 3d'}
  
  Watch: ${kpi}
  Refs: ${url}`;

	return { title: hook, body };
}

// --- compose gating ---
const BANNED_VERBS = ['explore', 'monitor', 'consider', 'keep an eye', 'stay updated'];
function shouldCompose(p: NotionPageLite): boolean {
	const t = p.type || 'Coverage';
	if (!['Platform', 'Rails', 'Marketplace'].includes(t)) return false; // skip Coverage/Long-form
	return true;
}

type FetchedContext = { excerpt: string; firstClause: string; firstDate?: string; citation: string };

async function fetchUrlContext(url: string): Promise<FetchedContext | null> {
	// 1) fetch HTML
	const html = await safeGet(url);
	if (!html) return null;

	// 2) crude text extraction
	const text = html
		.replace(/<script[\s\S]*?<\/script>/gi, '')
		.replace(/<style[\s\S]*?<\/style>/gi, '')
		.replace(/<[^>]+>/g, ' ')
		.replace(/\s+/g, ' ')
		.trim();

	if (!text) return null;

	// 3) find first YYYY or Month pattern
	const dateRe =
		/\b(20\d{2}|Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b[^.]{0,40}/i;
	const dateHit = text.match(dateRe)?.[0];

	// 4) try to grab a clause that looks like a rule/change
	const clauseRe =
		/(effective from|effective on|must|shall|required|will be|will start|deadline|deprecate|sunset|remove|enforce)[^.]{0,200}\./i;
	const clause = text.match(clauseRe)?.[0] || '';

	// 5) excerpt
	const excerpt = text.slice(0, 600);

	// 6) citation token [1] with minimal provenance
	const host = (() => {
		try {
			return new URL(url).hostname;
		} catch {
			return url;
		}
	})();
	const citation = `[1] ${host} – ${dateHit ? dateHit : 'no-explicit-date'}`;

	return { excerpt, firstClause: clause, firstDate: dateHit || undefined, citation };
}

async function notionPatchContext(env: Env, pageId: string, ctx: FetchedContext) {
	const props: any = {
		'Source Excerpt': { rich_text: [{ text: { content: ctx.excerpt.slice(0, 4000) } }] },
		Citations: { rich_text: [{ text: { content: ctx.citation } }] },
		'Content Fetched': { checkbox: true },
	};
	await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
		method: 'PATCH',
		headers: notionHeaders(env),
		body: JSON.stringify({ properties: props }),
	});
}

async function notionGetPage(env: Env, pageId: string): Promise<any> {
	const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
		headers: notionHeaders(env),
	});
	if (!res.ok) throw new Error(`Notion get page failed: ${res.status}`);
	return res.json();
}

function validateDraft(title: string, body: string): { ok: boolean; error?: string } {
	if (!title || title.length < 10) return { ok: false, error: 'title too short' };
	if (!/\[\d+\]/.test(body)) return { ok: false, error: 'missing citation token [n]' };
	if (!/\b(20\d{2}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(body)) return { ok: false, error: 'missing concrete date' };
	const lower = body.toLowerCase();
	if (BANNED_VERBS.some((v) => lower.includes(v))) return { ok: false, error: 'banned verb' };
	if (body.length > 1500) return { ok: false, error: 'task too long' };
	return { ok: true };
}

async function notionWriteDraft(env: Env, pageId: string, title: string, body: string) {
	const props: any = {
		'Draft Title': { rich_text: [{ text: { content: title.slice(0, 200) } }] },
		'Draft Body': { rich_text: [{ text: { content: body.slice(0, 4000) } }] },
		'Draft Status': { select: { name: 'Proposed' } },
	};
	await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
		method: 'PATCH',
		headers: notionHeaders(env),
		body: JSON.stringify({ properties: props }),
	});
}

async function composeDraftWithLLM(
	env: Env,
	p: NotionPageLite,
	excerpt: string,
	citations: string
): Promise<{ title: string; body: string }> {
	const base = `
  You write LinkedIn posts for Indian PMs. Plain English, no jargon. One tight post, not a thread.
  
  CONTEXT (verbatim excerpts from source):
  ${excerpt}
  
  CITATIONS TO INCLUDE (use token like [1]):
  ${citations}
  
  Produce:
  - Title: <= 70 chars, concrete.
  - Body: 5 short lines max. Must include: 
	• WHAT CHANGED (quote the clause if present) 
	• WHO’S AFFECTED (IC/Lead/VP) 
	• 1 SPECIFIC ACTION with date/deadline 
	• KPI direction (+/- Conversion/Approval/etc.)
	• One [1] citation token.
  
  Rules:
  - Start action with a verb (no “explore/monitor/consider/keep an eye”).
  - Include at least one concrete date or “effective from …”.
  - Keep it India-relevant; mention UPI/NPCI/TRAI only if applicable.
  Return STRICT JSON: {"title":"...","body":"..."}.
	`.trim();

	const raw = await callLLM(env, base);
	const j = safeParseLLM(raw);
	return { title: (j.title || '').toString(), body: (j.body || '').toString() };
}
