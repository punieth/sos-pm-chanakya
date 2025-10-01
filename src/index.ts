export default {
	async scheduled(_e: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(orchestrate(env));
	},

	async fetch(req: Request, env: Env): Promise<Response> {
		const url = new URL(req.url);

		if (url.pathname === '/run') {
			return json(await orchestrate(env));
		}

		// Small debug: compose one by id
		if (url.pathname === '/compose-one') {
			const id = url.searchParams.get('id');
			if (!id) return json({ ok: false, error: 'missing ?id=' }, 400);
			const page = await notionGetPageLite(env, id);
			if (!page) return json({ ok: false, error: 'not found' }, 404);
			const post = composeOneFromScored(page);
			const v = validatePosts([post])[0];
			if (!v.valid) return json({ ok: false, reason: v.reason, draft: v.post });
			await patchDraftToNotion(env, id, v.post!);
			return json({ ok: true, id, draft: v.post });
		}

		// Admin: clear ALL KV keys (protect with token)
		if (url.pathname === '/clear-all') {
			const token = url.searchParams.get('token') || '';

			const deleted = await clearAllKV(env.SEEN);
			return json({ ok: true, deleted });
		}

		if (url.pathname === '/health') return new Response('OK');
		return new Response('OK\nUse /run', { status: 200 });
	},
};

/** =======================
 * Config & Types
 * ======================= */
const MAX_AGE_DAYS = 90;

// Target intake per Type (per run)
const TARGET_PER_TYPE: Record<TypeName, number> = {
	Platform: 2,
	Rails: 6,
	Marketplace: 2,
	Coverage: 4,
	'Long-form': 1,
};

type TypeName = 'Platform' | 'Rails' | 'Marketplace' | 'Coverage' | 'Long-form';
type FormatName = 'Short-form' | 'Long-form';

type FeedCfg = { url: string; type: TypeName; source?: string; cap?: number };

const REGISTRY: FeedCfg[] = [
	// Platform (keep a few)
	{ url: 'https://developer.apple.com/news/rss/news.rss', type: 'Platform', source: 'Apple Dev News', cap: 2 },
	{ url: 'https://android-developers.googleblog.com/feeds/posts/default?alt=rss', type: 'Platform', source: 'Android Dev Blog', cap: 2 },
	{ url: 'https://blog.youtube/news/rss/', type: 'Platform', source: 'YouTube Blog', cap: 1 },

	// Rails / Indian policy & infra
	{ url: 'https://www.trai.gov.in/taxonomy/term/19/feed', type: 'Rails', source: 'TRAI', cap: 3 },
	{ url: 'https://www.npci.org.in/whats-new/press-releases/rss', type: 'Rails', source: 'NPCI', cap: 2 },
	{ url: 'https://pib.gov.in/RssFeeds/TopNewsRSS.aspx', type: 'Rails', source: 'PIB (Top News)', cap: 2 },
	{ url: 'https://pib.gov.in/RssFeeds/RssFeed_1.aspx', type: 'Rails', source: 'PIB (Releases)', cap: 2 },
	{ url: 'https://www.meity.gov.in/news-rss', type: 'Rails', source: 'MeitY', cap: 2 },
	{ url: 'https://cert-in.org.in/RssFeeds/AdvisoryRSS.xml', type: 'Rails', source: 'CERT-In Advisories', cap: 2 },
	{ url: 'https://www.rbi.org.in/Rss/PressReleases.xml', type: 'Rails', source: 'RBI Press', cap: 2 },
	{ url: 'https://www.sebi.gov.in/sebiweb/rss/MediaRSS.do', type: 'Rails', source: 'SEBI Media', cap: 2 },
	{ url: 'https://www.ondc.org/blog/feed/', type: 'Rails', source: 'ONDC Blog', cap: 1 },
	{ url: 'https://www.uidai.gov.in/en/component/obrss/press-releases?format=raw', type: 'Rails', source: 'UIDAI Press', cap: 1 },

	// Marketplace / Ecosystem ops
	{ url: 'https://sellercentral.amazon.in/forums/c/announcements/7.rss', type: 'Marketplace', source: 'Amazon IN', cap: 1 },
	{ url: 'https://razorpay.com/blog/rss.xml', type: 'Marketplace', source: 'Razorpay Blog', cap: 1 },
	{ url: 'https://paytm.com/blog/feed/', type: 'Marketplace', source: 'Paytm Blog', cap: 1 },

	// Coverage (curated India tech/policy coverage)
	{ url: 'https://www.medianama.com/feed/', type: 'Coverage', source: 'Medianama', cap: 2 },
	{ url: 'https://inc42.com/feed/', type: 'Coverage', source: 'Inc42', cap: 2 },
	{ url: 'https://the-ken.com/feed/', type: 'Coverage', source: 'The Ken', cap: 1 },
	{ url: 'https://www.moneycontrol.com/rss/technology.xml', type: 'Coverage', source: 'Moneycontrol Tech', cap: 1 },
	{ url: 'https://economictimes.indiatimes.com/tech/rssfeeds/13357270.cms', type: 'Coverage', source: 'ET Tech', cap: 1 },
];

const SCHEMA_TTL_MS = 5 * 60 * 1000;
let notionSchemaCache: { schema: NotionSchema; fetchedAt: number } | null = null;

interface Env {
	SEEN: KVNamespace;
	NOTION_TOKEN: string;
	NOTION_DATABASE_ID: string;

	// LLM
	LLM_PROVIDER?: string; // default: "gemini"
	GEMINI_API_KEY?: string;
	GEMINI_MODEL_FORCE?: string; // e.g., "gemini-2.0-flash-lite"
	LLM_MAX_OUTPUT_TOKENS?: string; // default 512
	DAILY_LLM_LIMIT?: string; // default 200

	SCORE_BATCH_SIZE?: string; // default 15
	FF_FORCE_ARCHIVE_LOW_SIGNAL?: string;
	FF_DISABLE_FETCH?: string;
	MAX_FETCH_BYTES?: string;
	MAX_BODY_LEN?: string;
	FF_DRY_RUN?: string;
}

declare const process: { env?: Record<string, string | undefined> } | undefined;

type FeedItem = { title: string; link: string; pubDate?: string };

type FeedStats = {
	scanned: number;
	created: number;
	skippedSeen: number;
	skippedNoise: number;
	skippedOld: number;
	samples: string[];
};

type StageReport<T = any> = {
	attempted: number;
	ok: number;
	failed: number;
	items: Array<{ id?: string; title?: string; url?: string; ok: boolean; error?: string; extra?: T }>;
};

type NotionPropertyType =
	| 'title'
	| 'rich_text'
	| 'select'
	| 'multi_select'
	| 'number'
	| 'date'
	| 'checkbox'
	| 'url'
	| 'status';

type NotionSchema = Record<
	string,
	{
		type: NotionPropertyType;
		options?: string[];
	}
>;

type ComposeConsider = {
	id: string;
	title?: string;
	type?: TypeName;
	score?: number;
	window?: NotionPageLite['decisionWindow'];
};
type ComposeSkipped = ComposeConsider & { reasons: string[] };

type OrchestrateResult = {
	ok: boolean;
	batchId: string;
	startedAt: string;
	ingest: {
		totals: { scanned: number; created: number; skippedSeen: number; skippedNoise: number; skippedOld: number };
		types: Record<TypeName, number>;
		feeds: Record<string, FeedStats>;
	};
	enrich: StageReport<{ excerpt?: string }>;
	score: { attempted: number; updated: number; llmCalls: number; results: Array<{ id: string; ok: boolean; error?: string }> };
	compose: StageReport<{ status?: string; preview?: string }>;
	composeDebug: {
		considered: Array<{
			id: string;
			title?: string;
			type?: TypeName;
			score: number | null;
			window: NotionPageLite['decisionWindow'] | null;
		}>;
		skipped: Array<{
			id: string;
			title?: string;
			type?: TypeName;
			score: number | null;
			window: NotionPageLite['decisionWindow'] | null;
			reasons: string[];
		}>;
	};
	validate: { attempted: number; passed: number; failed: number; details: Array<{ id: string; valid: boolean; reason?: string }> };
	patch: StageReport;
	notes?: string;
};

/** =======================
 * Orchestrator
 * ======================= */
async function clearAllKV(ns: KVNamespace): Promise<number> {
	let count = 0;
	let cursor: string | undefined = undefined;
	do {
		const list = (await ns.list({ cursor, limit: 1000 })) as {
			keys: Array<{ name: string }>;
			list_complete: boolean;
			cursor?: string;
		};
		if (list.keys.length === 0) break;
		await Promise.all(list.keys.map((k) => ns.delete(k.name)));
		count += list.keys.length;
		cursor = list.list_complete ? undefined : list.cursor;
	} while (cursor);
	return count;
}

// —— India policy-ish detector for Coverage
const INDIA_KEYWORDS = [
	'rbi',
	'pss act',
	'upi',
	'npci',
	'meity',
	'trai',
	'dot',
	'telecommunications act',
	'sebi',
	'mca',
	'mha',
	'it rules',
	'data protection',
	'dpdp',
	'digital personal data',
	'gst council',
	'income tax',
	'fema',
	'ed ',
	'enforcement directorate',
	'dppi',
	'privacy bill',
	'pricing cap',
	'price cap',
	'interchange',
	'kyd',
	'kyc',
	'kyb',
	'consent manager',
	'ocen',
	'account aggregator',
];

async function orchestrate(env: Env): Promise<OrchestrateResult> {
	const startedAt = new Date().toISOString();
	const batchId = startedAt.slice(0, 10);

	// 1) Ingest newest items by type
	const ingestRes = await ingestNewest(env);

	// 2) Enrich: fetch source content → Source Excerpt + Content Fetched
	const createdIds = await notionFetchTodayIds(env, batchId);
	const enrichReport: StageReport<{ excerpt?: string; status?: string; httpStatus?: number }> = {
		attempted: createdIds.length,
		ok: 0,
		failed: 0,
		items: [],
	};

	for (const id of createdIds) {
		try {
			const lite = await notionGetPageLite(env, id);
			if (!lite?.url) {
				enrichReport.items.push({ id, ok: false, error: 'No URL' });
				enrichReport.failed++;
				continue;
			}
			const health = await checkUrlHealth(env, lite.url);
			if (!health.alive) {
				logEvent('url_dead', { id, url: lite.url, status: health.status, reason: health.reason });
				const noteBits = ['Dead link'];
				if (health.status) noteBits.push(`(HTTP ${health.status})`);
				else if (health.reason) noteBits.push(`(${health.reason})`);
				await notionPatch(env, id, {
					Status: 'Archive',
					'Reviewers Notes': noteBits.join(' '),
				});
				enrichReport.items.push({
					id,
					title: lite.title,
					url: lite.url,
					ok: true,
					extra: { status: 'dead_link', httpStatus: health.status },
				});
				continue;
			}
			const { excerpt } = await fetchExcerpt(lite.url);
			await notionPatch(env, id, {
				'Source Excerpt': excerpt || '(no excerpt)',
				'Content Fetched': true,
			});
			enrichReport.items.push({ id, title: lite.title, url: lite.url, ok: true, extra: { excerpt } });
			enrichReport.ok++;
			logEvent('enrich_ok', { id, url: lite.url, excerptBytes: (excerpt || '').length });
		} catch (e: any) {
			enrichReport.items.push({ id, ok: false, error: errorMessage(e) });
			enrichReport.failed++;
			logEvent('enrich_error', { id, error: errorMessage(e) });
		}
	}

	// 3) Score: only unscored items from today’s batch
	const scoreLimit = clamp(parseInt(env.SCORE_BATCH_SIZE || '15', 10), 1, 50);
	const pagesToScore = await notionFetchUnscoredToday(env, batchId, scoreLimit);
	const scoreResults: Array<{ id: string; ok: boolean; error?: string }> = [];
	let llmCalls = 0;

	for (const p of pagesToScore) {
		try {
			const shaped = shapeForScoring(p);
			const raw = await callLLM(env, shaped);
			llmCalls++;
			const parsed = safeParseLLM(raw);
			await notionUpdateScoring(env, p.id, mapLLMToNotion(parsed, p));
			scoreResults.push({ id: p.id, ok: true });
			logEvent('score_ok', { id: p.id, url: p.url });
		} catch (e: any) {
			scoreResults.push({ id: p.id, ok: false, error: errorMessage(e) });
			logEvent('score_error', { id: p.id, url: p.url, error: errorMessage(e) });
		}
	}

	// 4) Compose: decide candidates, say why some were skipped, draft a few
	// broad pool (Content Fetched + Status in {Keep, Researching})
	// 4) Compose: decide candidates, say why some were skipped, draft a few
	const consider = await notionFetchComposeConsider(env, batchId);

	// Looser pool: score >=4 OR Rails (if fetched)
	const toComposeStrict = await notionFetchForCompose(env, batchId, 6);

	// Add India-policyish Coverage (score≥4, timely)
	const extraPolicyish = consider
		.filter(
			(p) =>
				p.type === 'Coverage' &&
				isIndiaPolicyish(p) &&
				(p.signalScore ?? 0) >= 4 &&
				p.decisionWindow &&
				p.decisionWindow !== '>30d' &&
				!toComposeStrict.find((t) => t.id === p.id)
		)
		.slice(0, Math.max(0, 6 - toComposeStrict.length));

	const composeCandidates = [...toComposeStrict, ...extraPolicyish];

	// Track skipped
	const skipped: Array<{
		id: string;
		title?: string;
		type?: TypeName;
		score: number | null;
		window: NotionPageLite['decisionWindow'] | null;
		reasons: string[];
	}> = [];
	for (const c of consider) {
		if (!composeCandidates.find((t) => t.id === c.id)) {
			const reasons: string[] = [];
			if ((c.signalScore ?? 0) < 4) reasons.push('score<4');
			if (!c.decisionWindow || c.decisionWindow === '>30d') reasons.push('window>30d/unknown');
			if (c.type === 'Coverage' && !isIndiaRelevant(c.excerpt || '')) reasons.push('Coverage not India-relevant');
			skipped.push({
				id: c.id,
				title: c.title,
				type: c.type,
				score: c.signalScore ?? null,
				window: c.decisionWindow ?? null,
				reasons,
			});
		}
	}

	// Actually compose drafts
	const composeReport: StageReport<{ status?: string; preview?: string }> = {
		attempted: composeCandidates.length,
		ok: 0,
		failed: 0,
		items: [],
	};
	const drafts: DraftPost[] = [];

	for (const p of composeCandidates) {
		try {
			if (p.type === 'Coverage' && !isIndiaRelevant(p.excerpt || '')) {
				composeReport.items.push({ id: p.id, title: p.title, url: p.url, ok: false, error: 'Coverage not India-relevant' });
				composeReport.failed++;
				logEvent('compose_skip', { id: p.id, reason: 'not_india_relevant', url: p.url });
				continue;
			}
			let draft = composeOneFromScored(p);
			if (env.FF_DISABLE_FETCH !== '1') {
				try {
					const geminiDraft = await composeWithGemini(env, p);
					draft = finalizeDraftFromGemini(env, draft, geminiDraft, p);
					logEvent('compose_gemini_ok', { id: p.id, url: p.url });
				} catch (err: unknown) {
					logEvent('compose_gemini_error', {
						id: p.id,
						url: p.url,
						error: errorMessage(err),
					});
				}
			}
			drafts.push(draft);
			composeReport.items.push({
				id: p.id,
				title: p.title,
				url: p.url,
				ok: true,
				extra: { status: 'composed', preview: (draft.body || '').slice(0, 180) },
			});
			composeReport.ok++;
		} catch (e: unknown) {
			composeReport.items.push({ id: p.id, title: p.title, url: p.url, ok: false, error: errorMessage(e) });
			composeReport.failed++;
			logEvent('compose_failure', { id: p.id, url: p.url, error: errorMessage(e) });
		}
	}

	// 5) Validate drafts (SOFT): we *still patch* even if they fail, but mark Needs Fact
	const validationResults = validatePosts(drafts); // {id, valid, reason?, post?}
	const passed = validationResults.filter((v) => v.valid);
	const failed = validationResults.filter((v) => !v.valid);

	// Mark failures on the row so you see why, and set status to Needs Fact
	for (const f of failed) {
		try {
			const note = `Validator: ${f.reason || 'unknown'}`;
			if (f.post) f.post.reviewerNote = note;
			await notionPatch(env, f.id, {
				'Draft Status': 'Needs Fact',
				'Reviewers Notes': note,
			});
			logEvent('validate_fail', { id: f.id, reason: f.reason });
		} catch {}
	}

	for (const v of validationResults) {
		if (v.valid && v.reviewerNote) {
			logEvent('validate_soft', { id: v.id, note: v.reviewerNote });
		}
	}

	// 6) Patch drafts back to Notion — BOTH passed and failed (failed as Needs Fact)
	const patchReport: StageReport = { attempted: validationResults.length, ok: 0, failed: 0, items: [] };
	for (const v of validationResults) {
		try {
			const post = v.post!;
			post.status = v.valid ? post.status || 'Proposed' : 'Needs Fact';
			await patchDraftToNotion(env, v.id, post);
			patchReport.items.push({ id: v.id, ok: true });
			patchReport.ok++;
			logEvent('patch_ok', { id: v.id, status: post.status, note: post.reviewerNote });
		} catch (e: unknown) {
			patchReport.items.push({ id: v.id, ok: false, error: errorMessage(e) });
			patchReport.failed++;
			logEvent('patch_error', { id: v.id, error: errorMessage(e) });
		}
	}

	return {
		ok: true,
		batchId,
		startedAt,
		ingest: ingestRes,
		enrich: enrichReport,
		score: { attempted: pagesToScore.length, updated: scoreResults.filter((r) => r.ok).length, llmCalls, results: scoreResults },
		compose: composeReport,
		composeDebug: {
			considered: consider.map((c) => ({
				id: c.id,
				title: c.title,
				type: c.type,
				score: c.signalScore ?? null,
				window: c.decisionWindow ?? null,
			})),
			skipped,
		},
		validate: {
			attempted: validationResults.length,
			passed: passed.length,
			failed: failed.length,
			details: validationResults.map((v) => ({ id: v.id, valid: v.valid, reason: v.reason })),
		},
		patch: patchReport,
		notes: 'End-to-end run (soft validation; always patch drafts)',
	};
}

/** =======================
 * Ingest stage
 * ======================= */
async function ingestNewest(env: Env) {
	const today = new Date();
	const batchId = today.toISOString().slice(0, 10);
	const perTypeCreated: Record<TypeName, number> = { Platform: 0, Rails: 0, Marketplace: 0, Coverage: 0, 'Long-form': 0 };

	const byType: Record<TypeName, FeedCfg[]> = { Platform: [], Rails: [], Marketplace: [], Coverage: [], 'Long-form': [] };
	for (const f of REGISTRY) byType[f.type].push(f);

	const feedsOut: Record<string, FeedStats> = {};
	let T_scanned = 0,
		T_created = 0,
		T_seen = 0,
		T_noise = 0,
		T_old = 0;

	for (const type of Object.keys(byType) as TypeName[]) {
		const target = TARGET_PER_TYPE[type] || 0;
		if (!target) continue;

		for (const cfg of byType[type]) {
			if (perTypeCreated[type] >= target) break;
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
			const perFeedCap = Math.max(1, Math.min(cfg.cap || 2, 5));

			for (const it of items) {
				if (created >= perFeedCap) break;
				if (perTypeCreated[type] >= target) break;

				scanned++;
				if (samples.length < 3) samples.push(it.title || '(no title)');
				if (!it.link) continue;

				const cls = classifyItem(it.title);
				if (cls === 'Noise') {
					skippedNoise++;
					continue;
				}

				const publishedAt = normalizeDate(it.pubDate);
				if (isTooOld(publishedAt)) {
					skippedOld++;
					continue;
				}

				const contentHash = hash(it.link);
				const key = `v3:${type}:${contentHash}`;
				if (await env.SEEN.get(key)) {
					skippedSeen++;
					continue;
				}

				try {
					const createdPageId = await createNotionPage(env, {
						sourceName: cfg.source || guessSourceName(feed),
						url: it.link,
						type,
						format: cls === 'Long-form' ? 'Long-form' : 'Short-form',
						publishedAt,
						importedAt: today.toISOString(),
						batchId,
						contentHash,
					});
					await env.SEEN.put(key, '1', { expirationTtl: 60 * 60 * 24 * 180 });
					if (createdPageId) {
						logEvent('ingest_create', { id: createdPageId, url: it.link, type });
						created++;
						perTypeCreated[type]++;
						T_created++;
					} else {
						skippedSeen++;
					}
				} catch (e) {
					const msg = errorMessage(e);
					samples.push(`ERR: ${msg.slice(0, 180)}`);
				}
			}

			feedsOut[feed] = { scanned, created, skippedSeen, skippedNoise, skippedOld, samples };
			T_scanned += scanned;
			T_seen += skippedSeen;
			T_noise += skippedNoise;
			T_old += skippedOld;
			if (perTypeCreated[type] >= target) break;
		}
	}

	return {
		totals: { scanned: T_scanned, created: T_created, skippedSeen: T_seen, skippedNoise: T_noise, skippedOld: T_old },
		types: perTypeCreated,
		feeds: feedsOut,
	};
}

/** =======================
 * Enrich helpers
 * ======================= */
async function notionFetchTodayIds(env: Env, batchId: string): Promise<string[]> {
	const res = await retryingFetch(
		`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`,
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify({
				filter: { property: 'Batch ID', rich_text: { contains: batchId } },
				sorts: [{ property: 'Published At', direction: 'descending' }],
				page_size: 50,
			}),
		},
		{ label: 'notion_fetch_today_ids' }
	);
	await assertNotionOk(res, 'notion_fetch_today_ids');
	const data: any = await res.json();
	return (data.results || []).map((r: any) => r.id);
}

async function fetchExcerpt(url: string): Promise<{ excerpt: string }> {
	const txt = await safeGet(url);
	if (!txt) return { excerpt: '' };
	const cleaned = stripTags(txt).replace(/\s+/g, ' ').trim();
	return { excerpt: cleaned.slice(0, 1400) };
}

async function checkUrlHealth(
	env: Env,
	url?: string
): Promise<{ alive: boolean; status?: number; reason?: string; skipped?: boolean; contentType?: string }> {
	if (!url) return { alive: false, reason: 'missing_url' };
	if (env.FF_DISABLE_FETCH === '1') return { alive: true, skipped: true };
	const headers = new Headers({ 'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0)' });
	let headRes: Response | null = null;
	try {
		headRes = await retryingFetch(url, { method: 'HEAD', redirect: 'follow', headers }, { label: 'url_health_head', retries: 1 });
	} catch {}
	const headOk = headRes?.ok === true;
	const headStatus = headRes?.status;
	if (headOk && headStatus && headStatus < 400) {
		const ct = headRes?.headers.get('content-type') || undefined;
		if (ct && !/html|xml|rss/i.test(ct)) {
			return { alive: false, status: headStatus, reason: 'content_type', contentType: ct };
		}
		return { alive: true, status: headStatus, contentType: headRes?.headers.get('content-type') || undefined };
	}
	let getRes: Response | null = null;
	try {
		const rangedHeaders = new Headers(headers);
		rangedHeaders.set('Range', 'bytes=0-8191');
		getRes = await retryingFetch(
			url,
			{ method: 'GET', redirect: 'follow', headers: rangedHeaders },
			{ label: 'url_health_get', retries: 1 }
		);
	} catch (err) {
		const reason = errorMessage(err) || 'fetch_error';
		return { alive: false, reason };
	}
	if (!getRes) return { alive: false, reason: 'fetch_error' };
	const status = getRes.status;
	if (status >= 400) {
		return { alive: false, status, reason: 'http_status' };
	}
	const ct = getRes.headers.get('content-type') || undefined;
	if (ct && !/html|xml|rss/i.test(ct)) {
		return { alive: false, status, reason: 'content_type', contentType: ct };
	}
	return { alive: true, status, contentType: ct };
}

/** =======================
 * Scoring helpers
 * ======================= */
type NotionPageLite = {
	id: string;
	url?: string;
	title?: string;
	type?: TypeName;
	format?: FormatName;
	why?: string;
	quickAction?: string;
	signalScore?: number;
	decisionWindow?: '<7d' | '7–30d' | '>30d';
	audienceTier?: 'IC' | 'Lead' | 'VP';
	excerpt?: string;
	affectedSteps?: string[];
	kpiImpact?: string[];
};

async function notionFetchUnscoredToday(env: Env, batchId: string, limit: number): Promise<NotionPageLite[]> {
	const res = await retryingFetch(
		`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`,
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify({
			filter: {
				and: [
					{ property: 'Batch ID', rich_text: { contains: batchId } },
					{ property: 'Signal Score', number: { is_empty: true } },
				],
			},
			sorts: [
				{ property: 'Type', direction: 'ascending' },
				{ property: 'Published At', direction: 'descending' },
			],
			page_size: Math.min(limit, 25),
			}),
		},
		{ label: 'notion_fetch_unscored' }
	);
	await assertNotionOk(res, 'notion_fetch_unscored');
	const data: any = await res.json();
	return mapResultsToLite(data.results);
}

function nz<T>(x: T | undefined | null): x is T {
	return x !== undefined && x !== null;
}
function andFilter(...clauses: any[]): any | undefined {
	const c = clauses.filter(nz);
	if (c.length === 0) return undefined;
	if (c.length === 1) return c[0];
	return { and: c };
}
function orFilter(...clauses: any[]): any | undefined {
	const c = clauses.filter(nz);
	if (c.length === 0) return undefined;
	if (c.length === 1) return c[0];
	return { or: c };
}

async function notionFetchForCompose(env: Env, batchId: string, limit: number): Promise<NotionPageLite[]> {
	const statusKeep = { property: 'Status', select: { equals: 'Keep' } };
	const statusResearch = { property: 'Status', select: { equals: 'Researching' } };
	const eligibleByScore = { property: 'Signal Score', number: { greater_than_or_equal_to: 4 } };
	const eligibleRails = { property: 'Type', select: { equals: 'Rails' } };
	const windowNotGT30 = { property: 'Decision Window', select: { does_not_equal: '>30d' } };
	const windowEmpty = { property: 'Decision Window', select: { is_empty: true } };

	const filter =
		andFilter(
			{ property: 'Batch ID', rich_text: { contains: batchId } },
			{ property: 'Content Fetched', checkbox: { equals: true } },
			orFilter(statusKeep, statusResearch),
			orFilter(eligibleByScore, eligibleRails),
			orFilter(windowNotGT30, windowEmpty)
		) || { property: 'Batch ID', rich_text: { contains: batchId } };

	const body = {
		filter,
		sorts: [
			{ property: 'Signal Score', direction: 'descending' as const },
			{ property: 'Type', direction: 'ascending' as const },
			{ property: 'Published At', direction: 'descending' as const },
		],
		page_size: Math.min(limit, 10),
	};

	const res = await retryingFetch(
		`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`,
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify(body),
		},
		{ label: 'notion_fetch_compose' }
	);
	await assertNotionOk(res, 'notion_fetch_compose');
	const data: any = await res.json();
	return mapResultsToLite(data.results);
}

async function notionFetchComposeConsider(env: Env, batchId: string): Promise<NotionPageLite[]> {
	const statusKeep = { property: 'Status', select: { equals: 'Keep' } };
	const statusResearch = { property: 'Status', select: { equals: 'Researching' } };

	const filter =
		andFilter(
			{ property: 'Batch ID', rich_text: { contains: batchId } },
			{ property: 'Content Fetched', checkbox: { equals: true } },
			orFilter(statusKeep, statusResearch)
		) || { property: 'Batch ID', rich_text: { contains: batchId } };

	const body = {
		filter,
		sorts: [
			{ property: 'Published At', direction: 'descending' as const },
			{ property: 'Type', direction: 'ascending' as const },
		],
		page_size: 50,
	};

	const res = await retryingFetch(
		`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`,
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify(body),
		},
		{ label: 'notion_fetch_consider' }
	);
	await assertNotionOk(res, 'notion_fetch_consider');
	const data: any = await res.json();
	return mapResultsToLite(data.results);
}

async function notionGetPageLite(env: Env, id: string): Promise<NotionPageLite | null> {
	const res = await retryingFetch(
		`https://api.notion.com/v1/pages/${id}`,
		{ headers: notionHeaders(env) },
		{ label: 'notion_get_page' }
	);
	if (!res.ok) {
		if (res.status === 404) return null;
		await assertNotionOk(res, 'notion_get_page');
	}
	const r: any = await res.json();
	return mapOneToLite(r);
}

function mapResultsToLite(results: any[]): NotionPageLite[] {
	return (results || []).map(mapOneToLite).filter(Boolean) as NotionPageLite[];
}

function mapOneToLite(r: any): NotionPageLite {
	const p = r.properties || {};
	const title = extractTitle(p['Source Name']);
	const url = p['URL']?.url;
	const type = p['Type']?.select?.name as TypeName | undefined;
	const format = p['Format']?.select?.name as FormatName | undefined;
	const score = p['Signal Score']?.number;
	const dw = p['Decision Window']?.select?.name as any;
	const role = p['Role Tag']?.select?.name as any;
	const why = extractText(p['Why']);
	const qa = extractText(p['Quick Action']);
	const excerpt = extractText(p['Source Excerpt']);
	const affectedSteps = Array.isArray(p['Affected Steps']?.multi_select)
		? p['Affected Steps'].multi_select.map((x: any) => x?.name).filter(Boolean)
		: [];
	const kpiImpact = Array.isArray(p['KPI Impact']?.multi_select)
		? p['KPI Impact'].multi_select.map((x: any) => x?.name).filter(Boolean)
		: [];

	return {
		id: r.id,
		url,
		title,
		type,
		format,
		why,
		quickAction: qa,
		signalScore: score,
		decisionWindow: dw,
		audienceTier: role,
		excerpt,
		affectedSteps,
		kpiImpact,
	};
}

function shapeForScoring(p: NotionPageLite): string {
	const t = p.title || '(no title)';
	const u = p.url || '';
	const type = p.type || '';
	const fmt = p.format || '';
	return `
  You are scoring news for Indian PMs. Input:
  TITLE: ${t}
  URL: ${u}
  TYPE: ${type}
  FORMAT: ${fmt}
  
  Return STRICT JSON:
  {
	"signal_score": 0-10 number,
	"role_tag": "IC"|"Lead"|"VP",
	"quick_action": "one imperative sentence <= 100 chars",
	"why": "2-4 lines in plain English (no jargon), India context if relevant",
	"decision_window": "<7d"|"7–30d"|">30d",
	"affected_steps": ["Search","Browse","Signup","Checkout","Payment","Refund","Returns","Notifications","Pricing","Policy"],
	"kpi_impact": ["Conversion","Churn","CAC","NPS","Approval","CTR","ASO","SEO"],
	"status": "Keep"|"Archive"|"Researching"|"Draft"
  }
  
  Rules:
  - Prefer PLATFORM/RAILS with policy/pricing/distribution/enforcement.
  - COVERAGE without clear rule → likely Archive 0–3.
  - Quick action must be <=100 chars and start with a verb.
  JSON only.
	`.trim();
}

async function notionUpdateScoring(env: Env, pageId: string, s: NotionScorePayload): Promise<void> {
	await notionPatch(env, pageId, {
		'Signal Score': clamp(+s.signalScore, 0, 10),
		'Role Tag': s.roleTag,
		'Quick Action': s.quickAction || '',
		Why: s.why || '',
		'Decision Window': s.decisionWindow,
		'Affected Steps': s.affectedSteps,
		'KPI Impact': s.kpiImpact,
		Status: s.status,
	});
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

function mapLLMToNotion(j: any, p: NotionPageLite): NotionScorePayload {
	const stepsAll = ['Search', 'Browse', 'Signup', 'Checkout', 'Payment', 'Refund', 'Returns', 'Notifications', 'Pricing', 'Policy'];
	const kpiAll = ['Conversion', 'Churn', 'CAC', 'NPS', 'Approval', 'CTR', 'ASO', 'SEO'];

	let status = pickEnum(j.status, ['Keep', 'Archive', 'Researching', 'Draft'], 'Keep');
	let score = typeof j.signal_score === 'number' ? j.signal_score : 0;
	let window = pickEnum(j.decision_window, ['<7d', '7–30d', '>30d'], '>30d');

	if (p.type === 'Coverage' && window === '>30d') {
		status = 'Archive';
		if (score > 3) score = 3;
	}

	// Coverage that smells like India policy — treat like Rails-lite
	if (p.type === 'Coverage' && isIndiaPolicyish(p)) {
		// make it eligible for compose
		if (score < 5) score = 5;
		if (window === '>30d') window = '7–30d';
		if (status === 'Archive') status = 'Researching';
	}

	return {
		signalScore: clamp(score, 0, 10),
		roleTag: pickEnum(j.role_tag, ['IC', 'Lead', 'VP'], 'IC'),
		quickAction: (j.quick_action || '').toString().slice(0, 100),
		why: (j.why || '').toString().slice(0, 4000),
		decisionWindow: window,
		affectedSteps: Array.isArray(j.affected_steps) ? j.affected_steps.filter((x: string) => stepsAll.includes(x)) : [],
		kpiImpact: Array.isArray(j.kpi_impact) ? j.kpi_impact.filter((x: string) => kpiAll.includes(x)) : [],
		status,
	};
}

/** =======================
 * Compose + Validate + Patch
 * ======================= */
type DraftPost = {
	id: string;
	title: string; // Draft Title
	body: string; // Draft Body
	status?: 'Proposed' | 'Ready' | 'Needs Fact';
	citations?: string;
	audienceTier?: 'IC' | 'Lead' | 'VP';
	postAngle?: Array<'Playbook' | 'Hot take' | 'Explainer'>;
	reviewerNote?: string;
	contentHash?: string;
	// meta from source so validator can be smarter (esp. Rails)
	_meta?: {
		type?: TypeName;
		excerpt?: string;
		decisionWindow?: '<7d' | '7–30d' | '>30d';
		sourceUrl?: string;
	};
};

function composeOneFromScored(p: NotionPageLite): DraftPost {
	const src = p.title || guessSourceName(p.url || '');
	const title = `[${p.type || 'Update'}] ${src}: What Indian PMs should do now`;

	// NEW: sanitize quick action so it always passes validator (≤100 chars, imperative)
	const qaRaw = (p.quickAction || '').trim();
	const qa = sanitizeAction(qaRaw, p) || fallbackQuickAction(p);

	const body = `What changed
	• ${summarizeChange(p)}
	
	Why it matters (India)
	• ${plainIndiaWhy(p)}
	
	Action for this week
	• ${qa}
	
	If you own Checkout/Payments — expect impact on Conversion/Approval.
	Source: ${p.url || ''}`;
	const citations = extractCitations(p);
	return {
		id: p.id,
		title,
		body,
		status: 'Proposed',
		citations,
		audienceTier: p.audienceTier || 'IC',
		postAngle: ['Explainer'],
		contentHash: hash(`${title}|${body}|${citations}`),
		_meta: {
			type: p.type,
			excerpt: p.excerpt,
			decisionWindow: p.decisionWindow,
			sourceUrl: p.url,
		},
	};
}
// map Type→action skeletons, always imperative + specific
function concreteAction(p: NotionPageLite, steps: string[]): string {
	const step = (steps[0] || '').replace(/[,/].*$/, '') || 'Checkout';
	if (p.type === 'Rails') return `Paste clause/date in Citations; audit ${step}; open Jira with owner and ETA.`;
	if (p.type === 'Platform') return `Review policy impact; update ${step} config; ship a canary within 48h.`;
	if (p.type === 'Marketplace') return `Compare seller policy vs flow; fix copy/flow; draft seller comms.`;
	// Coverage (policyish or not) — keep concrete but non-committal
	return `Record key claim + date in Citations; no product changes until verified.`;
}

// Ensure imperative, non-vague, ≤100 chars
function sanitizeAction(a: string, p: NotionPageLite): string {
	const banned = /(learn more|follow along|stay tuned|monitor trends)/i;
	const imperativeOk =
		/^(review|audit|ship|enable|disable|update|fix|move|switch|file|notify|publish|roll back|rollout|test|pin|deprecate|block|allow|whitelist|blacklist|add|remove|verify|collect|document)\b/i;

	let s = (a || '').replace(/\s+/g, ' ').trim();
	if (!s || banned.test(s) || s.length > 100 || !imperativeOk.test(s)) {
		// typed fallback
		if (p.type === 'Rails') s = 'Audit compliance doc and capture clause/date into Citations';
		else if (p.type === 'Platform') s = 'Test checkout/sign-in flows and document breakages';
		else s = 'Review impact area and list exact touchpoints';
	}
	if (s.length > 100) s = s.slice(0, 100);
	return s;
}

function summarizeChange(p: NotionPageLite): string {
	const e = (p.excerpt || '').replace(/\s+/g, ' ');
	// date-ish: “effective|from|by <date>”, or ISO, or “on <Month DD, YYYY>”
	const dateish =
		e.match(/\b(?:effective|from|by|on)\s+(?:\w+\s+\d{1,2},\s*\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\s\w+\s\d{4})/i) ||
		e.match(/\b\d{4}-\d{2}-\d{2}\b/);
	if (dateish) return `Timing signal: ${dateish[0]}. Confirm clause in source and paste into “Citations”.`;
	if (p.decisionWindow && p.decisionWindow !== '>30d') return `Window ${p.decisionWindow} implied; confirm clause/date in source.`;
	return 'Update announced; confirm exact clause/date in source.';
}

function plainIndiaWhy(p: NotionPageLite): string {
	if (p.type === 'Rails') return 'Affects compliance, distribution or pricing levers; missing it risks enforcement and churn.';
	if (p.type === 'Platform') return 'Impacts ranking, eligibility or fees. Align app, pricing and notifications accordingly.';
	return 'Likely lower signal; keep for context unless it drives rules, pricing or distribution.';
}

function extractCitations(p: NotionPageLite): string {
	const bits = [];
	if (p.url) bits.push(p.url);
	const e = p.excerpt || '';
	const firstLine = e.split('. ')[0]?.slice(0, 120);
	if (firstLine) bits.push(`Excerpt: "${firstLine}..."`);
	return bits.join('\n');
}

type ValidationResult = { id: string; valid: boolean; reason?: string; post?: DraftPost; reviewerNote?: string };

function validatePosts(posts: DraftPost[]): ValidationResult[] {
	const out: ValidationResult[] = [];
	for (const post of posts) {
		const clone: DraftPost = {
			...post,
			postAngle: post.postAngle ? [...post.postAngle] : undefined,
			_meta: post._meta ? { ...post._meta } : undefined,
		};
		let valid = true;
		let reason: string | undefined;

		const title = (post.title || '').trim();
		if (!title) {
			valid = false;
			reason = 'Missing Draft Title';
		}

		const body = post.body || '';
		if (valid && !/what changed/i.test(body)) {
			valid = false;
			reason = 'Missing "What changed" section';
		}

		const qaMatch = body.match(/Action for this week\s*\n\s*•\s*(.+)/i);
		const qaLine = qaMatch ? qaMatch[1].trim() : '';
		if (valid && !qaLine) {
			valid = false;
			reason = 'Missing Action line';
		}
		if (valid && qaLine.length > 100) {
			valid = false;
			reason = 'Action line > 100 chars';
		}
		if (valid && /learn more|follow along|stay tuned/i.test(qaLine)) {
			valid = false;
			reason = 'Vague action';
		}

		const citations = post.citations || '';
		const sourceUrl = post._meta?.sourceUrl;
		if (valid && sourceUrl && !citations.includes(sourceUrl)) {
			valid = false;
			reason = 'Citations missing source URL';
		}

		const clauseSignal = extractTimingClause(citations);
		if (!clauseSignal) {
			clone.reviewerNote = 'Validator: No concrete clause';
		}

		out.push({ id: post.id, valid, reason, post: clone, reviewerNote: clone.reviewerNote });
	}
	return out;
}

async function patchDraftToNotion(env: Env, pageId: string, post: DraftPost): Promise<void> {
	const payload: Record<string, unknown> = {
		'Draft Title': post.title || '',
		'Draft Body': post.body || '',
		'Draft Status': post.status || 'Proposed',
		'Citations': post.citations || '',
		'Audience Tier': post.audienceTier,
		'Post Angle': post.postAngle || [],
		'Content Hash': post.contentHash,
	};
	if (typeof post.reviewerNote === 'string') {
		payload['Reviewers Notes'] = post.reviewerNote;
	}
	await notionPatch(env, pageId, payload);
}

/** =======================
 * Notion write (ingest)
 * ======================= */
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
		contentHash,
	}: {
		sourceName: string;
		url: string;
		type: TypeName;
		format: FormatName;
		publishedAt?: string;
		importedAt: string;
		batchId: string;
		contentHash: string;
	}
): Promise<string | null> {
	const existing = await notionFindPageByHash(env, contentHash);
	if (existing) {
		logEvent('ingest_skip_duplicate', { id: existing, hash: contentHash, url });
		return null;
	}
	const schema = await loadNotionSchema(env);
	const props: Record<string, unknown> = {
		'Source Name': sourceName,
		URL: url,
		Type: type,
		Format: format,
		'Imported By': 'Cloudflare Worker',
		'Imported At': importedAt,
		'Batch ID': batchId,
		'Last Checked': new Date().toISOString(),
		'Content Hash': contentHash,
	};
	if (publishedAt) props['Published At'] = publishedAt;
	const payload = coerceProperties(schema, props);
	if (env.FF_DRY_RUN === '1') {
		logEvent('notion_create_skipped', { url, type, props: Object.keys(payload) });
		return 'dry-run';
	}
	const res = await retryingFetch(
		'https://api.notion.com/v1/pages',
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify({ parent: { database_id: env.NOTION_DATABASE_ID }, properties: payload }),
		},
		{ label: 'notion_create' }
	);
	await assertNotionOk(res, 'notion_create');
	const data: any = await res.json();
	return data?.id || null;
}

async function notionFindPageByHash(env: Env, contentHash: string): Promise<string | null> {
	const schema = await loadNotionSchema(env);
	if (!schema['Content Hash']) {
		logEvent('notion_hash_skip', { reason: 'missing_property' });
		return null;
	}
	const body = {
		filter: {
			property: 'Content Hash',
			rich_text: { equals: contentHash },
		},
		page_size: 1,
	};
	const res = await retryingFetch(
		`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`,
		{
			method: 'POST',
			headers: notionHeaders(env),
			body: JSON.stringify(body),
		},
		{ label: 'notion_find_hash' }
	);
	await assertNotionOk(res, 'notion_find_hash');
	const data: any = await res.json();
	return data?.results?.[0]?.id || null;
}

/** =======================
 * LLM (Gemini-first; cheap)
 * ======================= */
async function callLLM(env: Env, prompt: string): Promise<string> {
	const provider = env.LLM_PROVIDER || 'gemini';
	const dailyCap = parseInt(env.DAILY_LLM_LIMIT || '200', 10);
	const todayKey = `llm:${new Date().toISOString().slice(0, 10)}`;
	const used = parseInt((await env.SEEN.get(todayKey)) || '0', 10);
	if (used >= dailyCap) throw new Error('Daily LLM limit reached');
	await env.SEEN.put(todayKey, String(used + 1), { expirationTtl: 60 * 60 * 26 });

	if (provider === 'gemini') {
		const key = env.GEMINI_API_KEY;
		if (!key) throw new Error('Missing GEMINI_API_KEY');

		// Choose cheapest broadly available text model unless forced
		const model = env.GEMINI_MODEL_FORCE || 'gemini-2.0-flash-lite';
		const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
		const body = {
			contents: [{ role: 'user', parts: [{ text: prompt }] }],
			generationConfig: { temperature: 0.2, maxOutputTokens: clamp(parseInt(env.LLM_MAX_OUTPUT_TOKENS || '512', 10), 64, 2048) },
		};
		const res = await retryingFetch(
			url,
			{ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
			{ label: 'llm_score', retries: 2 }
		);
		if (!res.ok) throw new Error(`Gemini error ${res.status}: ${await res.text()}`);
		const data: any = await res.json();
		const text =
			data?.candidates?.[0]?.content?.parts
				?.map((p: any) => p?.text)
				.filter(Boolean)
				.join('\n') || data?.candidates?.[0]?.content?.parts?.[0]?.text;
		if (!text) throw new Error('Gemini returned no text');
		return text;
	}

	throw new Error('Only Gemini path implemented in this file (set LLM_PROVIDER=gemini).');
}

// Parse LLM output that may come back wrapped in ```json fences or with extra text
function safeParseLLM(raw: string): any {
	const cleaned = raw.replace(/```json|```/g, '').trim();
	try {
		return JSON.parse(cleaned);
	} catch {
		// try to salvage the first {...} block
		const m = cleaned.match(/\{[\s\S]*\}/);
		if (!m) throw new Error('LLM did not return JSON');
		return JSON.parse(m[0]);
	}
}

/** =======================
 * Net/Parsing helpers
 * ======================= */
async function safeGet(url: string): Promise<string | null> {
	try {
		const res = await retryingFetch(
			url,
			{
			headers: {
				Accept: 'text/html,application/rss+xml,application/atom+xml,text/xml;q=0.9',
				'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0)',
			},
			},
			{ label: 'safe_get', retries: 2 }
		);
		if (!res.ok) return null;
		return await res.text();
	} catch {
		return null;
	}
}

function parseFeed(xml: string): FeedItem[] {
	const items: FeedItem[] = [];

	// --- RSS items ---
	const rss = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
	for (const b of rss) {
		const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
		const link =
			pick(b, /<link[^>]*>([\s\S]*?)<\/link>/i) ||
			pick(b, /<link[^>]*href="([^"]+)"/i) ||
			// NEW: some Indian gov feeds omit <link>; use <guid> as stable URL-ish fallback
			pick(b, /<guid[^>]*>([\s\S]*?)<\/guid>/i);
		const pub = pick(b, /<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i) || pick(b, /<dc:date[^>]*>([\s\S]*?)<\/dc:date>/i);

		items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}

	// --- Atom entries ---
	const atom = xml.match(/<entry[\s\S]*?<\/entry>/gi) || [];
	for (const b of atom) {
		const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
		const link =
			pick(b, /<link[^>]*rel="alternate"[^>]*href="([^"]+)"/i) ||
			pick(b, /<link[^>]*href="([^"]+)"[^>]*rel="alternate"[^>]*>/i) ||
			pick(b, /<link[^>]*href="([^"]+)"/i);
		const pub = pick(b, /<updated[^>]*>([\s\S]*?)<\/updated>/i) || pick(b, /<published[^>]*>([\s\S]*?)<\/published>/i);

		items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}

	const seen = new Set<string>();
	return items.filter((x) => x.link && !seen.has(x.link) && seen.add(x.link));
}

function classifyItem(title = ''): 'Noise' | 'Long-form' | 'OK' {
	const t = title.toLowerCase();
	const indiaSignal = ['trai', 'rbi', 'uidai', 'npci', 'meity', 'sebi', 'ondc', 'pib', 'guidelines', 'advisory', 'press release'];
	if (indiaSignal.some((w) => t.includes(w))) return 'OK';

	const junk = ['hiring', 'careers', 'job opening', "we're hiring", 'award', 'funding round'];
	if (junk.some((w) => t.includes(w))) return 'Noise';
	if (t.includes('podcast') || t.includes('webinar') || t.includes('livestream')) return 'Long-form';
	return 'OK';
}

function normalizeDate(s?: string): string | undefined {
	if (!s) return undefined; // accept missing dates
	const d = new Date(s);
	return isNaN(d.getTime()) ? undefined : d.toISOString();
}

function isTooOld(publishedAt?: string): boolean {
	if (!publishedAt) return false; // no date → don't exclude
	const d = new Date(publishedAt);
	if (isNaN(d.getTime())) return false;
	return Date.now() - d.getTime() > MAX_AGE_DAYS * 86400000;
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

/** =======================
 * Notion generic helpers
 * ======================= */
function notionHeaders(env: Env): Record<string, string> {
	return {
		Authorization: `Bearer ${env.NOTION_TOKEN}`,
		'Notion-Version': '2022-06-28',
		'Content-Type': 'application/json',
	};
}

async function notionPatch(env: Env, pageId: string, props: Record<string, unknown>) {
	const schema = await loadNotionSchema(env);
	const payload = coerceProperties(schema, props);
	if (Object.keys(payload).length === 0) return;
	if (env.FF_DRY_RUN === '1') {
		logEvent('notion_patch_skipped', { pageId, props: Object.keys(payload) });
		return;
	}
	const res = await retryingFetch(
		`https://api.notion.com/v1/pages/${pageId}`,
		{
			method: 'PATCH',
			headers: notionHeaders(env),
			body: JSON.stringify({ properties: payload }),
		},
		{ label: 'notion_patch' }
	);
	await assertNotionOk(res, 'notion_patch');
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
function extractText(rt: any): string {
	const arr = rt?.rich_text || [];
	if (!Array.isArray(arr) || arr.length === 0) return '';
	return arr
		.map((t: any) => t.plain_text || t.text?.content)
		.filter(Boolean)
		.join(' ')
		.trim();
}

/** =======================
 * Utilities
 * ======================= */
function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data, null, 2), { status, headers: { 'content-type': 'application/json' } });
}
function pick(text: string, re: RegExp): string {
	const m = text.match(re);
	return m ? m[1] : '';
}
function clean(s: string): string {
	return (s || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
}
function stripTags(html: string): string {
	return html
		.replace(/<script[\s\S]*?<\/script>/gi, '')
		.replace(/<style[\s\S]*?<\/style>/gi, '')
		.replace(/<[^>]+>/g, ' ');
}
function hash(s: string): string {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = (h << 5) + h + s.charCodeAt(i);
	return (h >>> 0).toString(36);
}
function clamp(n: number, lo: number, hi: number) {
	return Math.max(lo, Math.min(hi, n));
}
function pickEnum<T extends string>(val: any, allowed: T[], fallback: T): T {
	return typeof val === 'string' && (allowed as any).includes(val) ? (val as T) : fallback;
}

function extractSignalsFromExcerpt(e: string) {
	const dateish = /\b(?:effective|from|on|by)\s+(?:\w+\s+\d{1,2},\s*\d{4}|\d{1,2}\s\w+\s\d{4}|\d{4}-\d{2}-\d{2})/i.exec(e || '');
	const numeric = /\b(?:₹|Rs\.?|INR|USD|\d[\d,]*\s?(?:MHz|GHz|%)|\b\d{1,3}(?:,\d{3})+\b|\b\d{4}-\d{2}-\d{2}\b)/.exec(e || '');
	return { dateish: dateish?.[0], hasNumeric: !!numeric };
}

function isIndiaRelevant(text: string): boolean {
	const t = text.toLowerCase();
	const needles = [
		'india',
		'indian',
		'rbi',
		'meity',
		'trai',
		'uidai',
		'npci',
		'ondc',
		'sebi',
		'dpiit',
		'mha',
		'mof',
		'up i',
		'aadhaar',
		'bharat',
		'karnataka',
		'maharashtra',
		'delhi',
		'gst',
		'upi',
		'india stack',
		'indias stack',
	];
	return needles.some((n) => t.includes(n));
}

function isIndiaPolicyish(p: NotionPageLite): boolean {
	const base = `${p.title || ''} ${p.excerpt || ''}`.toLowerCase();
	const kws = [
		'policy',
		'pricing',
		'fee',
		'regulator',
		'ban',
		'compliance',
		'guideline',
		'notified',
		'circular',
		'order',
		'mandate',
		'framework',
		'licens',
	];
	return isIndiaRelevant(base) && kws.some((k) => base.includes(k));
}

function fallbackQuickAction(p: NotionPageLite): string {
	// Short, imperative, ≤100 chars
	if (p.type === 'Rails') return 'Scan source; list clauses affecting pricing/comms; flag teams with owners by EOD.';
	if (p.type === 'Platform') return 'Review feature/policy shift; note app impacts; open a tracking ticket.';
	if (p.type === 'Marketplace') return 'Check seller/ops policy changes; update playbooks if applicable.';
	// Coverage
	return 'Skim and capture 1 actionable risk or opportunity for your product area.';
}

function logEvent(phase: string, payload: Record<string, unknown>): void {
	const base = typeof payload === 'object' && payload ? payload : {};
	const entry = {
		phase,
		ts: new Date().toISOString(),
		...base,
	};
	try {
		console.log(JSON.stringify(entry));
	} catch {
		console.log(`{"phase":"${phase}","error":"log stringify failed"}`);
	}
}

function errorMessage(err: unknown): string {
	if (err instanceof Error && typeof err.message === 'string') return err.message;
	if (typeof err === 'string') return err;
	try {
		return JSON.stringify(err);
	} catch {
		return String(err);
	}
}

async function assertNotionOk(res: Response, label: string): Promise<void> {
	if (res.ok) return;
	const body = await res.text();
	const requestId = res.headers.get('x-request-id') || undefined;
	logEvent('notion_error', {
		label,
		status: res.status,
		requestId,
		bodySnippet: body.slice(0, 500),
	});
	throw new Error(`Notion ${label} failed: ${res.status} ${body}`);
}

async function loadNotionSchema(env: Env): Promise<NotionSchema> {
	const now = Date.now();
	if (notionSchemaCache && now - notionSchemaCache.fetchedAt < SCHEMA_TTL_MS) {
		return notionSchemaCache.schema;
	}
	const res = await retryingFetch(`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}`, {
		headers: notionHeaders(env),
	});
	await assertNotionOk(res, 'notion_schema');
	const data: any = await res.json();
	const props = data?.properties || {};
	const schema: NotionSchema = {};
	for (const [name, def] of Object.entries<any>(props)) {
		const type = def?.type as NotionPropertyType | undefined;
		if (!type) continue;
		const base: { type: NotionPropertyType; options?: string[] } = { type };
		if (type === 'select' && Array.isArray(def?.select?.options)) {
			base.options = def.select.options.map((o: any) => o?.name).filter(Boolean);
		}
		if (type === 'multi_select' && Array.isArray(def?.multi_select?.options)) {
			base.options = def.multi_select.options.map((o: any) => o?.name).filter(Boolean);
		}
		schema[name] = base;
	}
	notionSchemaCache = { schema, fetchedAt: now };
	return schema;
}

function coerceProperties(schema: NotionSchema, props: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [name, raw] of Object.entries(props)) {
		if (raw === undefined || raw === null) continue;
		const def = schema[name];
		if (!def) {
			logEvent('prop_skip', { name });
			continue;
		}
		switch (def.type) {
			case 'title': {
				const text = toRichText(raw);
				if (text) out[name] = { title: text };
				break;
			}
			case 'rich_text': {
				const text = toRichText(raw);
				if (text) out[name] = { rich_text: text };
				break;
			}
			case 'select': {
				const value = firstString(raw);
				if (value) out[name] = { select: { name: value } };
				break;
			}
			case 'multi_select': {
				const values = toStringArray(raw);
				out[name] = { multi_select: values.map((v) => ({ name: v })) };
				break;
			}
			case 'number': {
				const num = typeof raw === 'number' ? raw : Number(raw);
				if (!Number.isNaN(num)) out[name] = { number: num };
				break;
			}
			case 'date': {
				if (typeof raw === 'string' && raw) {
					out[name] = { date: { start: raw } };
				} else if (
					raw &&
					typeof raw === 'object' &&
					'start' in (raw as Record<string, unknown>)
				) {
					out[name] = { date: raw };
				}
				break;
			}
			case 'checkbox': {
				out[name] = { checkbox: !!raw };
				break;
			}
			case 'url': {
				const url = firstString(raw);
				if (url) out[name] = { url };
				break;
			}
			case 'status': {
				const status = firstString(raw);
				if (status) out[name] = { status: { name: status } };
				break;
			}
			default:
				break;
		}
	}
	return out;
}

type GeminiComposeResult = {
	draftTitle: string;
	draftBody: string;
	citations: string[];
	postAngle: string[];
};

async function composeWithGemini(env: Env, item: NotionPageLite): Promise<GeminiComposeResult> {
	const key = env.GEMINI_API_KEY;
	if (!key) throw new Error('Missing GEMINI_API_KEY');
	const model = env.GEMINI_MODEL_FORCE || 'gemini-1.5-flash';
	const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
	const contentPieces = [item.title, item.excerpt, item.quickAction, item.why]
		.filter((p): p is string => typeof p === 'string' && p.trim().length > 0)
		.map((p) => p.trim())
		.join('\n');
	const prompt =
		'Summarize this source for Indian PMs. Return strict JSON: {"draftTitle":string,"draftBody":string,"citations":string[],"postAngle":string[]}. ' +
		'Constraints: title<=120 chars; body<=12000 chars; include a \'Clause:\' line if present; otherwise leave citations with the source URL and a note to confirm. ' +
		`SOURCE:\n${contentPieces || '(no excerpt)'}\nTYPE:${item.type || ''}\nDECISION_WINDOW:${item.decisionWindow || ''}\nURL:${item.url || ''}`;
	const body = {
		contents: [
			{
				parts: [
					{
						text: prompt,
					},
				],
			},
		],
	};
	const res = await retryingFetch(endpoint, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body),
	});
	if (!res.ok) {
		throw new Error(`Gemini compose failed: ${res.status} ${await res.text()}`);
	}
	const data: any = await res.json();
	const text =
		data?.candidates?.[0]?.content?.parts
			?.map((p: any) => p?.text)
			.filter(Boolean)
			.join('\n') || '';
	const cleaned = text.replace(/```json|```/gi, '').trim();
	const parsed = JSON.parse(cleaned) as GeminiComposeResult;
	return parsed;
}

function finalizeDraftFromGemini(env: Env, base: DraftPost, result: GeminiComposeResult, page: NotionPageLite): DraftPost {
	const maxBody = clamp(parseInt(env.MAX_BODY_LEN || '12000', 10), 1000, 20000);
	const allowedAngles = new Set(['Playbook', 'Hot take', 'Explainer']);
	const title = (result.draftTitle || '').trim().slice(0, 120) || base.title;
	let body = (result.draftBody || '').trim();
	if (!body) body = base.body;
	if (!/what changed/i.test(body)) {
		body = base.body;
	}
	body = body.slice(0, maxBody);
	const rawAngles = Array.isArray(result.postAngle) ? result.postAngle : [];
	const postAngle = rawAngles
		.map((a) => (typeof a === 'string' ? a.trim() : ''))
		.filter((a) => allowedAngles.has(a)) as DraftPost['postAngle'];
	const url = page.url || base._meta?.sourceUrl || '';
	const cited = new Set<string>();
	const citationsArr = Array.isArray(result.citations) ? result.citations : [];
	for (const c of citationsArr) {
		if (typeof c === 'string' && c.trim()) cited.add(c.trim());
	}
	if (url) {
		const hasUrl = Array.from(cited).some((c) => c.includes(url));
		if (!hasUrl) cited.add(url);
	}
	const citations = Array.from(cited).join('\n') || base.citations || '';
	return {
		...base,
		title,
		body,
		postAngle: postAngle && postAngle.length ? postAngle : base.postAngle,
		citations,
		contentHash: hash(`${title}|${body}|${citations}`),
	};
}

async function retryingFetch(
	input: RequestInfo,
	init: RequestInit = {},
	{
		retries = 3,
		backoffMs = 250,
		maxBackoffMs = 5000,
		retryStatuses,
		label,
	}: { retries?: number; backoffMs?: number; maxBackoffMs?: number; retryStatuses?: number[]; label?: string } = {}
): Promise<Response> {
	const allowed = retryStatuses || [408, 425, 429, 500, 502, 503, 504, 520, 522, 524];
	const body = init.body;
	let attempt = 0;
	while (true) {
		try {
			const attemptInit = { ...init };
			attemptInit.body = body;
			const res = await fetch(input, attemptInit);
			if (!allowed.includes(res.status) || attempt >= retries) {
				return res;
			}
			const retryAfter = parseRetryAfter(res) ?? jitter(backoffMs * Math.pow(2, attempt), maxBackoffMs);
			logEvent('retrying_fetch', {
				label,
				attempt,
				status: res.status,
				url: typeof input === 'string' ? input : (input as Request).url,
			});
			await waitMs(retryAfter);
		} catch (err) {
			if (attempt >= retries) throw err;
			const delay = jitter(backoffMs * Math.pow(2, attempt), maxBackoffMs);
			logEvent('retrying_fetch_error', {
				label,
				attempt,
				error: errorMessage(err),
				url: typeof input === 'string' ? input : (input as Request).url,
			});
			await waitMs(delay);
		}
		attempt++;
	}
}

function toRichText(value: unknown): Array<{ type: 'text'; text: { content: string } }> | null {
	const str = firstString(value) ?? '';
	if (!str) return [{ type: 'text', text: { content: '' } }];
	return [{ type: 'text', text: { content: str.slice(0, 4000) } }];
}

function firstString(value: unknown): string | null {
	if (typeof value === 'string') return value;
	if (Array.isArray(value)) {
		const c = value.find((v) => typeof v === 'string');
		return typeof c === 'string' ? c : null;
	}
	if (value && typeof value === 'object' && 'name' in (value as any) && typeof (value as any).name === 'string') {
		return (value as any).name;
	}
	if (value instanceof Date) return value.toISOString();
	return value != null ? String(value) : null;
}

function toStringArray(value: unknown): string[] {
	if (Array.isArray(value)) {
		return value.map((v) => (v != null ? String(v).trim() : '')).filter(Boolean);
	}
	if (typeof value === 'string') {
		return value
			.split(/[;,]/)
			.map((v) => v.trim())
			.filter(Boolean);
	}
	return value != null ? [String(value)] : [];
}

function parseRetryAfter(res: Response): number | null {
	const header = res.headers.get('Retry-After');
	if (!header) return null;
	const seconds = Number(header);
	if (!Number.isNaN(seconds)) return Math.max(0, seconds * 1000);
	const date = new Date(header);
	if (Number.isNaN(date.getTime())) return null;
	return Math.max(0, date.getTime() - Date.now());
}

function jitter(base: number, max: number): number {
	const capped = Math.min(max, base);
	return Math.random() * capped + capped / 2;
}

async function waitMs(ms: number): Promise<void> {
	await new Promise((resolve) => setTimeout(resolve, ms));
}

function extractTimingClause(text?: string): string | null {
	if (!text) return null;
	const clauseLine = text
		.split(/\n+/)
		.map((line) => line.trim())
		.find((line) => /^Clause:/i.test(line));
	if (clauseLine) return clauseLine;
	const iso = text.match(/\b\d{4}-\d{2}-\d{2}\b/);
	if (iso) return iso[0];
	const month = text.match(
		/\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s*\d{4}\b/
	);
	if (month) return month[0];
	return null;
}

if (typeof process !== 'undefined' && process.env?.RUN_INLINE_TESTS === '1') {
	(function testCoerceProperties() {
		const schema: NotionSchema = {
			'Post Angle': { type: 'multi_select' },
			'Draft Title': { type: 'rich_text' },
		};
		const props = coerceProperties(schema, {
			'Post Angle': ['Explainer', 'Playbook'],
			'Draft Title': 'Sample',
			Unknown: 'skip me',
		});
		const angles = (props['Post Angle'] as any)?.multi_select;
		console.assert(Array.isArray(angles) && angles.length === 2, 'Post Angle should map to multi_select');
		console.assert(!('Unknown' in props), 'Unknown props should be dropped');
	})();

	(function testExtractTimingClause() {
		console.assert(extractTimingClause('See ISO 2024-05-01') === '2024-05-01', 'Should pick ISO date');
		console.assert(
			extractTimingClause('Clause: Section 3.2\nEffective on March 3, 2024') === 'Clause: Section 3.2',
			'Should detect Clause line'
		);
	})();

	(function testValidateDraftSoftPass() {
		const draft: DraftPost = {
			id: 'test1',
			title: 'Rails update',
			body: 'What changed\n• Update announced\n\nAction for this week\n• Ship fix now',
			citations: 'https://example.com/source',
			postAngle: ['Explainer'],
			contentHash: hash('t'),
			_meta: { sourceUrl: 'https://example.com/source' },
		};
		const res = validatePosts([draft])[0];
		console.assert(res.valid === true, 'Draft should soft-pass without clause');
		console.assert(res.post?.reviewerNote === 'Validator: No concrete clause', 'Reviewer note should be attached');
	})();

	console.log('inline tests ok');
}
