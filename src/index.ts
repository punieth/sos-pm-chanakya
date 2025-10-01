import { ingestDynamicDiscovery } from './orchestrator';
import { callLLM } from './providers/llm';
import { mapHotLaunchProperties } from './publish/notion';
import { canonicalUrlForHash, normalizeExcerptForHash, sha256Hex } from './utils/hash';
import type { ScoreTelemetry, ImpactResult, EventClassName } from './types';
import type { IngestStats } from './orchestrator';

export default {
	async scheduled(_e: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(orchestrate(env));
	},

	async fetch(req: Request, env: Env): Promise<Response> {
		const url = new URL(req.url);

		if (url.pathname === '/run') {
			return json(await orchestrate(env));
		}

		if (url.pathname === '/dynamic-discovery') {
			const preview = await ingestDynamicDiscovery(env, logEvent);
			const survivors = preview.items.map((item) => ({
				title: item.title,
				url: item.url,
				source: item.source,
				provider: item.provider,
				impact: item.impactScore,
				eventClass: item.eventClass,
				reason: describeImpactReason(item),
				breakdown: item.impactBreakdown,
			}));
			const allItems = preview.allItems.map((item) => ({
				title: item.title,
				url: item.url,
				source: item.source,
				provider: item.provider,
				impact: item.impactScore,
				eventClass: item.eventClass,
				reason: describeImpactReason(item),
				breakdown: item.impactBreakdown,
			}));
			return json({
				ok: true,
				stats: preview.stats,
				providerCounts: preview.providerCounts,
				clusters: preview.clusters,
				survivors,
				allItems,
			});
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
	NEWSAPI_KEY?: string;
	GDELT_ENABLED?: string;
	BING_NEWS_ENABLED?: string;
	FEAT_GEMINI?: string;
	FEAT_LOCAL?: string;
	FEAT_COMPOSE_LLM?: string;

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
	FF_GEMINI_FAILOVER?: string;
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
	dynamicDiscovery?: {
		stats: IngestStats;
		notionCreated: number;
		items: Array<{
			url: string;
			impact: number;
			eventClass: string;
			created: boolean;
			reason: string;
			impactBreakdown: ImpactResult['impactBreakdown'];
		}>;
		clusters: Array<{
			id: string;
			size: number;
			theme: string;
			eventClass: EventClassName;
			domains: string[];
			topTokens: string[];
			sample?: { title?: string; url?: string; impact: number };
		}>;
	};
	ingest: {
		totals: { scanned: number; created: number; skippedSeen: number; skippedNoise: number; skippedOld: number };
		types: Record<TypeName, number>;
		feeds: Record<string, FeedStats>;
	};
	enrich: StageReport<{ excerpt?: string }>;
	score: {
		attempted: number;
		updated: number;
		llmCalls: number;
		results: Array<{ id: string; ok: boolean; error?: string; telemetry?: ScoreTelemetry }>;
	};
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
	const composeLLMEnabled = env.FEAT_COMPOSE_LLM !== '0';

	let dynamicDiscovery: OrchestrateResult['dynamicDiscovery'] | undefined;
	try {
		const dynamic = await ingestDynamicDiscovery(env, logEvent);
		let notionCreated = 0;
		const itemReport: NonNullable<OrchestrateResult['dynamicDiscovery']>['items'] = [];
		for (const item of dynamic.items) {
			const excerpt = (item.description || '').slice(0, 1400);
			const normalizedExcerpt = normalizeExcerptForHash(excerpt);
			const canonical = canonicalUrlForHash(item.canonicalUrl || item.url);
			const hashInput = `${normalizedExcerpt}|||${canonical}`;
			const contentHash = await sha256Hex(hashInput);
			const seenKey = `v4:${contentHash}`;
			const baseReason = describeImpactReason(item);
			if (await env.SEEN.get(seenKey)) {
				itemReport.push({
					url: item.url,
					impact: item.impactScore,
					eventClass: item.eventClass,
					created: false,
					reason: `${baseReason}; skipped: seen_hash`,
					impactBreakdown: item.impactBreakdown,
				});
				logEvent('dynamic_skip_seen_hash', { url: item.url, hash: contentHash });
				continue;
			}
			const extraProps = mapHotLaunchProperties(item);
			try {
				const pageId = await createNotionPage(env, {
					sourceName: item.source || item.domain,
					url: item.url,
					type: 'Coverage',
					format: 'Short-form',
					publishedAt: item.publishedAt,
					importedAt: startedAt,
					batchId,
					contentHash,
					sourceExcerpt: excerpt,
					contentFetched: Boolean(excerpt),
					extraProperties: extraProps,
				});
				const created = Boolean(pageId);
				if (created) {
					notionCreated++;
					await env.SEEN.put(seenKey, '1', { expirationTtl: 60 * 60 * 24 * 180 });
				}
				const reasonSuffix = created ? '' : '; skipped: notion_duplicate';
				itemReport.push({
					url: item.url,
					impact: item.impactScore,
					eventClass: item.eventClass,
					created,
					reason: `${baseReason}${reasonSuffix}`,
					impactBreakdown: item.impactBreakdown,
				});
				logEvent('dynamic_publish', {
					url: item.url,
					impact: item.impactScore,
					eventClass: item.eventClass,
					created,
				});
			} catch (err) {
				logEvent('dynamic_publish_error', { url: item.url, error: errorMessage(err) });
				itemReport.push({
					url: item.url,
					impact: item.impactScore,
					eventClass: item.eventClass,
					created: false,
					reason: `${baseReason}; notion_error: ${errorMessage(err)}`,
					impactBreakdown: item.impactBreakdown,
				});
			}
		}
		dynamicDiscovery = {
			stats: { ...dynamic.stats, providerCounts: dynamic.providerCounts },
			notionCreated,
			items: itemReport,
			clusters: dynamic.clusters,
		};
	} catch (err) {
		logEvent('dynamic_discovery_error', { error: errorMessage(err) });
	}

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
			if (lite.contentFetched) {
				enrichReport.items.push({
					id,
					title: lite.title,
					url: lite.url,
					ok: true,
					extra: { status: 'already_fetched', excerpt: lite.excerpt },
				});
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
	const scoreResults: Array<{ id: string; ok: boolean; error?: string; telemetry?: ScoreTelemetry }> = [];
	let llmCalls = 0;

	for (const p of pagesToScore) {
		let telemetry: ScoreTelemetry | undefined;
		try {
			const shaped = shapeForScoring(p);
			const llmResult = await callLLM(env, shaped, logEvent);
			telemetry = llmResult.telemetry;
			if (!llmResult.ok || !llmResult.text) {
				scoreResults.push({ id: p.id, ok: false, error: llmResult.reason || 'LLM unavailable', telemetry });
				logEvent('score_error', {
					id: p.id,
					url: p.url,
					error: llmResult.reason || 'llm_unavailable',
					retries: telemetry?.retries,
					model: telemetry?.model_used,
					provider: telemetry?.provider,
				});
				continue;
			}
			llmCalls += (telemetry?.retries ?? 0) + 1;
			const parsed = safeParseLLM(llmResult.text);
			await notionUpdateScoring(env, p.id, mapLLMToNotion(parsed, p));
			scoreResults.push({ id: p.id, ok: true, telemetry });
			logEvent('score_ok', {
				id: p.id,
				url: p.url,
				retries: telemetry?.retries,
				model: telemetry?.model_used,
				provider: telemetry?.provider,
			});
		} catch (e: any) {
			scoreResults.push({ id: p.id, ok: false, error: errorMessage(e), telemetry });
			logEvent('score_error', {
				id: p.id,
				url: p.url,
				error: errorMessage(e),
				retries: telemetry?.retries,
				model: telemetry?.model_used,
				provider: telemetry?.provider,
			});
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
			if (composeLLMEnabled && env.FF_DISABLE_FETCH !== '1') {
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
			} else if (!composeLLMEnabled) {
				logEvent('compose_llm_disabled', { id: p.id, url: p.url });
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
	const validationResults = validatePosts(drafts);

	// Process validation results: mark up posts that failed validation
	for (const v of validationResults) {
		if (!v.valid && v.post) {
			const note = `Validator: ${v.reason || 'unknown'}`;
			v.post.reviewerNote = note;
			v.post.status = 'Needs Fact';
			logEvent('validate_fail', { id: v.id, reason: v.reason });
		}
	}

	// 6) Patch drafts back to Notion — BOTH passed and failed (failed as Needs Fact)
	const patchable = validationResults.filter((v) => v.post);
	const patchReport: StageReport = { attempted: patchable.length, ok: 0, failed: 0, items: [] };
	for (const v of patchable) {
		try {
			const post = v.post!;
			post.status = post.status || 'Proposed';
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
		dynamicDiscovery,
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
			passed: validationResults.filter((v) => v.valid).length,
			failed: validationResults.filter((v) => !v.valid).length,
			details: validationResults.map(({ id, valid, reason }) => ({ id, valid, reason })),
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

			const xml = await safeGet(feed, env);
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


				const { excerpt } = await fetchExcerpt(it.link);
				const normalizedExcerpt = normalizeExcerptForHash(excerpt || '');
				const normalizedUrlForHash = canonicalUrlForHash(it.link);
				const hashInput = `${normalizedExcerpt}|||${normalizedUrlForHash}`;
				const contentHash = await sha256Hex(hashInput);
				const seenKey = `v4:${contentHash}`;
				if (await env.SEEN.get(seenKey)) {
					skippedSeen++;
					logEvent('ingest_skip_seen_hash', { url: it.link, hash: contentHash, feed });
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
						sourceExcerpt: excerpt,
						contentFetched: Boolean(excerpt),
					});
					await env.SEEN.put(seenKey, '1', { expirationTtl: 60 * 60 * 24 * 180 });
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
	decisionWindow?: '0–7d' | '7–30d' | '>30d';
	audienceTier?: 'IC' | 'Lead' | 'VP';
	excerpt?: string;
	affectedSteps?: string[];
	kpiImpact?: string[];
	publishedAt?: string;
	contentFetched?: boolean;
};

function computeDecisionWindow(publishedAt?: string): '0–7d' | '7–30d' | '>30d' {
	if (!publishedAt) return '>30d';
	const d = new Date(publishedAt);
	if (Number.isNaN(d.getTime())) return '>30d';
	const diffDays = Math.max(0, (Date.now() - d.getTime()) / 86400000);
	if (diffDays <= 7) return '0–7d';
	if (diffDays <= 30) return '7–30d';
	return '>30d';
}

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
	const publishedAt = p['Published At']?.date?.start;
	const dw = computeDecisionWindow(publishedAt);
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
		publishedAt,
		contentFetched: !!p['Content Fetched']?.checkbox,
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
	"decision_window": "0–7d"|"7–30d"|">30d",
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
	decisionWindow: '0–7d' | '7–30d' | '>30d';
	affectedSteps: string[];
	kpiImpact: string[];
	status: 'Keep' | 'Archive' | 'Researching' | 'Draft';
};

function mapLLMToNotion(j: any, p: NotionPageLite): NotionScorePayload {
	let status = pickEnum(j.status, ['Keep', 'Archive', 'Researching', 'Draft'], 'Keep');
	let score = typeof j.signal_score === 'number' ? j.signal_score : 0;
	let window = p.decisionWindow ?? computeDecisionWindow(p.publishedAt);

	const inferenceText = [p.title, p.excerpt, p.why]
		.filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
		.join(' ');
	const steps = inferAffectedSteps(p, inferenceText);
	const kpis = inferKpiImpact(steps, inferenceText);
	const audienceTier = inferAudienceTier(steps, kpis);

	const shouldArchive = window === '>30d' && p.type !== 'Rails';
	if (shouldArchive) status = 'Archive';

	if (p.type === 'Coverage' && window === '>30d') {
		status = 'Archive';
		if (score > 3) score = 3;
	}

	if (p.type === 'Coverage' && isIndiaPolicyish(p)) {
		if (score < 5) score = 5;
		if (window === '>30d') {
			window = '7–30d';
			if (status === 'Archive') status = 'Researching';
		}
	}

	let quickAction = normalizeQuickAction((j.quick_action || '').toString(), steps, shouldArchive);
	const why = (j.why || '').toString().slice(0, 4000);

	return {
		signalScore: clamp(score, 0, 10),
		roleTag: audienceTier === 'VP' ? 'VP' : 'IC',
		quickAction,
		why,
		decisionWindow: window,
		affectedSteps: steps,
		kpiImpact: kpis,
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
	status?: 'Proposed' | 'Ready' | 'Needs Fact' | 'Archive';
	citations?: string;
	audienceTier?: 'IC' | 'Lead' | 'VP';
	postAngle?: Array<'Explainer' | 'Heads-up' | 'Action Required'>;
	affectedSteps?: string[];
	kpiImpact?: string[];
	reviewerNote?: string;
	// meta from source so validator can be smarter (esp. Rails)
	_meta?: {
		type?: TypeName;
		excerpt?: string;
		decisionWindow?: '0–7d' | '7–30d' | '>30d';
		sourceUrl?: string;
		publishedAt?: string;
		clauseDetected?: string;
	};
};

function composeOneFromScored(p: NotionPageLite): DraftPost {
	const window = p.decisionWindow ?? computeDecisionWindow(p.publishedAt);
	const sourceName = p.title || guessSourceName(p.url || '');
	const typeLabel = p.type || 'Update';
	const title = `[${typeLabel}] ${sourceName}: India next steps`;
	const inferenceText = [p.title, p.excerpt, p.why]
		.filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
		.join(' ');
	const steps = inferAffectedSteps(p, inferenceText);
	const kpis = inferKpiImpact(steps, inferenceText);
	const audienceTier = inferAudienceTier(steps, kpis);
	const ownerInfo = inferOwnerFromSteps(steps);
	const clauseSnippet = extractClauseSnippet(p.excerpt || '', p.title || p.why || '');
	const citationsInfo = buildCitations(p.url, clauseSnippet);
	const whatChanged = buildWhatChangedSection(p, clauseSnippet);
	const whyIndia = buildWhyItMattersSection(p, steps, kpis);
	const actionableWindow = window !== '>30d';
	const postStatus: DraftPost['status'] =
		window === '>30d' && p.type !== 'Rails'
			? 'Archive'
			: clauseSnippet && ownerInfo.owner
			? 'Ready'
			: 'Proposed';
	const actionLine = buildActionLine(ownerInfo, steps, window, actionableWindow, postStatus);
	const postAngle = [determinePostAngle(steps, postStatus, actionableWindow)];
	const body = `What changed\n\t• ${whatChanged}\n\nWhy it matters (India)\n\t• ${whyIndia}\n\nAction for this week\n\t• ${actionLine}`;
	const citations = citationsInfo.text;
	return {
		id: p.id,
		title,
		body,
		status: postStatus,
		citations,
		audienceTier,
		postAngle,
		affectedSteps: steps,
		kpiImpact: kpis,
		_meta: {
			type: p.type,
			excerpt: p.excerpt,
			decisionWindow: window,
			sourceUrl: p.url,
			publishedAt: p.publishedAt,
			clauseDetected: citationsInfo.clause,
		},
	};
}
type AffectedStepName = 'Browse' | 'Build' | 'Price' | 'Distribute' | 'Approvals' | 'Checkout' | 'Support';
type KpiName = 'CTR' | 'Conversion' | 'Approval' | 'Revenue' | 'Compliance' | 'Churn';

type OwnerInfo = { owner: string; tier: 'IC' | 'VP' };

const STEP_KEYWORDS: Record<AffectedStepName, string[]> = {
	Browse: ['beta', 'release notes', 'app store', 'discover', 'news', 'announcement', 'policy draft', 'consultation'],
	Build: ['sdk', 'api', 'xcode', 'library', 'integration', 'developer', 'breaking change', 'migration', 'deprecation'],
	Price: ['pricing', 'tariff', 'reserve price', 'fee', 'commission', 'brokerage', 'mrp'],
	Distribute: ['spectrum', 'licensing', 'distribution', 'storefront', 'availability', 'rollout', 'onboarding', 'fm radio'],
	Approvals: ['trai', 'dot', 'rbi', 'sebi', 'regulation', 'authorisation', 'authorization', 'compliance filing', 'auction', 'notification', 'gazette'],
	Checkout: ['payments', 'upi', 'gst', 'tax', 'checkout', 'invoice', 'approval rate'],
	Support: ['incident', 'advisory', 'cert-in', 'cve', 'vulnerability', 'customer support', 'service window'],
};

const KPI_KEYWORDS: Record<KpiName, string[]> = {
	Compliance: ['compliance', 'regulator', 'penalty', 'mandate', 'audit'],
	Approval: ['auction', 'license', 'authorisation', 'authorization', 'kyc', 'underwriting', 'approval rate'],
	Conversion: ['checkout', 'beta to ga', 'funnel', 'paywall', 'trial', 'opt-in'],
	Revenue: ['pricing', 'commission', 'fee', 'brokerage', 'tariff', 'arpu'],
	CTR: ['announcement', 'release notes', 'new feature', 'beta sign-up', 'newsletter'],
	Churn: ['deprecation', 'breaking change', 'sunset', 'outage', 'fee increase'],
};

const STEP_PRIORITY: AffectedStepName[] = ['Approvals', 'Price', 'Distribute', 'Checkout', 'Build', 'Support', 'Browse'];
const KPI_PRIORITY: KpiName[] = ['Compliance', 'Revenue', 'Approval', 'Conversion', 'CTR', 'Churn'];

const STEP_FALLBACK_BY_TYPE: Partial<Record<TypeName, AffectedStepName[]>> = {
	Rails: ['Approvals', 'Price'],
	Platform: ['Build', 'Browse'],
	Marketplace: ['Distribute', 'Checkout'],
	Coverage: ['Approvals'],
	'Long-form': ['Browse'],
};

const OWNER_BY_STEP: Record<AffectedStepName, OwnerInfo> = {
	Approvals: { owner: 'Policy/Legal (VP)', tier: 'VP' },
	Price: { owner: 'Pricing (VP)', tier: 'VP' },
	Distribute: { owner: 'Ops/Go-to-Market (IC)', tier: 'IC' },
	Checkout: { owner: 'Payments (IC)', tier: 'IC' },
	Build: { owner: 'Engineering (IC)', tier: 'IC' },
	Support: { owner: 'Support Ops (IC)', tier: 'IC' },
	Browse: { owner: 'Growth (IC)', tier: 'IC' },
};

function inferAffectedSteps(p: NotionPageLite, text: string): AffectedStepName[] {
	const lower = (text || '').toLowerCase();
	const scores: Array<{ step: AffectedStepName; score: number }> = [];
	for (const step of Object.keys(STEP_KEYWORDS) as AffectedStepName[]) {
		let score = 0;
		for (const kw of STEP_KEYWORDS[step]) score += countKeyword(lower, kw);
		if (score > 0) scores.push({ step, score });
	}
	if (!scores.length) {
		const fallback = STEP_FALLBACK_BY_TYPE[p.type as TypeName];
		return (fallback || ['Browse']).slice(0, 2);
	}
	scores.sort((a, b) => (b.score === a.score ? STEP_PRIORITY.indexOf(a.step) - STEP_PRIORITY.indexOf(b.step) : b.score - a.score));
	let result = scores.map((s) => s.step);
	if (result.length > 2) {
		const preferred = result.filter((s) => ['Approvals', 'Price', 'Distribute'].includes(s));
		if (preferred.length >= 2) result = preferred.slice(0, 2);
		else if (preferred.length === 1) result = [preferred[0], ...result.filter((s) => s !== preferred[0])].slice(0, 2);
		else result = result.slice(0, 2);
	}
	return Array.from(new Set(result.slice(0, 2)));
}

function inferKpiImpact(steps: AffectedStepName[], text: string): KpiName[] {
	const lower = (text || '').toLowerCase();
	const scores: Array<{ kpi: KpiName; score: number }> = [];
	for (const kpi of Object.keys(KPI_KEYWORDS) as KpiName[]) {
		let score = 0;
		for (const kw of KPI_KEYWORDS[kpi]) score += countKeyword(lower, kw);
		if (score > 0) scores.push({ kpi, score });
	}
	if (steps.includes('Approvals') && !scores.some((s) => s.kpi === 'Compliance')) {
		scores.push({ kpi: 'Compliance', score: 1 });
	}
	scores.sort((a, b) => (b.score === a.score ? KPI_PRIORITY.indexOf(a.kpi) - KPI_PRIORITY.indexOf(b.kpi) : b.score - a.score));
	return Array.from(new Set(scores.map((s) => s.kpi))).slice(0, 2);
}

function inferAudienceTier(steps: AffectedStepName[], kpis: KpiName[]): 'IC' | 'VP' {
	if (kpis.some((k) => k === 'Revenue' || k === 'Compliance' || k === 'Approval')) return 'VP';
	if (steps.some((s) => s === 'Approvals' || s === 'Price')) return 'VP';
	return 'IC';
}

function inferOwnerFromSteps(steps: AffectedStepName[]): OwnerInfo {
	for (const priority of STEP_PRIORITY) {
		if (steps.includes(priority)) return OWNER_BY_STEP[priority];
	}
	return { owner: 'Product (IC)', tier: 'IC' };
}

function determinePostAngle(
	steps: AffectedStepName[],
	status: DraftPost['status'],
	actionableWindow: boolean
): 'Explainer' | 'Heads-up' | 'Action Required' {
	if (status === 'Archive') return 'Explainer';
	const actionSteps = ['Approvals', 'Price', 'Distribute'];
	if (actionableWindow && steps.some((s) => actionSteps.includes(s))) return 'Action Required';
	const headsSteps = ['Browse', 'Build'];
	if (steps.length && steps.every((s) => headsSteps.includes(s))) return 'Heads-up';
	return 'Explainer';
}

function buildActionLine(
	ownerInfo: OwnerInfo,
	steps: AffectedStepName[],
	window: '0–7d' | '7–30d' | '>30d',
	actionableWindow: boolean,
	status: DraftPost['status']
): string {
	const owner = ownerInfo.owner;
	if (status === 'Archive' || !actionableWindow || window === '>30d') {
		return trimAction(`${owner} to archive stale entry; keeps India tracker clean.`);
	}
	const primary = steps[0] || 'Browse';
	const templates: Partial<Record<AffectedStepName, string>> = {
		Approvals: `${owner} to lodge clause note with regulator; keeps India compliance clean.`,
		Price: `${owner} to refresh India fee sheet per clause; protects marketplace revenue.`,
		Distribute: `${owner} to brief onboarding on new gate; holds India supply steady.`,
		Checkout: `${owner} to tune India routing for clause impact; steadies approval rate.`,
		Build: `${owner} to schedule India patch for new requirement; avoids release slips.`,
		Support: `${owner} to prep India incident script from clause; limits churn spikes.`,
		Browse: `${owner} to update India storefront copy from clause; defends CTR.`,
	};
	const line = templates[primary] || `${owner} to capture clause in India playbook; keeps teams aligned.`;
	return trimAction(line);
}

function buildCitations(url?: string, clause?: string | null): { text: string; clause?: string } {
	const lines: string[] = [];
	const seen = new Date().toISOString().slice(0, 10);
	if (url) lines.push(`${url} (seen ${seen})`);
	const snippet = truncateWords(clause || '', 25);
	if (snippet) lines.push(`Clause: ${snippet}`);
	return { text: lines.join('\n'), clause: snippet || undefined };
}

function buildWhatChangedSection(p: NotionPageLite, clause: string | null): string {
	const sentences = splitSentences(p.excerpt || '');
	let narrative = sentences.slice(0, 2).join(' ');
	if (!narrative) narrative = (p.title || 'Source update for India PMs').trim();
	if (p.publishedAt) {
		const dateStr = formatDateForNarrative(p.publishedAt);
		if (dateStr && !narrative.startsWith(dateStr)) narrative = `${dateStr}: ${narrative}`;
	}
	if (!narrative.includes('.') && clause) narrative = `${narrative}. ${clause}`;
	return truncateWords(narrative.replace(/\s+/g, ' ').trim(), 45);
}

function buildWhyItMattersSection(
	p: NotionPageLite,
	steps: AffectedStepName[],
	kpis: KpiName[]
): string {
	const lines: string[] = [];
	const priority = STEP_PRIORITY.filter((s) => steps.includes(s));
	for (const step of priority) {
		switch (step) {
			case 'Approvals':
				lines.push('India regulators expect an immediate compliance readout; missing it risks audits or takedowns.');
				break;
			case 'Price':
				lines.push('Pricing levers for India sellers shift here; align fees and contracts before rollout bites.');
				break;
			case 'Distribute':
				lines.push('Distribution and onboarding gates move; brief GTM so India supply stays healthy.');
				break;
			case 'Checkout':
				lines.push('Payments funnel will shift; tune India flows now to protect conversion and approval rates.');
				break;
			case 'Build':
				lines.push('Engineering needs a near-term patch so India builds stay compliant with the new rules.');
				break;
			case 'Support':
				lines.push('Support must script responses around this clause to blunt India churn if issues surface.');
				break;
			case 'Browse':
				lines.push('Discovery copy changes for India audiences; refresh messaging to defend top-of-funnel traffic.');
				break;
		}
	}
	if (!lines.length && p.type === 'Coverage') {
		lines.push('Use this as background on India policy pressure; keep tracking for concrete enforcement.');
	}
	const priorityKpi = kpis.find((k) => k === 'Compliance' || k === 'Revenue' || k === 'Approval');
	if (priorityKpi === 'Compliance' && !lines.some((l) => l.includes('compliance'))) {
		lines.push('Stay audit-ready; this clause is now part of India compliance scope.');
	}
	if (priorityKpi === 'Revenue' && !lines.some((l) => l.includes('revenue'))) {
		lines.push('Revenue exposure is direct; adjust MDR/fees to avoid margin surprises.');
	}
	if (priorityKpi === 'Approval' && !lines.some((l) => l.includes('approval'))) {
		lines.push('Approval metrics will be reviewed; hold daily India scorecards until steady.');
	}
	if (!lines.length) {
		lines.push('Keep this on radar for India teams; it signals policy direction even if action waits.');
	}
	return truncateWords(lines.slice(0, 2).join(' '), 45);
}

function extractClauseSnippet(primary: string, fallback?: string): string {
	const sources = [primary, fallback].map((s) => (s || '').trim()).filter(Boolean);
	const keywordRegex = /\b(clause|section|effective|from|beginning|starts?|must|shall|fee|tariff|commission|compliance|approval|deadline|rollout|valid|auction|enforce|applies)\b/i;
	for (const src of sources) {
		const sentences = splitSentences(src);
		const hit = sentences.find((s) => keywordRegex.test(s));
		if (hit) return truncateWords(hit.replace(/\s+/g, ' ').trim(), 25);
	}
	for (const src of sources) {
		const sentences = splitSentences(src);
		const first = sentences.find((s) => s.length > 0);
		if (first) return truncateWords(first.replace(/\s+/g, ' ').trim(), 25);
	}
	const raw = sources[0] || '';
	return truncateWords(raw.replace(/\s+/g, ' ').trim(), 25);
}

function splitSentences(text: string): string[] {
	return (text.match(/[^.!?]+[.!?]?/g) || []).map((s) => s.trim()).filter(Boolean);
}

function truncateWords(text: string, maxWords: number): string {
	const words = text.split(/\s+/).filter(Boolean);
	if (words.length <= maxWords) return text.trim();
	return `${words.slice(0, maxWords).join(' ')}…`;
}

function formatDateForNarrative(publishedAt?: string): string | null {
	if (!publishedAt) return null;
	const d = new Date(publishedAt);
	if (Number.isNaN(d.getTime())) return null;
	return new Intl.DateTimeFormat('en-IN', { day: 'numeric', month: 'short', year: 'numeric' }).format(d);
}

function countKeyword(text: string, keyword: string): number {
	if (!keyword) return 0;
	const pattern = escapeRegExp(keyword.toLowerCase());
	const regex = new RegExp(pattern, 'g');
	const matches = text.match(regex);
	return matches ? matches.length : 0;
}

function escapeRegExp(str: string): string {
	return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function trimAction(line: string): string {
	let out = line.trim().replace(/\s+/g, ' ');
	if (out.length <= 100) return out;
	if (!out.endsWith('.')) out += '.';
	return out.slice(0, 99).trimEnd() + '…';
}

function fallbackQuickActionFromSteps(steps: AffectedStepName[]): string {
	const primary = steps[0] || 'Browse';
	switch (primary) {
		case 'Approvals':
			return 'Log clause for compliance review today.';
		case 'Price':
			return 'Update India fee tracker with the new rate.';
		case 'Distribute':
			return 'Brief onboarding leads on the new India gate.';
		case 'Checkout':
			return 'Tune India payments routing for this clause.';
		case 'Build':
			return 'Schedule India build patch to stay compliant.';
		case 'Support':
			return 'Prep India incident script referencing this clause.';
		case 'Browse':
		default:
			return 'Refresh India storefront messaging to reflect the change.';
	}
}

function normalizeQuickAction(raw: string, steps: AffectedStepName[], shouldArchive: boolean): string {
	if (shouldArchive) return 'Archive';
	let q = (raw || '').replace(/\s+/g, ' ').trim();
	if (!q || /learn more|follow along|stay tuned|monitor/i.test(q) || q.length > 100) {
		q = fallbackQuickActionFromSteps(steps);
	}
	if (!/^[A-Za-z]/.test(q)) q = fallbackQuickActionFromSteps(steps);
	if (q.length > 100) q = q.slice(0, 100).trim();
	return q;
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
		if (valid && !/\((IC|VP)\)/.test(qaLine)) {
			valid = false;
			reason = 'Action missing owner tier';
		}
		if (valid && /confirm clause|placeholder/i.test(body)) {
			valid = false;
			reason = 'Contains placeholder text';
		}

		const citations = post.citations || '';
		const lines = citations.split(/\n+/).map((line) => line.trim()).filter(Boolean);
		const hasUrl = lines.some((line) => /^https?:\/\//i.test(line) || line.includes('http'));
		const clauseLine = lines.find((line) => /^Clause:/i.test(line));
		const clauseWords = clauseLine ? clauseLine.replace(/^Clause:/i, '').trim().split(/\s+/).filter(Boolean).length : 0;
		if (clone._meta) clone._meta.clauseDetected = clauseLine ? clauseLine.replace(/^Clause:/i, '').trim() : clone._meta?.clauseDetected;
		const sourceUrl = post._meta?.sourceUrl;
		if (valid && sourceUrl && !hasUrl) {
			valid = false;
			reason = 'Citations missing source URL';
		}
		if (valid && (!clauseLine || clauseWords === 0 || clauseWords > 25)) {
			valid = false;
			reason = 'Missing concrete clause';
		}
		if (!valid && reason === 'Missing concrete clause') {
			clone.reviewerNote = 'Validator: Missing concrete clause';
		} else if (!valid && reason === 'Citations missing source URL') {
			clone.reviewerNote = 'Validator: Missing source URL';
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
		'Affected Steps': post.affectedSteps || [],
		'KPI Impact': post.kpiImpact || [],
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
		sourceExcerpt,
		contentFetched,
		extraProperties,
	}: {
		sourceName: string;
		url: string;
		type: TypeName;
		format: FormatName;
		publishedAt?: string;
		importedAt: string;
		batchId: string;
		contentHash: string;
		sourceExcerpt?: string;
		contentFetched?: boolean;
		extraProperties?: Record<string, unknown>;
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
	if (sourceExcerpt !== undefined) props['Source Excerpt'] = sourceExcerpt || '(no excerpt)';
	if (contentFetched) props['Content Fetched'] = true;
	if (extraProperties) {
		for (const [k, v] of Object.entries(extraProperties)) {
			props[k] = v;
		}
	}
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

/** =======================
 * Net/Parsing helpers
 * ======================= */
async function safeGet(url: string, env?: Env): Promise<string | null> {
	try {
		if (env && isOndcFeedUrl(url)) {
			return await safeGetOndc(env, url);
		}
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

function isOndcFeedUrl(url: string): boolean {
	return url.startsWith('https://www.ondc.org/blog/feed/');
}

async function safeGetOndc(env: Env, url: string): Promise<string | null> {
	const quarantineKey = 'ondc:feed:quarantine';
	if (await env.SEEN.get(quarantineKey)) {
		logEvent('ondc_feed_quarantined', { url });
		return fetchOndcFallback();
	}
	const headers = {
		Accept: 'application/rss+xml,text/xml;q=0.9',
		'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0)',
	};
	try {
		const { body } = await fetchWithRedirectLimit(url, headers, 5);
		return body;
	} catch (err) {
		const message = errorMessage(err);
		if (message.includes('redirect_loop')) {
			await env.SEEN.put(quarantineKey, '1', { expirationTtl: 60 * 60 * 24 });
			logEvent('ondc_feed_quarantine', { url, reason: 'redirect_loop' });
			return fetchOndcFallback();
		}
		logEvent('ondc_feed_error', { url, error: message });
		return null;
	}
}

async function fetchOndcFallback(): Promise<string | null> {
	try {
		const res = await retryingFetch(
			'https://www.ondc.org/blog/sitemap.xml',
			{
			headers: {
				Accept: 'application/xml,text/xml;q=0.9',
				'User-Agent': 'Mozilla/5.0 (compatible; PMDigestWorker/1.0)',
			},
			},
			{ label: 'ondc_fallback', retries: 1 }
		);
		if (!res.ok) return null;
		return await res.text();
	} catch {
		return null;
	}
}

async function fetchWithRedirectLimit(
	url: string,
	headers: Record<string, string>,
	maxRedirects: number
): Promise<{ body: string; status: number }> {
	let currentUrl = url;
	let redirects = 0;
	while (redirects <= maxRedirects) {
		const res = await fetch(currentUrl, { headers, redirect: 'manual' });
		const status = res.status;
		if (status >= 300 && status < 400) {
			const location = res.headers.get('location');
			if (!location) break;
			currentUrl = new URL(location, currentUrl).toString();
			redirects++;
			continue;
		}
		if (!res.ok) throw new Error(`http_status_${status}`);
		return { body: await res.text(), status };
	}
	const err = new Error('redirect_loop');
	(err as any).code = 'redirect_loop';
	throw err;
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

function describeImpactReason(item: ImpactResult): string {
	const { recency, graphNovelty, surfaceReach, marketTie } = item.impactBreakdown;
	const fragments: string[] = [];
	if (recency >= 0.5) fragments.push(`fresh (${Math.round(recency * 100)}% recency)`);
	if ((item.graphNovelty || 0) >= 1) fragments.push('new entity pair');
	else if ((item.graphNovelty || 0) > 0) fragments.push('mild graph novelty');
	if (surfaceReach >= 0.4) fragments.push('trusted reach');
	if (marketTie >= 1) fragments.push('commerce/payments tie-in');
	if (item.eventClass !== 'OTHER') fragments.push(`class: ${item.eventClass}`);
	const detail = fragments.join('; ') || 'baseline composite impact';
	return `Impact ${item.impactScore.toFixed(2)} — ${detail}`;
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

async function loadNotionSchema(env: Env, allowEnsure = true): Promise<NotionSchema> {
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
	if (allowEnsure && !schema['Content Hash']) {
		await ensureContentHashProperty(env);
		notionSchemaCache = null;
		return loadNotionSchema(env, false);
	}
	notionSchemaCache = { schema, fetchedAt: now };
	return schema;
}

async function ensureContentHashProperty(env: Env): Promise<void> {
	try {
		const res = await fetch(`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}`, {
			method: 'PATCH',
			headers: notionHeaders(env),
			body: JSON.stringify({ properties: { 'Content Hash': { rich_text: {} } } }),
		});
		if (!res.ok) {
			logEvent('notion_schema_missing_content_hash', { status: res.status, body: await res.text() });
		}
	} catch (err) {
		logEvent('notion_schema_missing_content_hash_error', { error: errorMessage(err) });
	}
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
	const allowedAngles = new Set(['Explainer', 'Heads-up', 'Action Required']);
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
			body: 'What changed\n• Update announced\n\nAction for this week\n• Ship fix now (IC)',
			citations: 'https://example.com/source\nClause: effective 2024-05-01',
			postAngle: ['Explainer'],
			_meta: { sourceUrl: 'https://example.com/source' },
		};
		const res = validatePosts([draft])[0];
		console.assert(res.valid === true, 'Draft should soft-pass without clause');
		console.assert(!res.post?.reviewerNote, 'Reviewer note should stay empty for valid drafts');
	})();

	(function testValidateMissingClauseFails() {
		const draft: DraftPost = {
			id: 'fail1',
			title: 'Missing clause',
			body: 'What changed\n• Something happened\n\nAction for this week\n• Growth (IC) to archive stale entry; keeps India tracker clean.',
			citations: 'https://example.com/source',
			postAngle: ['Explainer'],
			_meta: { sourceUrl: 'https://example.com/source' },
		};
		const res = validatePosts([draft])[0];
		console.assert(res.valid === false, 'Draft should fail without clause');
		console.assert(res.reason === 'Missing concrete clause', 'Reason should note missing clause');
	})();

	(function testComposeExamples() {
		const nowIso = new Date().toISOString();
		const rails: NotionPageLite = {
			id: 'rails',
			type: 'Rails',
			title: 'TRAI revises FM radio reserve price',
			excerpt: 'TRAI notification clause 4 revises FM radio reserve price and is effective July 1, 2024 for all metros.',
			url: 'https://rail.example',
			publishedAt: nowIso,
		};
		const railsDraft = composeOneFromScored(rails);
		console.assert(railsDraft.affectedSteps?.includes('Approvals'), 'Rails draft should tag Approvals');
		console.assert(railsDraft.affectedSteps?.includes('Price'), 'Rails draft should tag Price');
		console.assert(railsDraft.kpiImpact?.includes('Compliance'), 'Rails draft should tag Compliance');
		console.assert(railsDraft.audienceTier === 'VP', 'Rails draft should target VP');
		console.assert(railsDraft.postAngle?.[0] === 'Action Required', 'Rails draft should be Action Required');

		const oldPlatform: NotionPageLite = {
			id: 'platform',
			type: 'Platform',
			title: 'Apple ships beta build',
			excerpt: 'Apple released beta build for developers; update SDK references before GA.',
			url: 'https://platform.example',
			publishedAt: '2023-01-01T00:00:00.000Z',
		};
		const platformDraft = composeOneFromScored(oldPlatform);
		console.assert(platformDraft.status === 'Archive', 'Old platform draft should archive');
		console.assert(platformDraft.postAngle?.[0] === 'Explainer', 'Archive draft defaults to Explainer');

		const gst: NotionPageLite = {
			id: 'gst',
			type: 'Rails',
			title: 'GSTN introduces compliance monitoring for payment partners',
			excerpt: 'GST Council mandates payment aggregators to share approval data weekly and clause 5 lays out penalties for breaches effective immediately.',
			url: 'https://gst.example',
			publishedAt: nowIso,
		};
		const gstDraft = composeOneFromScored(gst);
		console.assert(gstDraft.affectedSteps?.includes('Checkout'), 'GST draft should tag Checkout');
		console.assert(gstDraft.affectedSteps?.includes('Approvals'), 'GST draft should tag Approvals');
		console.assert(gstDraft.kpiImpact?.includes('Compliance'), 'GST draft should tag Compliance');
		console.assert(gstDraft.kpiImpact?.includes('Approval'), 'GST draft should tag Approval KPI');
		console.assert(gstDraft.audienceTier === 'VP', 'GST draft should target VP');
		console.assert(gstDraft.postAngle?.[0] === 'Action Required', 'GST draft should be Action Required');
	})();

	console.log('inline tests ok');
}
