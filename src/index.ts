import { ingestDynamicDiscovery, type DiscoveryEnv } from './orchestrator';
import { type LLMEnv } from './providers/llm';
import { selectPmStories, type ShortlistedCandidate } from './pm/select';
import { composePmPost, type PmPost } from './pm/compose';

interface Env extends DiscoveryEnv, LLMEnv {
	PM_MAX_POSTS?: string;
}

export default {
	async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(orchestrate(env).then(() => {}));
	},

	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/run') {
			return json(await orchestrate(env));
		}

		if (url.pathname === '/preview') {
			const discovery = await ingestDynamicDiscovery(env);
			const shortlist = selectPmStories(discovery.allItems, resolveLimit(env, 12));
			return json({
				stats: discovery.stats,
				providerCounts: discovery.providerCounts,
				shortlist: shortlist.map(({ item, pmScore, indiaScore, signals, urgency }) => ({
					title: item.title,
					url: item.url,
					domain: item.domain,
					eventClass: item.eventClass,
					impactScore: item.impactScore,
					pmScore,
					indiaScore,
					signals,
					urgency,
					publishedAt: item.publishedAt,
				})),
			});
		}

		return new Response('OK', { status: 200 });
	},
};

async function orchestrate(env: Env): Promise<{
	startedAt: string;
	scanned: number;
	providerCounts: Record<string, number>;
	posts: PmPost[];
	generationNotes: { shortlisted: number; limit: number };
}> {
	const startedAt = new Date().toISOString();
	const discovery = await ingestDynamicDiscovery(env);
	const limit = resolveLimit(env, 10);
	const shortlisted = selectPmStories(discovery.allItems, limit);
	const posts: PmPost[] = [];

	for (const candidate of shortlisted) {
		const post = await composePmPost(env, candidate);
		posts.push(post);
	}

	return {
		startedAt,
		scanned: discovery.stats.scanned,
		providerCounts: discovery.providerCounts,
		posts,
		generationNotes: { shortlisted: shortlisted.length, limit },
	};
}

function resolveLimit(env: Env, fallback: number): number {
	const raw = env.PM_MAX_POSTS ? parseInt(env.PM_MAX_POSTS, 10) : NaN;
	if (Number.isFinite(raw) && raw > 0) {
		return clamp(raw, 3, 25);
	}
	return fallback;
}

function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data, null, 2), {
		status,
		headers: { 'content-type': 'application/json' },
	});
}

function clamp(value: number, min: number, max: number): number {
	if (Number.isNaN(value)) return min;
	return Math.max(min, Math.min(max, value));
}
