import { ingestDynamicSources } from './ingest/broker';
import { updateEntityGraph } from './analysis/entities';
import { applyClassification } from './analysis/eventClass';
import { prepareClusters, rerankPrimaries } from './analysis/rerank';
import { scoreImpact } from './analysis/impact';
import { ImpactResult, EventClassName, SourceProvider } from './types';
import { tokenize } from './utils/text';

const IMPACT_THRESHOLD = 0.4;

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface IngestStats {
	scanned: number;
	classified: number;
	clusters: number;
	impactQualified: number;
	published: number;
	noveltyHits: number;
	providerCounts?: Partial<Record<SourceProvider, number>>;
}

export interface DynamicDiscoveryResult {
	items: ImpactResult[];
	allItems: ImpactResult[];
	stats: IngestStats;
	providerCounts: Record<SourceProvider, number>;
	clusters: ClusterSummary[];
}

type ClusterSummary = {
	id: string;
	size: number;
	theme: string;
	eventClass: EventClassName;
	domains: string[];
	topTokens: string[];
	sample?: { title?: string; url?: string; impact: number };
};

export interface DiscoveryEnv {
	SEEN: KVNamespace;
	NEWSAPI_KEY?: string;
	NEWSAPI_USER_AGENT?: string;
	GDELT_ENABLED?: string;
	BING_NEWS_ENABLED?: string;
}

const DEFAULT_LOGGER: Logger = () => {};

export async function ingestDynamicDiscovery(
	env: DiscoveryEnv,
	logger: Logger = DEFAULT_LOGGER
): Promise<DynamicDiscoveryResult> {
	const normalized = await ingestDynamicSources(env, logger);
	if (normalized.length === 0) {
		return {
			items: [],
			allItems: [],
			stats: { scanned: 0, classified: 0, clusters: 0, impactQualified: 0, published: 0, noveltyHits: 0, providerCounts: {} },
			providerCounts: { newsapi: 0, gdelt: 0, 'google-rss': 0 },
			clusters: [],
		};
	}

	const providerCounts = normalized.reduce<Record<SourceProvider, number>>((acc, item) => {
		acc[item.provider] = (acc[item.provider] || 0) + 1;
		return acc;
	}, { newsapi: 0, gdelt: 0, 'google-rss': 0 });

	const graphUpdate = await updateEntityGraph(normalized, { kv: env.SEEN, logger });
	const noveltyHits = Object.values(graphUpdate.itemNovelty).filter(Boolean).length;
	const classified = normalized.map((item) => applyClassification(item));
	const prep = prepareClusters(classified);
	const impacted = scoreImpact(classified, {
		graphNovelty: graphUpdate.itemNovelty,
		clusterDomainCounts: prep.clusterDomainCounts,
	});

	const reranked = rerankPrimaries(impacted, prep);
	const survivors = reranked.filter((item) => item.impactScore >= IMPACT_THRESHOLD);
	const clusterSummaries = summarizeClusters(prep, impacted);

	logger('ingest_dynamic_summary', {
		scanned: normalized.length,
		classified: classified.length,
		clusters: Object.keys(prep.clusters).length,
		impactQualified: reranked.length,
		published: survivors.length,
		noveltyHits,
	});

	return {
		items: survivors,
		allItems: reranked,
		stats: {
			scanned: normalized.length,
			classified: classified.length,
			clusters: Object.keys(prep.clusters).length,
			impactQualified: reranked.length,
			published: survivors.length,
			noveltyHits,
			providerCounts,
		},
		providerCounts,
		clusters: clusterSummaries,
	};
}

const CLUSTER_STOPWORDS = new Set([
	'the',
	'and',
	'for',
	'with',
	'from',
	'that',
	'this',
	'into',
	'about',
	'after',
	'before',
	'will',
	'could',
	'2025',
	'2024',
	'2023',
	'news',
	'update',
	'latest',
	'launch',
	'report',
]);

function summarizeClusters(prep: ReturnType<typeof prepareClusters>, impacted: ImpactResult[]): ClusterSummary[] {
	const impactMap = new Map<string, ImpactResult>();
	for (const item of impacted) impactMap.set(item.id, item);
	const summaries = Object.values(prep.clusters).map((cluster) => {
		const size = cluster.items.length;
		const domains = Array.from(new Set(cluster.items.map((i) => i.domain))).slice(0, 5);
		const classCounts = cluster.items.reduce<Record<EventClassName, number>>((acc, item) => {
			acc[item.eventClass] = (acc[item.eventClass] || 0) + 1;
			return acc;
		}, { PARTNERSHIP: 0, PAYMENTS: 0, PLATFORM_POLICY: 0, MODEL_LAUNCH: 0, OTHER: 0 });
		const topClass = (Object.entries(classCounts)
			.sort((a, b) => (b[1] as number) - (a[1] as number))[0]?.[0] as EventClassName) || 'OTHER';
		const tokenCounts: Record<string, number> = {};
		for (const item of cluster.items) {
			const tokens = tokenize(`${item.title || ''} ${item.description || ''}`);
			for (const token of tokens) {
				if (token.length < 3) continue;
				if (CLUSTER_STOPWORDS.has(token)) continue;
				tokenCounts[token] = (tokenCounts[token] || 0) + 1;
			}
		}
		const topTokens = Object.entries(tokenCounts)
			.sort((a, b) => b[1] - a[1])
			.slice(0, 3)
			.map(([token]) => token);
		const representative = cluster.items
			.map((item) => impactMap.get(item.id))
			.filter((nz): nz is ImpactResult => Boolean(nz))
			.sort((a, b) => b.impactScore - a.impactScore)[0];
		const theme =
			topClass !== 'OTHER'
				? topClass
				: topTokens.join(' ') || representative?.domain || 'general';
		return {
			id: cluster.id,
			size,
			theme,
			eventClass: topClass,
			domains,
			topTokens,
			sample: representative
				? { title: representative.title, url: representative.url, impact: representative.impactScore }
				: undefined,
		};
	});
	return summaries.sort((a, b) => (b.sample?.impact || 0) - (a.sample?.impact || 0));
}
