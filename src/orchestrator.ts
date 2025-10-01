import { ingestDynamicSources, dedupeByCanonicalUrl } from './ingest/broker';
import { ingestRegistryFeeds, RegistryEnv } from './ingest/registry';
import { ImpactResult, SourceProvider } from './types';
import { runAnalysisPipeline, ClusterSummary, DEFAULT_PROVIDER_COUNTS } from './analysis/pipeline';

const IMPACT_THRESHOLD = 0;

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

export interface DiscoveryEnv extends RegistryEnv {
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
	const dynamicItems = await ingestDynamicSources(env, logger);
	const registryItems = await ingestRegistryFeeds(env, logger);
	const combined = dedupeByCanonicalUrl([...dynamicItems, ...registryItems]);
	if (combined.length === 0) {
		return {
			items: [],
			allItems: [],
			stats: { scanned: 0, classified: 0, clusters: 0, impactQualified: 0, published: 0, noveltyHits: 0, providerCounts: {} },
			providerCounts: { ...DEFAULT_PROVIDER_COUNTS },
			clusters: [],
		};
	}

	const analysis = await runAnalysisPipeline(combined, { kv: env.SEEN, logger });
	const survivors = analysis.reranked.filter((item) => item.impactScore >= IMPACT_THRESHOLD);

	logger('ingest_dynamic_summary', {
		scanned: combined.length,
		classified: analysis.classified.length,
		clusters: analysis.clusterSummaries.length,
		impactQualified: analysis.impacted.length,
		published: survivors.length,
		noveltyHits: analysis.stats.noveltyHits,
		dynamic: dynamicItems.length,
		registry: registryItems.length,
	});

	return {
		items: survivors,
		allItems: analysis.reranked,
		stats: {
			scanned: combined.length,
			classified: analysis.classified.length,
			clusters: analysis.clusterSummaries.length,
			impactQualified: analysis.impacted.length,
			published: survivors.length,
			noveltyHits: analysis.stats.noveltyHits,
			providerCounts: analysis.providerCounts,
		},
		providerCounts: analysis.providerCounts,
		clusters: analysis.clusterSummaries,
	};
}
