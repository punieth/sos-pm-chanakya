import { NormalizedItem, ImpactResult, ClassifiedItem, SourceProvider, EventClassName } from '../types';
import { updateEntityGraph } from './entities';
import { applyClassification } from './eventClass';
import { prepareClusters, ClusterPreparation, rerankPrimaries } from './rerank';
import { scoreImpact } from './impact';
import { tokenize } from '../utils/text';

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface PipelineOptions {
	kv: KVNamespace;
	logger?: Logger;
}

export interface PipelineStats {
	scanned: number;
	classified: number;
	clusters: number;
	impactQualified: number;
	published: number;
	noveltyHits: number;
}

export type ProviderCounts = Record<SourceProvider, number>;

export interface PipelineResult {
	classified: ClassifiedItem[];
	impacted: ImpactResult[];
	reranked: ImpactResult[];
	clusterSummaries: ClusterSummary[];
	providerCounts: ProviderCounts;
	stats: PipelineStats;
	noveltyMap: Record<string, boolean>;
}

export type ClusterSummary = {
	id: string;
	size: number;
	theme: string;
	eventClass: EventClassName;
	domains: string[];
	topTokens: string[];
	sample?: { title?: string; url?: string; impact: number };
};

export const DEFAULT_PROVIDER_COUNTS: ProviderCounts = {
	newsapi: 0,
	gdelt: 0,
	'google-rss': 0,
	registry: 0,
};

const SUPPORTED_LANGS = new Set(['en', 'en-us', 'en-gb', 'hi']);

export async function runAnalysisPipeline(
	items: NormalizedItem[],
	{ kv, logger }: PipelineOptions
): Promise<PipelineResult> {
	if (items.length === 0) {
		return {
			classified: [],
			impacted: [],
			reranked: [],
			clusterSummaries: [],
			providerCounts: { ...DEFAULT_PROVIDER_COUNTS },
			stats: { scanned: 0, classified: 0, clusters: 0, impactQualified: 0, published: 0, noveltyHits: 0 },
			noveltyMap: {},
		};
	}

	const loggerFn: Logger = logger || (() => {});
	const filtered = items.filter(isSupportedLanguage);
	const providerCounts = filtered.reduce<ProviderCounts>((acc, item) => {
		const provider = item.provider;
		acc[provider] = (acc[provider] || 0) + 1;
		return acc;
	}, { ...DEFAULT_PROVIDER_COUNTS });

	const graphUpdate = await updateEntityGraph(filtered, { kv, logger: loggerFn });
	const noveltyHits = Object.values(graphUpdate.itemNovelty).filter(Boolean).length;
	const classified = filtered.map((item) => applyClassification(item));
	const prep = prepareClusters(classified);
	const impacted = scoreImpact(classified, {
		graphNovelty: graphUpdate.itemNovelty,
		clusterDomainCounts: prep.clusterDomainCounts,
	});
	const reranked = rerankPrimaries(impacted, prep);
	const clusterSummaries = summarizeClusters(prep, impacted);

	return {
		classified,
		impacted,
		reranked,
		clusterSummaries,
		providerCounts,
		noveltyMap: graphUpdate.itemNovelty,
		stats: {
			scanned: items.length,
			classified: classified.length,
			clusters: Object.keys(prep.clusters).length,
			impactQualified: impacted.length,
			published: reranked.length,
			noveltyHits,
		},
	};
}

function summarizeClusters(prep: ClusterPreparation, impacted: ImpactResult[]): ClusterSummary[] {
	const impactMap = new Map<string, ImpactResult>();
	for (const item of impacted) impactMap.set(item.id, item);
	const summaries = Object.values(prep.clusters).map((cluster) => {
		const size = cluster.items.length;
		const domains = Array.from(new Set(cluster.items.map((i) => i.domain))).slice(0, 5);
		const classCounts = cluster.items.reduce<Record<EventClassName, number>>((acc, item) => {
			acc[item.eventClass] = (acc[item.eventClass] || 0) + 1;
			return acc;
		}, {} as Record<EventClassName, number>);
		const topClass = (Object.entries(classCounts)
			.sort((a, b) => (b[1] as number) - (a[1] as number))[0]?.[0] as EventClassName) || 'TREND';
		const tokenCounts: Record<string, number> = {};
		for (const item of cluster.items) {
			const tokens = tokenize(`${item.title || ''} ${item.description || ''}`);
			for (const token of tokens) {
				if (token.length < 3) continue;
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
			topClass !== 'TREND'
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

function isSupportedLanguage(item: NormalizedItem): boolean {
	const lang = (item.language || '').toLowerCase();
	if (!lang) {
		return /[a-z]/i.test(`${item.title || ''} ${item.description || ''}`);
	}
	return SUPPORTED_LANGS.has(lang);
}