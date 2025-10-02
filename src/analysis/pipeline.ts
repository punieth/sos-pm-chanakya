import { NormalizedItem, ClassifiedItem, ScoredItem, ClusterContext, EventClass, SourceProvider } from '../types';
import { updateEntityGraph } from './entities';
import { scoreEvent, classifyWithEvidence } from './eventClass';
import { buildClusters, rerankMMR } from './rerank';
import { buildClusterSignals, ClusterEnvelope } from './cluster';
import { scoreItems, ImpactComputationContext } from './impact';
import { tokenize } from '../utils/text';

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface PipelineOptions {
  kv: KVNamespace;
  logger?: Logger;
  shortlistSize?: number;
  mmrLambda?: number;
}

export interface PipelineStats {
  scanned: number;
  classified: number;
  clusters: number;
  impactQualified: number;
  shortlisted: number;
  noveltyHits: number;
  classDistribution: Record<EventClass, number>;
}

export type ProviderCounts = Record<SourceProvider, number>;

export interface PipelineResult {
  classified: ClassifiedItem[];
  clusters: ClusterContext[];
  scored: ScoredItem[];
  reranked: ScoredItem[];
  shortlisted: ScoredItem[];
  clusterSignals: NonNullable<ImpactComputationContext['clusterSignals']>;
  providerCounts: ProviderCounts;
  stats: PipelineStats;
  noveltyMap: Record<string, boolean>;
}

export const DEFAULT_PROVIDER_COUNTS: ProviderCounts = {
  newsapi: 0,
  gdelt: 0,
  'google-rss': 0,
  registry: 0,
};

const SUPPORTED_LANGS = new Set(['en', 'en-us', 'en-gb', 'hi']);
const IMPACT_SHORTLIST_THRESHOLD = 0.65;

export async function runAnalysisPipeline(
  items: NormalizedItem[],
  { kv, logger, shortlistSize = 20, mmrLambda }: PipelineOptions
): Promise<PipelineResult> {
  if (items.length === 0) {
    return emptyResult();
  }

  const loggerFn: Logger = logger || (() => {});
  const filtered = items.filter(isSupportedLanguage);
  const providerCounts = filtered.reduce<ProviderCounts>((acc, item) => {
    acc[item.provider] = (acc[item.provider] || 0) + 1;
    return acc;
  }, { ...DEFAULT_PROVIDER_COUNTS });

  const graphUpdate = await updateEntityGraph(filtered, { kv, logger: loggerFn });

  const baseEvidenceMap = new Map<string, ReturnType<typeof scoreEvent>>();
  const classified: ClassifiedItem[] = filtered.map((item) => {
    const evidence = scoreEvent(item);
    baseEvidenceMap.set(item.id, evidence);
    return {
      ...item,
      eventClass: evidence.className,
      classConfidence: evidence.confidence,
      classSignals: {
        lexicon: evidence.lexiconScore,
        embedding: evidence.embeddingScore,
        clusterVoting: 0,
      },
    };
  });

  const { contexts } = buildClusters(classified);
  attachEvidenceToContexts(contexts, baseEvidenceMap);

  applyClusterConsensus(contexts, classified);

  const envelopes: ClusterEnvelope[] = contexts.map((context) => ({
    id: context.id,
    items: context.items,
  }));
  const clusterSignals = buildClusterSignals(envelopes);

  const scored = scoreItems(classified, {
    graphNovelty: graphUpdate.itemNovelty,
    clusterSignals,
  });

  scored.sort((a, b) => b.impact.impact - a.impact.impact);
  const reranked = rerankMMR(scored, scored.length, { lambda: mmrLambda });
  const shortlisted = reranked.filter((item) => item.impact.impact >= IMPACT_SHORTLIST_THRESHOLD).slice(0, shortlistSize);

  const classDistribution = tallyClasses(reranked.slice(0, shortlistSize));

  return {
    classified,
    clusters: contexts,
    scored,
    reranked,
    shortlisted,
    clusterSignals: clusterSignals || {},
    providerCounts,
    noveltyMap: graphUpdate.itemNovelty,
    stats: {
      scanned: items.length,
      classified: classified.length,
      clusters: contexts.length,
      impactQualified: scored.filter((item) => item.impact.impact >= IMPACT_SHORTLIST_THRESHOLD).length,
      shortlisted: shortlisted.length,
      noveltyHits: Object.values(graphUpdate.itemNovelty).filter(Boolean).length,
      classDistribution,
    },
  };
}

function attachEvidenceToContexts(
  contexts: ClusterContext[],
  baseEvidenceMap: Map<string, ReturnType<typeof scoreEvent>>
): void {
  for (const context of contexts) {
    context.evidence = context.evidence || {};
    for (const item of context.items) {
      const evidence = baseEvidenceMap.get(item.id);
      if (evidence) {
        context.evidence[item.id] = evidence;
      }
    }
  }
}

function applyClusterConsensus(contexts: ClusterContext[], classified: ClassifiedItem[]): void {
  const itemMap = new Map(classified.map((item) => [item.id, item] as const));
  for (const context of contexts) {
    for (const item of context.items) {
      const finalEvidence = classifyWithEvidence(item, context);
      const baseEvidence = context.evidence?.[item.id];
      const record = itemMap.get(item.id);
      if (!record) continue;
      record.eventClass = finalEvidence.className;
      record.classConfidence = finalEvidence.confidence;
      record.classSignals = {
        lexicon: finalEvidence.lexiconScore,
        embedding: finalEvidence.embeddingScore,
        clusterVoting: baseEvidence ? finalEvidence.hybridScore - baseEvidence.hybridScore : 0,
      };
    }
  }
}

function tallyClasses(items: ScoredItem[]): Record<EventClass, number> {
  const counts: Record<EventClass, number> = {
    MODEL_LAUNCH: 0,
    PRODUCT_UPDATE: 0,
    PRICING_POLICY: 0,
    PARTNERSHIP_INTEGRATION: 0,
    PAYMENT_COMMERCE: 0,
    DATA_PRIVACY: 0,
    CONTENT_POLICY: 0,
    PLATFORM_RULE: 0,
    RISK_INCIDENT: 0,
    TREND_ANALYSIS: 0,
    OTHER: 0,
  };
  for (const item of items) {
    counts[item.eventClass] = (counts[item.eventClass] || 0) + 1;
  }
  return counts;
}

function isSupportedLanguage(item: NormalizedItem): boolean {
  const lang = (item.language || '').toLowerCase();
  if (!lang) {
    return /[a-z]/i.test(`${item.title || ''} ${item.description || ''}`);
  }
  if (SUPPORTED_LANGS.has(lang)) return true;
  const tokens = tokenize(`${item.title || ''} ${item.description || ''}`);
  return tokens.some((token) => /[a-z]/.test(token));
}

function emptyResult(): PipelineResult {
  return {
    classified: [],
    clusters: [],
    scored: [],
    shortlisted: [],
    reranked: [],
    clusterSignals: {},
    providerCounts: { ...DEFAULT_PROVIDER_COUNTS },
    stats: {
      scanned: 0,
      classified: 0,
      clusters: 0,
      impactQualified: 0,
      shortlisted: 0,
      noveltyHits: 0,
      classDistribution: {
        MODEL_LAUNCH: 0,
        PRODUCT_UPDATE: 0,
        PRICING_POLICY: 0,
        PARTNERSHIP_INTEGRATION: 0,
        PAYMENT_COMMERCE: 0,
        DATA_PRIVACY: 0,
        CONTENT_POLICY: 0,
        PLATFORM_RULE: 0,
        RISK_INCIDENT: 0,
        TREND_ANALYSIS: 0,
        OTHER: 0,
      },
    },
    noveltyMap: {},
  };
}
