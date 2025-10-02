import { ingestDynamicSources, dedupeByCanonicalUrl } from './ingest/broker';
import { ingestRegistryFeeds, RegistryEnv } from './ingest/registry';
import { ScoredItem, SourceProvider, ClusterContext } from './types';
import { runAnalysisPipeline, PipelineResult, DEFAULT_PROVIDER_COUNTS } from './analysis/pipeline';
import { runCalibration, CalibrationResult } from './agent/calibrator';

const IMPACT_THRESHOLD = 0.65;

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface IngestStats {
  scanned: number;
  classified: number;
  clusters: number;
  impactQualified: number;
  shortlisted: number;
  noveltyHits: number;
  providerCounts?: Partial<Record<SourceProvider, number>>;
}

export interface DynamicDiscoveryResult {
  items: ScoredItem[];
  allItems: ScoredItem[];
  stats: IngestStats;
  providerCounts: Record<SourceProvider, number>;
  clusters: ClusterContext[];
  analytics: PipelineResult['stats'];
  calibration?: CalibrationResult;
  pipeline: PipelineResult;
}

export interface DiscoveryEnv extends RegistryEnv {
  SEEN: KVNamespace;
  NEWSAPI_KEY?: string;
  NEWSAPI_USER_AGENT?: string;
  GDELT_ENABLED?: string;
  BING_NEWS_ENABLED?: string;
  SELF_CALIBRATE?: string;
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
    const emptyStats = {
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
    } as PipelineResult['stats'];
    const emptyPipeline: PipelineResult = {
      classified: [],
      clusters: [],
      scored: [],
      reranked: [],
      shortlisted: [],
      clusterSignals: {},
      providerCounts: { ...DEFAULT_PROVIDER_COUNTS },
      stats: emptyStats,
      noveltyMap: {},
    };
    return {
      items: [],
      allItems: [],
      stats: { scanned: 0, classified: 0, clusters: 0, impactQualified: 0, shortlisted: 0, noveltyHits: 0, providerCounts: { ...DEFAULT_PROVIDER_COUNTS } },
      providerCounts: { ...DEFAULT_PROVIDER_COUNTS },
      clusters: [],
      analytics: emptyStats,
      calibration: undefined,
      pipeline: emptyPipeline,
    };
  }

  const analysis = await runAnalysisPipeline(combined, { kv: env.SEEN, logger });
  const shortlisted = analysis.shortlisted.filter((item) => item.impact.impact >= IMPACT_THRESHOLD);
  const rejectedPool = analysis.reranked.filter((item) => item.impact.impact < IMPACT_THRESHOLD || !shortlisted.includes(item));

  let calibration: CalibrationResult | undefined;
  if (flagEnabled(env.SELF_CALIBRATE)) {
    const batchId = `${Date.now()}`;
    calibration = await runCalibration(batchId, {
      shortlisted,
      rejected: rejectedPool,
      logger,
    });
  }

  logger('ingest_dynamic_summary', {
    scanned: combined.length,
    classified: analysis.classified.length,
    clusters: analysis.clusters.length,
    impactQualified: analysis.stats.impactQualified,
    shortlisted: shortlisted.length,
    noveltyHits: analysis.stats.noveltyHits,
    dynamic: dynamicItems.length,
    registry: registryItems.length,
  });

  return {
    items: shortlisted,
    allItems: analysis.reranked,
    stats: {
      scanned: combined.length,
      classified: analysis.classified.length,
      clusters: analysis.clusters.length,
      impactQualified: analysis.stats.impactQualified,
      shortlisted: shortlisted.length,
      noveltyHits: analysis.stats.noveltyHits,
      providerCounts: analysis.providerCounts,
    },
    providerCounts: analysis.providerCounts,
    clusters: analysis.clusters,
    analytics: analysis.stats,
    calibration,
    pipeline: analysis,
  };
}

function flagEnabled(value?: string): boolean {
  if (!value) return false;
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}
