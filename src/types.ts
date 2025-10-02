export type SourceProvider = 'newsapi' | 'gdelt' | 'google-rss' | 'registry';

export interface SourceItem {
  title: string;
  url: string;
  publishedAt: string;
  source: string;
  description?: string;
  language?: string;
  provider: SourceProvider;
  canonicalUrl?: string;
  authors?: string[];
  country?: string;
}

export interface NormalizedItem extends SourceItem {
  id: string;
  domain: string;
  embedding?: number[];
  verbs?: string[];
  entities?: EntityExtraction;
}

export type EventClassSeed =
  | 'MODEL_LAUNCH'
  | 'PRODUCT_UPDATE'
  | 'PRICING_POLICY'
  | 'PARTNERSHIP_INTEGRATION'
  | 'PAYMENT_COMMERCE'
  | 'DATA_PRIVACY'
  | 'CONTENT_POLICY'
  | 'PLATFORM_RULE'
  | 'RISK_INCIDENT'
  | 'TREND_ANALYSIS';

export type EventClass = EventClassSeed | 'OTHER';

export interface ClassificationSignals {
  lexicon: number;
  embedding: number;
  clusterVoting: number;
}

export interface ClassifiedItem extends NormalizedItem {
  eventClass: EventClass;
  classConfidence: number;
  classSignals: ClassificationSignals;
  clusterId?: string;
  clusterSize?: number;
  clusterScore?: number;
}

export interface ClassificationEvidence {
  className: EventClass;
  lexiconScore: number;
  embeddingScore: number;
  hybridScore: number;
  confidence: number;
}

export interface ClusterContext {
  id: string;
  items: ClassifiedItem[];
  windowStart: string;
  windowEnd: string;
  domainCounts: Record<string, number>;
  representativeId?: string;
  evidence?: Record<string, ClassificationEvidence>;
}

export interface ClusterImpactSummary {
  id: string;
  surfaceReach: number;
  distinctDomains: number;
  trustedDomains: number;
  velocity: number;
  totalItems: number;
  windowHours: number;
}

export interface ImpactComponents {
  recency: number;
  surfaceReach: number;
  graphNovelty: number;
  authority: number;
  commerceTie: number;
  indiaTie: number;
  momentum: number;
}

export type ImpactComponentKey = keyof ImpactComponents;
export type ImpactWeights = Record<ImpactComponentKey, number>;

export interface ImpactScore {
  impact: number;
  components: ImpactComponents;
  weights: ImpactWeights;
}

export interface ImpactMeta {
  features: Record<string, number>;
  decisions: Record<string, unknown>;
}

export interface ScoredItem extends ClassifiedItem {
  impact: ImpactScore;
  impactMeta?: ImpactMeta;
  duplicateUrls?: string[];
  clusterImpact?: ClusterImpactSummary;
  trustedDomainCount?: number;
}

export type PipelineItem = ScoredItem;

export interface EntityExtraction {
  orgs: string[];
  products: string[];
  verbs: string[];
}

export interface EntityGraphSnapshot {
  nodes: Record<string, EntityNode>;
  edges: Record<string, EntityEdge>;
}

export interface EntityNode {
  id: string;
  label: string;
  degree: number;
  lastSeen: number;
  type: 'ORG' | 'PRODUCT';
}

export interface EntityEdge {
  id: string;
  source: string;
  target: string;
  verbBucket: string;
  count: number;
  lastSeen: number;
}

export interface FeatureFlagConfig {
  FEAT_GEMINI?: string;
  FEAT_LOCAL?: string;
  FEAT_COMPOSE_LLM?: string;
  GDELT_ENABLED?: string;
  BING_NEWS_ENABLED?: string;
}

export interface ScoreTelemetry {
  model_used: string;
  retries: number;
  status_code: number;
  provider?: string;
}
