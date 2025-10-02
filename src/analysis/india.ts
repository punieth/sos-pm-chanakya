import modelConfig from '../../config/indiaClassifier.json';
import trustedDomains from '../../config/trustedDomains.json';
import { ClassifiedItem, ClusterImpactSummary } from '../types';
import { sanitize, tokenize } from '../utils/text';

export interface IndiaRelevanceResult {
  score: number;
  raw: number;
  features: Record<string, number>;
}

interface ModelConfig {
  version: number;
  bias: number;
  weights: Record<string, number>;
  eventClassLift: Record<string, number>;
  geoTld: Record<string, number>;
  trustedIndianDomains: Record<string, number>;
  indianEntityPrefixes: string[];
  semantic: {
    prototypeCount: number;
    similarityFloor: number;
  };
  keywordHints: string[];
  isotonic: {
    breakpoints: number[];
    values: number[];
  };
}

const CONFIG = modelConfig as unknown as ModelConfig;
const REGULATOR_DOMAINS = new Set(Object.keys(trustedDomains.scores).filter((domain) => domain.endsWith('.gov.in') || domain.includes('rbi') || domain.includes('sebi') || domain.includes('trai')));
const KEYWORD_SET = new Set(CONFIG.keywordHints.map((token) => token.toLowerCase()));
const ENTITY_PREFIXES = CONFIG.indianEntityPrefixes.map((p) => p.toLowerCase());

export function indiaRelevance(
  item: ClassifiedItem,
  cluster: ClusterImpactSummary | undefined
): IndiaRelevanceResult {
  const text = sanitize(`${item.title || ''} ${item.description || ''}`);
  const tokens = tokenize(text);
  const featureValues: Record<string, number> = {};

  featureValues.publisher_geo = resolvePublisherGeo(item.domain);
  featureValues.regulator_signal = resolveRegulatorSignal(item, tokens);
  featureValues.event_class_lift = resolveEventClassLift(item.eventClass);
  featureValues.entity_overlap = resolveEntityOverlap(item);
  featureValues.semantic_similarity = resolveSemanticSimilarity(tokens, cluster);
  featureValues.keyword_prior = resolveKeywordPrior(tokens, item.domain);

  const rawScore = computeLogit(featureValues);
  const logistic = 1 / (1 + Math.exp(-rawScore));
  const calibrated = calibrateIsotonic(logistic);

  return {
    score: calibrated,
    raw: logistic,
    features: featureValues,
  };
}

function resolvePublisherGeo(domain: string | undefined): number {
  if (!domain) return 0;
  const lower = domain.toLowerCase();
  const direct = CONFIG.trustedIndianDomains[lower];
  if (typeof direct === 'number') return clamp01(direct);
  for (const [suffix, weight] of Object.entries(CONFIG.geoTld)) {
    if (lower.endsWith(suffix)) return clamp01(weight);
  }
  return 0;
}

function resolveRegulatorSignal(item: ClassifiedItem, tokens: string[]): number {
  const domain = (item.domain || '').toLowerCase();
  if (REGULATOR_DOMAINS.has(domain)) return 1;
  const source = sanitize(item.source || '').toLowerCase();
  if (REGULATOR_DOMAINS.has(source)) return 0.85;
  const text = `${tokens.join(' ')}`;
  if (text.includes('regulator') || text.includes('reserve bank') || text.includes('gst council')) {
    return 0.6;
  }
  return 0;
}

function resolveEventClassLift(eventClass: ClassifiedItem['eventClass']): number {
  return clamp01(CONFIG.eventClassLift[eventClass] ?? 0);
}

function resolveEntityOverlap(item: ClassifiedItem): number {
  const entities = item.entities?.orgs || [];
  if (entities.length === 0) return 0;
  let hits = 0;
  for (const entity of entities) {
    const value = entity.toLowerCase();
    if (ENTITY_PREFIXES.some((prefix) => value.startsWith(prefix))) {
      hits += 1;
    }
  }
  return clamp01(hits / Math.max(1, entities.length));
}

function resolveSemanticSimilarity(tokens: string[], cluster: ClusterImpactSummary | undefined): number {
  if (tokens.length === 0) return CONFIG.semantic.similarityFloor;
  let hits = 0;
  for (const token of tokens) {
    if (KEYWORD_SET.has(token)) hits += 1;
  }
  const lexical = hits / Math.max(1, tokens.length);
  const clusterBoost = cluster ? clamp01(cluster.trustedDomains / Math.max(1, cluster.totalItems)) : 0;
  const base = Math.max(CONFIG.semantic.similarityFloor, lexical * 2 + clusterBoost * 0.3);
  return clamp01(base);
}

function resolveKeywordPrior(tokens: string[], domain: string | undefined): number {
  let hits = 0;
  for (const token of tokens) {
    if (KEYWORD_SET.has(token)) hits += 1;
  }
  if (domain) {
    const lower = domain.toLowerCase();
    if (CONFIG.trustedIndianDomains[lower]) hits += 2;
  }
  return clamp01(hits / 12);
}

function computeLogit(features: Record<string, number>): number {
  let value = CONFIG.bias;
  for (const [key, weight] of Object.entries(CONFIG.weights)) {
    const featureValue = clamp01(features[key] ?? 0);
    value += weight * featureValue;
  }
  return value;
}

function calibrateIsotonic(probability: number): number {
  const table = CONFIG.isotonic;
  const xs = table.breakpoints;
  const ys = table.values;
  if (xs.length !== ys.length || xs.length === 0) return clamp01(probability);
  if (probability <= xs[0]) return clamp01(ys[0]);
  for (let i = 1; i < xs.length; i++) {
    if (probability <= xs[i]) {
      const x0 = xs[i - 1];
      const x1 = xs[i];
      const y0 = ys[i - 1];
      const y1 = ys[i];
      if (x1 === x0) return clamp01(y1);
      const t = (probability - x0) / (x1 - x0);
      return clamp01(y0 + t * (y1 - y0));
    }
  }
  return clamp01(ys[xs.length - 1]);
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}
