import impactWeightConfig from '../../config/impactWeights.json';
import trustedConfig from '../../config/trustedDomains.json';
import lexiconConfig from '../../config/eventLexicons.json';
import { ClassifiedItem, ImpactComponents, ImpactScore, ImpactWeights, ScoredItem, ClusterImpactSummary } from '../types';
import { HOUR_MS, nowUtc, parseUtc } from '../utils/time';
import { tokenize } from '../utils/text';
import { indiaRelevance } from './india';
import type { IndiaRelevanceResult } from './india';

export interface ImpactComputationContext {
  now?: Date;
  weights?: Partial<ImpactWeights>;
  graphNovelty?: Record<string, boolean>;
  clusterSignals?: Record<string, ClusterImpactSummary>;
  preregisteredIndia?: Record<string, IndiaRelevanceResult>;
  authorityOverrides?: Record<string, number>;
  commerceOverrides?: Record<string, number>;
  momentumOverrides?: Record<string, number>;
}

interface ImpactFeatureTrace {
  recencyHours: number;
  publicationLagHours: number;
  surfaceReachDomains: number;
  trustedDomainScore: number;
  graphNovelty: number;
  commerceTokens: number;
  momentumRaw: number;
  indiaRaw: number;
  authorityRaw: number;
}

const DEFAULT_WEIGHTS: ImpactWeights = normalizeWeights(impactWeightConfig.weights as ImpactWeights);
const RECENCY_HALF_LIFE_HOURS = clampNumber(impactWeightConfig.decay?.recencyHalfLifeHours ?? 48, 12, 96);
const MOMENTUM_WINDOW_HOURS = clampNumber(impactWeightConfig.decay?.momentumWindowHours ?? 18, 4, 48);
const SURFACE_REACH_CAP = clampNumber(impactWeightConfig.caps?.surfaceReachDomains ?? 10, 3, 20);

const TRUSTED_DOMAIN_SCORES: Record<string, number> = { ...trustedConfig.scores };
const TRUSTED_WINDOW_HOURS = trustedConfig.windowHours ?? 72;

const PAYMENT_LEXICON = buildLexiconSet('PAYMENT_COMMERCE');
const MOMENTUM_FLOOR = 0.05;

export function computeImpact(components: ImpactComponents, weights: ImpactWeights): ImpactScore {
  const normalizedWeights = normalizeWeights(weights);
  let impact = 0;
  for (const key of Object.keys(normalizedWeights) as (keyof ImpactComponents)[]) {
    const componentValue = clamp01(components[key]);
    const weight = normalizedWeights[key];
    impact += componentValue * weight;
  }
  return {
    impact: clamp01(impact),
    components: { ...components },
    weights: normalizedWeights,
  };
}

export function scoreItems(
  items: ClassifiedItem[],
  context: ImpactComputationContext = {}
): ScoredItem[] {
  const now = context.now || nowUtc();
  const weights = resolveWeights(context.weights);
  return items.map((item) => scoreSingleItem(item, now, weights, context));
}

export function scoreSingleItem(
  item: ClassifiedItem,
  now: Date,
  weights: ImpactWeights,
  context: ImpactComputationContext
): ScoredItem {
  const trace = initialiseTrace();
  const clusterSignal = lookupClusterSignal(item, context.clusterSignals);
  const components = buildImpactComponents(item, now, trace, context, clusterSignal);
  const impact = computeImpact(components, weights);
  const trustedFallback = TRUSTED_DOMAIN_SCORES[item.domain?.toLowerCase() || ''] ? 1 : 0;
  const trustedCount = clusterSignal?.trustedDomains ?? trustedFallback;
  const featureRecord = { ...trace } as Record<string, number>;
  return {
    ...item,
    impact,
    impactMeta: {
      features: featureRecord,
      decisions: {
        weightVersion: impactWeightConfig.version,
        recencyHalfLifeHours: RECENCY_HALF_LIFE_HOURS,
        momentumWindowHours: MOMENTUM_WINDOW_HOURS,
        surfaceReachCap: SURFACE_REACH_CAP,
        trustedWindowHours: TRUSTED_WINDOW_HOURS,
      },
    },
  };
}

function buildImpactComponents(
  item: ClassifiedItem,
  now: Date,
  trace: ImpactFeatureTrace,
  context: ImpactComputationContext,
  providedSignal?: ClusterImpactSummary
): ImpactComponents {
  const recency = computeRecency(item.publishedAt, now, trace);
  const clusterSignal = providedSignal ?? lookupClusterSignal(item, context.clusterSignals);
  const graphNovelty = context.graphNovelty?.[item.id] ? 1 : 0;
  trace.graphNovelty = graphNovelty;

  const india = resolveIndiaTie(item, clusterSignal, context, trace);
  const surfaceReach = computeSurfaceReach(clusterSignal, trace);
  const authority = computeAuthority(item, clusterSignal, context, trace);
  const commerceTie = computeCommerceTie(item, context, trace);
  const momentum = computeMomentum(clusterSignal, context, trace);

  return {
    recency,
    surfaceReach,
    graphNovelty,
    authority,
    commerceTie,
    indiaTie: india,
    momentum,
  };
}

function computeRecency(publishedAt: string | undefined, now: Date, trace: ImpactFeatureTrace): number {
  const published = parseUtc(publishedAt);
  if (!published) {
    trace.recencyHours = Number.NaN;
    trace.publicationLagHours = Number.POSITIVE_INFINITY;
    return 0;
  }
  const diffMs = now.getTime() - published.getTime();
  const diffHours = diffMs / HOUR_MS;
  trace.publicationLagHours = diffHours;
  const recencyScore = Math.exp(-Math.max(0, diffHours) / RECENCY_HALF_LIFE_HOURS);
  trace.recencyHours = Math.max(0, RECENCY_HALF_LIFE_HOURS - diffHours);
  return clamp01(recencyScore);
}

function computeSurfaceReach(signal: ClusterImpactSummary | undefined, trace: ImpactFeatureTrace): number {
  if (!signal) {
    trace.surfaceReachDomains = 0;
    return 0;
  }
  trace.surfaceReachDomains = signal.trustedDomains;
  if (Number.isFinite(signal.surfaceReach)) {
    return clamp01(signal.surfaceReach);
  }
  const capped = Math.min(signal.trustedDomains, SURFACE_REACH_CAP);
  const score = SURFACE_REACH_CAP === 0 ? 0 : capped / SURFACE_REACH_CAP;
  return clamp01(score);
}

function computeAuthority(
  item: ClassifiedItem,
  signal: ClusterImpactSummary | undefined,
  context: ImpactComputationContext,
  trace: ImpactFeatureTrace
): number {
  if (context.authorityOverrides && typeof context.authorityOverrides[item.id] === 'number') {
    const override = clamp01(context.authorityOverrides[item.id]);
    trace.authorityRaw = override;
    return override;
  }
  const domain = item.domain?.toLowerCase() || '';
  const directScore = TRUSTED_DOMAIN_SCORES[domain] ?? 0;
  trace.trustedDomainScore = directScore;
  const clusterMultiplier = signal ? Math.min(1, (signal.trustedDomains || 0) / Math.max(1, signal.totalItems || 1)) : 0;
  const confidenceLift = clamp01(item.classConfidence);
  const combined = clamp01(directScore * 0.7 + clusterMultiplier * 0.2 + confidenceLift * 0.1);
  trace.authorityRaw = combined;
  return combined;
}

function computeCommerceTie(
  item: ClassifiedItem,
  context: ImpactComputationContext,
  trace: ImpactFeatureTrace
): number {
  if (context.commerceOverrides && typeof context.commerceOverrides[item.id] === 'number') {
    const override = clamp01(context.commerceOverrides[item.id]);
    trace.commerceTokens = override;
    return override;
  }
  if (item.eventClass === 'PAYMENT_COMMERCE') {
    trace.commerceTokens = PAYMENT_LEXICON.size;
    return 1;
  }
  const tokens = tokenize(`${item.title || ''} ${item.description || ''}`);
  let hits = 0;
  for (const token of tokens) {
    if (PAYMENT_LEXICON.has(token)) hits += 1;
  }
  trace.commerceTokens = hits;
  return clamp01(Math.min(1, hits / 6));
}

function computeMomentum(
  signal: ClusterImpactSummary | undefined,
  context: ImpactComputationContext,
  trace: ImpactFeatureTrace
): number {
  if (signal && context.momentumOverrides && typeof context.momentumOverrides[signal.id] === 'number') {
    const override = clamp01(context.momentumOverrides[signal.id]);
    trace.momentumRaw = override;
    return override;
  }
  if (!signal) {
    trace.momentumRaw = 0;
    return MOMENTUM_FLOOR;
  }
  const velocity = clamp01(signal.velocity);
  trace.momentumRaw = velocity;
  return Math.max(MOMENTUM_FLOOR, velocity);
}

function resolveIndiaTie(
  item: ClassifiedItem,
  signal: ClusterImpactSummary | undefined,
  context: ImpactComputationContext,
  trace: ImpactFeatureTrace
): number {
  const cached = context.preregisteredIndia?.[item.id];
  const result = cached || indiaRelevance(item, signal);
  trace.indiaRaw = result.raw;
  return clamp01(result.score);
}

function lookupClusterSignal(
  item: ClassifiedItem,
  signals: ImpactComputationContext['clusterSignals']
): ClusterImpactSummary | undefined {
  if (!signals) return undefined;
  const key = item.clusterId || item.id;
  const resolved = signals[key];
  if (!resolved) return undefined;
  if (!resolved.id) {
    return { ...resolved, id: key };
  }
  return resolved;
}

function initialiseTrace(): ImpactFeatureTrace {
  return {
    recencyHours: 0,
    publicationLagHours: 0,
    surfaceReachDomains: 0,
    trustedDomainScore: 0,
    graphNovelty: 0,
    commerceTokens: 0,
    momentumRaw: 0,
    indiaRaw: 0,
    authorityRaw: 0,
  };
}

function resolveWeights(overrides?: Partial<ImpactWeights>): ImpactWeights {
  if (!overrides || Object.keys(overrides).length === 0) {
    return { ...DEFAULT_WEIGHTS };
  }
  const merged: ImpactWeights = { ...DEFAULT_WEIGHTS };
  for (const key of Object.keys(overrides) as (keyof ImpactWeights)[]) {
    const value = overrides[key];
    if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
      merged[key] = value;
    }
  }
  return normalizeWeights(merged);
}

function normalizeWeights(weights: ImpactWeights): ImpactWeights {
  let sum = 0;
  for (const key of Object.keys(weights) as (keyof ImpactWeights)[]) {
    const value = weights[key];
    sum += Number.isFinite(value) && value >= 0 ? value : 0;
  }
  if (sum <= 0) {
    return { ...DEFAULT_WEIGHTS };
  }
  const normalized: ImpactWeights = { ...weights };
  for (const key of Object.keys(weights) as (keyof ImpactWeights)[]) {
    const value = weights[key];
    normalized[key] = value >= 0 && Number.isFinite(value) ? value / sum : 0;
  }
  return normalized;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function clampNumber(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  if (value < min) return min;
  if (value > max) return max;
  return value;
}

function buildLexiconSet(className: keyof typeof lexiconConfig.classes): Set<string> {
  const entry = lexiconConfig.classes[className];
  if (!entry) return new Set();
  const bag = new Set<string>();
  const add = (value: string) => {
    const lower = value.toLowerCase();
    if (lower) bag.add(lower);
  };
  for (const verb of entry.verbs || []) add(verb);
  for (const noun of entry.nouns || []) add(noun);
  for (const phrase of entry.phrases || []) {
    const parts = tokenize(phrase);
    for (const part of parts) add(part);
  }
  return bag;
}

