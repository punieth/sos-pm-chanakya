import impactWeightConfig from '../../config/impactWeights.json';
import trustedConfig from '../../config/trustedDomains.json';
import { ClassifiedItem, ClusterImpactSummary } from '../types';
import { HOUR_MS, nowUtc, parseUtc } from '../utils/time';

export interface ClusterEnvelope {
  id: string;
  items: ClassifiedItem[];
}

export interface SurfaceReachOptions {
  now?: Date;
  windowHours?: number;
  cap?: number;
}

const DEFAULT_WINDOW_HOURS = trustedConfig.windowHours ?? 72;
const DEFAULT_CAP = impactWeightConfig.caps?.surfaceReachDomains ?? 10;
const MOMENTUM_WINDOW_HOURS = impactWeightConfig.decay?.momentumWindowHours ?? 18;

const TRUSTED_SET = new Set(Object.keys(trustedConfig.scores));

export function computeClusterImpactSignals(
  envelope: ClusterEnvelope,
  options: SurfaceReachOptions = {}
): ClusterImpactSummary {
  const now = options.now || nowUtc();
  const windowHours = options.windowHours ?? DEFAULT_WINDOW_HOURS;
  const cap = options.cap ?? DEFAULT_CAP;

  const windowMs = windowHours * HOUR_MS;
  const recentWindowMs = MOMENTUM_WINDOW_HOURS * HOUR_MS;
  const previousWindowMs = recentWindowMs * 2;

  const distinctDomains = new Set<string>();
  const trustedDomains = new Set<string>();
  let totalItems = 0;
  let recentCount = 0;
  let previousCount = 0;

  for (const item of envelope.items) {
    const published = parseUtc(item.publishedAt);
    if (!published) continue;
    const ageMs = now.getTime() - published.getTime();
    if (ageMs < 0 || ageMs > windowMs) continue;
    totalItems += 1;
    distinctDomains.add(item.domain.toLowerCase());
    if (TRUSTED_SET.has(item.domain.toLowerCase())) {
      trustedDomains.add(item.domain.toLowerCase());
    }
    if (ageMs <= recentWindowMs) {
      recentCount += 1;
    } else if (ageMs <= previousWindowMs) {
      previousCount += 1;
    }
  }

  const trustedCount = trustedDomains.size;
  const surfaceReach = totalItems === 0 ? 0 : Math.min(trustedCount, cap) / cap;

  const recencyShare = totalItems === 0 ? 0 : recentCount / totalItems;
  const previousShare = totalItems === 0 ? 0 : previousCount / totalItems;
  const velocityRaw = Math.max(0, recencyShare - previousShare * 0.6) + recencyShare * 0.4;
  const velocity = Math.min(1, velocityRaw);

  return {
    id: envelope.id,
    surfaceReach,
    distinctDomains: distinctDomains.size,
    trustedDomains: trustedCount,
    velocity,
    totalItems,
    windowHours,
  };
}

export function buildClusterSignals(
  clusters: ClusterEnvelope[],
  options: SurfaceReachOptions = {}
): Record<string, ClusterImpactSummary> {
  const map: Record<string, ClusterImpactSummary> = {};
  for (const cluster of clusters) {
    map[cluster.id] = computeClusterImpactSignals(cluster, options);
  }
  return map;
}
