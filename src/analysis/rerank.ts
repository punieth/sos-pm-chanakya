import { ClusterContext, ClassifiedItem, EventClass, ScoredItem } from '../types';
import { sanitize, tokenize } from '../utils/text';
import { canonicalizeUrl } from '../utils/url';

interface ClusterProfile {
  id: string;
  tokens: Set<string>;
  vector: number[];
  canonicalUrl: string;
}

interface ClusterState {
  id: string;
  items: ClassifiedItem[];
  profile: ClusterProfile;
}

export interface ClusteringResult {
  contexts: ClusterContext[];
  profiles: Record<string, ClusterProfile>;
}

interface SimilarityProfile {
  tokens: Set<string>;
  vector: number[];
}

const TITLE_THRESHOLD = 0.68;
const EMBEDDING_WEIGHT = 0.4;
const TOKEN_WEIGHT = 0.6;

export function buildClusters(items: ClassifiedItem[]): ClusteringResult {
  const clusters: Record<string, ClusterState> = {};
  const profiles: Record<string, ClusterProfile> = {};
  let clusterIndex = 0;

  for (const item of items) {
    const profile = buildProfile(item);
    const { clusterId, score } = findBestCluster(profile, clusters);
    if (!clusterId || score < TITLE_THRESHOLD) {
      clusterIndex += 1;
      const id = `cluster-${clusterIndex}`;
      clusters[id] = createCluster(id, item, profile);
      profiles[item.id] = profile;
      item.clusterId = id;
    } else {
      const cluster = clusters[clusterId];
      cluster.items.push(item);
      cluster.profile = mergeProfiles(cluster.profile, profile);
      profiles[item.id] = profile;
      item.clusterId = cluster.id;
    }
  }

  const contexts: ClusterContext[] = Object.values(clusters).map((state) => buildContext(state));

  return { contexts, profiles };
}

export function rerankMMR(items: ScoredItem[], N: number, opts?: { lambda?: number }): ScoredItem[] {
  const lambda = clamp01(opts?.lambda ?? 0.75);
  const clusterHeads = selectClusterHeads(items);
  const candidates = clusterHeads.map((head) => ({ head, profile: buildSimilarityProfile(head) }));

  const ordered = mmrOrder(candidates, lambda);
  const shortlisted: ScoredItem[] = [];
  const classCounts: Partial<Record<EventClass, number>> = {};
  const deferred: { head: ScoredItem; profile: SimilarityProfile }[] = [];

  for (const candidate of ordered) {
    if (shortlisted.length >= N) break;
    const className = candidate.head.eventClass;
    const currentCount = classCounts[className] || 0;
    const projectedShare = (currentCount + 1) / (shortlisted.length + 1);
    const impact = candidate.head.impact.impact;
    if (projectedShare > 0.6 && impact < 0.8) {
      deferred.push(candidate);
      continue;
    }
    if (impact < 0.55) {
      continue;
    }
    shortlisted.push(candidate.head);
    classCounts[className] = currentCount + 1;
  }

  for (const candidate of deferred) {
    if (shortlisted.length >= N) break;
    const className = candidate.head.eventClass;
    const currentCount = classCounts[className] || 0;
    const projectedShare = (currentCount + 1) / (shortlisted.length + 1);
    if (projectedShare > 0.6) continue;
    if (candidate.head.impact.impact < 0.55) continue;
    shortlisted.push(candidate.head);
    classCounts[className] = currentCount + 1;
  }

  return shortlisted.slice(0, N);
}

function selectClusterHeads(items: ScoredItem[]): ScoredItem[] {
  const byCluster = new Map<string, ScoredItem[]>();
  for (const item of items) {
    const clusterId = item.clusterId || item.id;
    if (!byCluster.has(clusterId)) byCluster.set(clusterId, []);
    byCluster.get(clusterId)!.push(item);
  }
  const heads: ScoredItem[] = [];
  for (const [clusterId, group] of byCluster.entries()) {
    const [head, ...rest] = group.sort((a, b) => b.impact.impact - a.impact.impact);
    if (!head) continue;
    head.duplicateUrls = rest.map((item) => item.url);
    head.clusterId = clusterId;
    head.clusterSize = group.length;
    heads.push(head);
  }
  return heads;
}

function mmrOrder(candidates: { head: ScoredItem; profile: SimilarityProfile }[], lambda: number): {
  head: ScoredItem;
  profile: SimilarityProfile;
}[] {
  const selected: { head: ScoredItem; profile: SimilarityProfile }[] = [];
  const pool = [...candidates];
  while (pool.length > 0) {
    let bestIndex = 0;
    let bestScore = -Infinity;
    for (let i = 0; i < pool.length; i++) {
      const candidate = pool[i];
      const relevance = candidate.head.impact.impact;
      const diversity = selected.length === 0 ? 0 : maxSimilarity(candidate.profile, selected.map((s) => s.profile));
      const mmr = lambda * relevance - (1 - lambda) * diversity;
      if (mmr > bestScore) {
        bestScore = mmr;
        bestIndex = i;
      }
    }
    selected.push(pool.splice(bestIndex, 1)[0]);
  }
  return selected;
}

function maxSimilarity(profile: SimilarityProfile, others: SimilarityProfile[]): number {
  let max = 0;
  for (const other of others) {
    const sim = similarity(profile, other);
    if (sim > max) max = sim;
  }
  return max;
}

function similarity(a: SimilarityProfile, b: SimilarityProfile): number {
  const jaccard = jaccardSimilarity(a.tokens, b.tokens);
  const cosine = cosineSimilarity(a.vector, b.vector);
  return TOKEN_WEIGHT * jaccard + EMBEDDING_WEIGHT * cosine;
}

function jaccardSimilarity(a: Set<string>, b: Set<string>): number {
  if (a.size === 0 && b.size === 0) return 1;
  let intersection = 0;
  for (const token of a) {
    if (b.has(token)) intersection += 1;
  }
  const union = a.size + b.size - intersection;
  if (union === 0) return 0;
  return intersection / union;
}

function cosineSimilarity(a: number[], b: number[]): number {
  const length = Math.min(a.length, b.length);
  if (length === 0) return 0;
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  const denom = Math.sqrt(magA) * Math.sqrt(magB);
  if (!Number.isFinite(denom) || denom === 0) return 0;
  return dot / denom;
}

function buildProfile(item: ClassifiedItem): ClusterProfile {
  const canonicalUrl = canonicalizeUrl(item.canonicalUrl || item.url);
  const tokens = new Set(tokenize(`${item.title || ''} ${item.description || ''}`));
  const vector = buildVector(tokens);
  return {
    id: item.id,
    tokens,
    vector,
    canonicalUrl,
  };
}

function buildSimilarityProfile(item: ScoredItem): SimilarityProfile {
  const tokens = new Set(tokenize(`${item.title || ''} ${item.description || ''}`));
  const vector = buildVector(tokens);
  return { tokens, vector };
}

function buildVector(tokens: Set<string>): number[] {
  const vector: number[] = [];
  for (const token of tokens) {
    const index = hashToken(token) % 32;
    vector[index] = (vector[index] || 0) + 1;
  }
  return normalize(vector);
}

function normalize(vector: number[]): number[] {
  const magnitude = Math.sqrt(vector.reduce((sum, value) => sum + value * value, 0));
  if (!Number.isFinite(magnitude) || magnitude === 0) {
    return vector.map(() => 0);
  }
  return vector.map((value) => value / magnitude);
}

function hashToken(token: string): number {
  let hash = 0;
  for (let i = 0; i < token.length; i++) {
    hash = (hash << 5) - hash + token.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function findBestCluster(profile: ClusterProfile, clusters: Record<string, ClusterState>): {
  clusterId: string | null;
  score: number;
} {
  let bestClusterId: string | null = null;
  let bestScore = 0;
  for (const [clusterId, cluster] of Object.entries(clusters)) {
    const score = similarity(profile, cluster.profile);
    if (score > bestScore) {
      bestScore = score;
      bestClusterId = clusterId;
    }
  }
  return { clusterId: bestClusterId, score: bestScore };
}

function createCluster(id: string, item: ClassifiedItem, profile: ClusterProfile): ClusterState {
  return {
    id,
    items: [item],
    profile,
  };
}

function mergeProfiles(a: ClusterProfile, b: ClusterProfile): ClusterProfile {
  const tokens = new Set([...a.tokens, ...b.tokens]);
  const vector = normalize(addVectors(a.vector, b.vector));
  return {
    id: a.id,
    tokens,
    vector,
    canonicalUrl: a.canonicalUrl || b.canonicalUrl,
  };
}

function addVectors(a: number[], b: number[]): number[] {
  const length = Math.max(a.length, b.length);
  const result = new Array(length).fill(0);
  for (let i = 0; i < length; i++) {
    result[i] = (a[i] || 0) + (b[i] || 0);
  }
  return result;
}

function buildContext(state: ClusterState): ClusterContext {
  const times = state.items
    .map((item) => new Date(item.publishedAt).getTime())
    .filter((value) => !Number.isNaN(value))
    .sort((a, b) => a - b);
  const windowStart = times.length ? new Date(times[0]).toISOString() : new Date().toISOString();
  const windowEnd = times.length ? new Date(times[times.length - 1]).toISOString() : windowStart;
  const domainCounts: Record<string, number> = {};
  for (const item of state.items) {
    const domain = item.domain.toLowerCase();
    domainCounts[domain] = (domainCounts[domain] || 0) + 1;
  }
  return {
    id: state.id,
    items: state.items,
    windowStart,
    windowEnd,
    domainCounts,
  };
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}
