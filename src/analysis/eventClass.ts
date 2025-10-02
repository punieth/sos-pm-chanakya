import lexicons from '../../config/eventLexicons.json';
import prototypeVectors from '../../config/prototypes/vectors.json';
import { ClassificationEvidence, EventClass, NormalizedItem, ClusterContext } from '../types';
import { sanitize, tokenize } from '../utils/text';

const LEXICON_ENTRIES = lexicons.classes as Record<EventClass, { verbs?: string[]; nouns?: string[]; phrases?: string[] }>; // OTHER excluded intentionally
const PROTOTYPE_DIMENSION: number = prototypeVectors.dimension || 8;
const PROTOTYPE_MAP: Record<EventClass, number[]> = buildPrototypeMap();

const CLASS_LIST = Object.keys(LEXICON_ENTRIES) as EventClass[];
const HYBRID_WEIGHT = 0.6;
const EMBEDDING_WEIGHT = 0.4;
const SWITCH_MARGIN = 0.05;

interface ClassScore {
  className: EventClass;
  lexicon: number;
  embedding: number;
  hybrid: number;
}

export function scoreEvent(item: NormalizedItem): ClassificationEvidence {
  const tokens = collectTokens(item);
  const classScores = evaluateClasses(tokens);
  const [best, runnerUp] = classScores;
  const baseEvidence = buildEvidence(best, runnerUp);
  return baseEvidence;
}

export function classifyEvent(
  item: NormalizedItem,
  clusterContext?: ClusterContext
): { className: EventClass; confidence: number } {
  if (!clusterContext) {
    const base = scoreEvent(item);
    return { className: base.className, confidence: base.confidence };
  }
  const evidenceMap = clusterContext.evidence || {};
  const base = evidenceMap[item.id] ?? scoreEvent(item);
  const finalEvidence = applyClusterConsensus(item.id, base, clusterContext);
  if (!clusterContext.evidence) {
    clusterContext.evidence = {};
  }
  clusterContext.evidence[item.id] = finalEvidence;
  return { className: finalEvidence.className, confidence: finalEvidence.confidence };
}

export function classifyWithEvidence(
  item: NormalizedItem,
  clusterContext: ClusterContext
): ClassificationEvidence {
  const evidenceMap = clusterContext.evidence || {};
  const base = evidenceMap[item.id] ?? scoreEvent(item);
  const finalEvidence = applyClusterConsensus(item.id, base, clusterContext);
  clusterContext.evidence = clusterContext.evidence || {};
  clusterContext.evidence[item.id] = finalEvidence;
  return finalEvidence;
}

function applyClusterConsensus(
  itemId: string,
  base: ClassificationEvidence,
  clusterContext: ClusterContext
): ClassificationEvidence {
  const evidenceMap = clusterContext.evidence || {};
  const peerEvidence = Object.entries(evidenceMap)
    .filter(([id]) => id !== itemId)
    .map(([, evidence]) => evidence);

  if (peerEvidence.length === 0) {
    return base;
  }

  const aggregates = new Map<EventClass, { hybrid: number; lexicon: number; votes: number }>();
  for (const peer of peerEvidence) {
    if (!aggregates.has(peer.className)) {
      aggregates.set(peer.className, { hybrid: 0, lexicon: 0, votes: 0 });
    }
    const data = aggregates.get(peer.className)!;
    data.hybrid += peer.hybridScore;
    data.lexicon += peer.lexiconScore;
    data.votes += 1;
  }

  if (aggregates.size === 0) {
    return base;
  }

  let winningClass: EventClass | null = null;
  let winningHybrid = -Infinity;
  let winningVotes = 0;
  let winningLex = 0;

  for (const [className, data] of aggregates.entries()) {
    const avgHybrid = data.hybrid / data.votes;
    const avgLex = data.lexicon / data.votes;
    if (
      avgHybrid > winningHybrid + 1e-6 ||
      (Math.abs(avgHybrid - winningHybrid) < 1e-6 && data.votes > winningVotes) ||
      (Math.abs(avgHybrid - winningHybrid) < 1e-6 && data.votes === winningVotes && avgLex > winningLex)
    ) {
      winningClass = className;
      winningHybrid = avgHybrid;
      winningLex = avgLex;
      winningVotes = data.votes;
    }
  }

  if (!winningClass) {
    return base;
  }

  const requiresSwitch = shouldSwitchClass(base, winningClass, winningHybrid, winningVotes, peerEvidence.length);
  if (!requiresSwitch) {
    return {
      ...base,
      confidence: clamp01(base.confidence + winningHybrid * 0.1),
    };
  }

  const switched: ClassificationEvidence = {
    className: winningClass,
    lexiconScore: winningLex,
    embeddingScore: base.embeddingScore,
    hybridScore: Math.max(base.hybridScore, winningHybrid),
    confidence: clamp01(Math.max(base.confidence, winningHybrid)),
  };
  return switched;
}

function shouldSwitchClass(
  base: ClassificationEvidence,
  winningClass: EventClass,
  winningHybrid: number,
  winningVotes: number,
  peerCount: number
): boolean {
  if (winningClass === base.className) return false;
  const majority = winningVotes / Math.max(1, peerCount);
  if (majority < 0.5) return false;
  const delta = winningHybrid - base.hybridScore;
  if (delta < SWITCH_MARGIN) return false;
  if (base.confidence >= 0.85) return false;
  return true;
}

function buildEvidence(best: ClassScore, runnerUp?: ClassScore): ClassificationEvidence {
  const confidence = computeConfidence(best, runnerUp);
  return {
    className: best.className,
    lexiconScore: best.lexicon,
    embeddingScore: best.embedding,
    hybridScore: best.hybrid,
    confidence,
  };
}

function computeConfidence(best: ClassScore, runnerUp?: ClassScore): number {
  const margin = runnerUp ? best.hybrid - runnerUp.hybrid : best.hybrid;
  const base = best.hybrid * 0.7 + margin * 0.3;
  return clamp01(base);
}

function evaluateClasses(tokens: TokenBundle): ClassScore[] {
  const scores: ClassScore[] = CLASS_LIST.map((className) => {
    const lex = lexiconScore(className, tokens);
    const embedding = embeddingScore(className, tokens.embedding);
    const hybrid = HYBRID_WEIGHT * lex + EMBEDDING_WEIGHT * embedding;
    return { className, lexicon: lex, embedding, hybrid };
  });

  scores.sort((a, b) => b.hybrid - a.hybrid || b.lexicon - a.lexicon);

  const best = scores[0];
  if (!best || best.hybrid < 0.25) {
    return [
      {
        className: 'OTHER',
        lexicon: best ? best.lexicon : 0,
        embedding: best ? best.embedding : 0,
        hybrid: best ? best.hybrid : 0,
      },
      scores[1],
    ].filter(Boolean) as ClassScore[];
  }

  return scores;
}

function lexiconScore(className: EventClass, tokens: TokenBundle): number {
  const entry = LEXICON_ENTRIES[className];
  if (!entry) return 0;
  let hits = 0;
  let total = 0;
  for (const verb of entry.verbs || []) {
    total += 1;
    if (tokens.verbs.has(verb.toLowerCase())) hits += 1.2;
    if (tokens.tokens.has(verb.toLowerCase())) hits += 0.8;
  }
  for (const noun of entry.nouns || []) {
    total += 1;
    if (tokens.tokens.has(noun.toLowerCase())) hits += 1;
  }
  for (const phrase of entry.phrases || []) {
    total += 1;
    if (tokens.text.includes(phrase.toLowerCase())) hits += 1.1;
  }
  if (total === 0) return 0;
  return clamp01(hits / (total * 1.2));
}

function embeddingScore(className: EventClass, embedding: number[]): number {
  const prototype = PROTOTYPE_MAP[className];
  if (!prototype) return 0;
  return cosineSimilarity(embedding, prototype);
}

function collectTokens(item: NormalizedItem): TokenBundle {
  const text = sanitize(`${item.title || ''} ${item.description || ''}`).toLowerCase();
  const tokens = new Set(tokenize(text));
  const verbSet = new Set<string>();
  if (Array.isArray(item.verbs)) {
    for (const verb of item.verbs) {
      const lower = verb.toLowerCase();
      tokens.add(lower);
      verbSet.add(lower);
    }
  }
  if (item.entities?.verbs) {
    for (const verb of item.entities.verbs) {
      const lower = verb.toLowerCase();
      tokens.add(lower);
      verbSet.add(lower);
    }
  }
  if (item.entities?.orgs) {
    for (const org of item.entities.orgs) {
      const lower = org.toLowerCase();
      tokens.add(lower);
    }
  }
  const embedding = buildEmbedding(Array.from(tokens));
  return {
    text,
    tokens,
    verbs: verbSet,
    embedding,
  };
}

interface TokenBundle {
  text: string;
  tokens: Set<string>;
  verbs: Set<string>;
  embedding: number[];
}

function buildEmbedding(tokens: string[]): number[] {
  const vector = new Array(PROTOTYPE_DIMENSION).fill(0);
  for (const token of tokens) {
    const index = hashToken(token) % PROTOTYPE_DIMENSION;
    vector[index] += 1;
  }
  return normalize(vector);
}

function buildPrototypeMap(): Record<EventClass, number[]> {
  const vectors = prototypeVectors.vectors as Record<string, number[]>;
  const map: Record<EventClass, number[]> = {} as Record<EventClass, number[]>;
  for (const [className, vector] of Object.entries(vectors)) {
    const normalized = normalize(vector);
    map[className as EventClass] = normalized;
  }
  return map;
}

function normalize(vector: number[]): number[] {
  const magnitude = Math.sqrt(vector.reduce((sum, value) => sum + value * value, 0));
  if (!Number.isFinite(magnitude) || magnitude === 0) {
    return vector.map(() => 0);
  }
  return vector.map((value) => value / magnitude);
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
  return clamp01(dot / denom);
}

function hashToken(token: string): number {
  let hash = 0;
  for (let i = 0; i < token.length; i++) {
    hash = (hash << 5) - hash + token.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}
