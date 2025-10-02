import path from 'node:path';
import { promises as fs } from 'node:fs';

import { ScoredItem, ImpactWeights, ImpactComponentKey } from '../types';
import impactConfig from '../../config/impactWeights.json';

export interface CalibrationInput {
  shortlisted: ScoredItem[];
  rejected: ScoredItem[];
  weightsOverride?: ImpactWeights;
  alpha?: number;
  logger?: (phase: string, payload: Record<string, unknown>) => void;
}

export interface CalibrationResult {
  batchId: string;
  sampledShortlisted: number;
  sampledRejected: number;
  weightsBefore: ImpactWeights;
  weightsAfter: ImpactWeights;
  deltas: Record<ImpactComponentKey, number>;
  reasons: string[];
  historyEntry: CalibrationHistoryEntry;
}

export interface CalibrationHistoryEntry {
  batchId: string;
  timestamp: string;
  deltas: Record<ImpactComponentKey, number>;
  weights: ImpactWeights;
  notes: string;
}

interface ImpactWeightsConfigFile {
  version: number;
  updatedAt: string;
  weights: ImpactWeights;
  decay?: Record<string, number>;
  caps?: Record<string, number>;
  history?: CalibrationHistoryEntry[];
  scales?: Partial<Record<ImpactComponentKey, number>>;
}

const WEIGHTS_PATH = path.join(process.cwd(), 'config', 'impactWeights.json');
const SAMPLE_SIZE = 20;
const MIN_SAMPLE = 5;
const EMA_ALPHA = 0.1;
const MAX_DELTA = 0.03;
const MIN_WEIGHT = 0.01;

const ACTIONABLE_CLASSES = new Set([
  'PRICING_POLICY',
  'PLATFORM_RULE',
  'CONTENT_POLICY',
  'DATA_PRIVACY',
  'PAYMENT_COMMERCE',
  'RISK_INCIDENT',
]);

const UPSIDE_CLASSES = new Set([
  'MODEL_LAUNCH',
  'PRODUCT_UPDATE',
  'PARTNERSHIP_INTEGRATION',
  'PAYMENT_COMMERCE',
]);

export async function runCalibration(batchId: string, input: CalibrationInput): Promise<CalibrationResult> {
  const alpha = input.alpha ?? EMA_ALPHA;
  const logger = input.logger ?? (() => {});

  const shortlisted = stratifiedSample(input.shortlisted, SAMPLE_SIZE);
  const rejected = stratifiedSample(input.rejected, SAMPLE_SIZE);

  if (shortlisted.length < MIN_SAMPLE || rejected.length < MIN_SAMPLE) {
    logger('calibration_skipped', {
      shortlisted: shortlisted.length,
      rejected: rejected.length,
      reason: 'insufficient_sample_size',
    });
    const weightsBefore = normalizeWeights(input.weightsOverride ?? (impactConfig.weights as ImpactWeights));
    const zeroDeltas = zeroComponentRecord();
    const historyEntry: CalibrationHistoryEntry = {
      batchId,
      timestamp: new Date().toISOString(),
      deltas: zeroDeltas,
      weights: weightsBefore,
      notes: 'insufficient_sample_size',
    };
    return {
      batchId,
      sampledShortlisted: shortlisted.length,
      sampledRejected: rejected.length,
      weightsBefore,
      weightsAfter: weightsBefore,
      deltas: zeroDeltas,
      reasons: ['Calibration skipped: insufficient sample size'],
      historyEntry,
    };
  }

  const weightsBefore = normalizeWeights(input.weightsOverride ?? (impactConfig.weights as ImpactWeights));
  const componentKeys = Object.keys(weightsBefore) as ImpactComponentKey[];

  const labels = buildLabels(shortlisted, rejected);
  const componentErrors = zeroComponentRecord();

  for (const key of componentKeys) {
    const posGap = labels.positiveTarget[key] - labels.positivePrediction[key];
    const negGap = labels.negativePrediction[key] - labels.negativeTarget[key];
    componentErrors[key] = posGap * 0.7 + negGap * 0.3;
  }

  const deltas: Record<ImpactComponentKey, number> = {
    recency: clampDelta(componentErrors.recency * alpha),
    surfaceReach: clampDelta(componentErrors.surfaceReach * alpha),
    graphNovelty: clampDelta(componentErrors.graphNovelty * alpha),
    authority: clampDelta(componentErrors.authority * alpha),
    commerceTie: clampDelta(componentErrors.commerceTie * alpha),
    indiaTie: clampDelta(componentErrors.indiaTie * alpha),
    momentum: clampDelta(componentErrors.momentum * alpha),
  };

  const weightsAfter: ImpactWeights = normalizeWeights({
    recency: Math.max(MIN_WEIGHT, weightsBefore.recency + deltas.recency),
    surfaceReach: Math.max(MIN_WEIGHT, weightsBefore.surfaceReach + deltas.surfaceReach),
    graphNovelty: Math.max(MIN_WEIGHT, weightsBefore.graphNovelty + deltas.graphNovelty),
    authority: Math.max(MIN_WEIGHT, weightsBefore.authority + deltas.authority),
    commerceTie: Math.max(MIN_WEIGHT, weightsBefore.commerceTie + deltas.commerceTie),
    indiaTie: Math.max(MIN_WEIGHT, weightsBefore.indiaTie + deltas.indiaTie),
    momentum: Math.max(MIN_WEIGHT, weightsBefore.momentum + deltas.momentum),
  });

  const reasons = buildReasons(labels, deltas);
  const historyEntry: CalibrationHistoryEntry = {
    batchId,
    timestamp: new Date().toISOString(),
    deltas,
    weights: weightsAfter,
    notes: reasons.join('; '),
  };

  await persistWeights(weightsAfter, historyEntry);

  logger('calibration_completed', {
    batchId,
    shortlisted: shortlisted.length,
    rejected: rejected.length,
    deltas,
    notes: reasons,
  });

  return {
    batchId,
    sampledShortlisted: shortlisted.length,
    sampledRejected: rejected.length,
    weightsBefore,
    weightsAfter,
    deltas,
    reasons,
    historyEntry,
  };
}

function stratifiedSample(items: ScoredItem[], limit: number): ScoredItem[] {
  if (items.length <= limit) return [...items];
  const buckets = new Map<string, ScoredItem[]>();
  for (const item of items) {
    const key = item.eventClass || 'OTHER';
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(item);
  }
  const perBucket = Math.max(1, Math.floor(limit / Math.max(1, buckets.size)));
  const sample: ScoredItem[] = [];
  for (const group of buckets.values()) {
    shuffle(group);
    sample.push(...group.slice(0, perBucket));
    if (sample.length >= limit) break;
  }
  if (sample.length < limit) {
    const remaining = items.filter((item) => !sample.includes(item));
    shuffle(remaining);
    sample.push(...remaining.slice(0, limit - sample.length));
  }
  return sample.slice(0, limit);
}

function shuffle<T>(list: T[]): void {
  for (let i = list.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
}

interface LabelSummary {
  positivePrediction: Record<ImpactComponentKey, number>;
  negativePrediction: Record<ImpactComponentKey, number>;
  positiveTarget: Record<ImpactComponentKey, number>;
  negativeTarget: Record<ImpactComponentKey, number>;
}

function buildLabels(shortlisted: ScoredItem[], rejected: ScoredItem[]): LabelSummary {
  const positives = shortlisted.map((item) => scoreItem(item, 1));
  const negatives = rejected.map((item) => scoreItem(item, 0));

  const summary: LabelSummary = {
    positivePrediction: zeroComponentRecord(),
    negativePrediction: zeroComponentRecord(),
    positiveTarget: zeroComponentRecord(),
    negativeTarget: zeroComponentRecord(),
  };

  for (const key of Object.keys(summary.positivePrediction) as ImpactComponentKey[]) {
    summary.positivePrediction[key] = average(positives.map((entry) => entry.components[key]));
    summary.negativePrediction[key] = average(negatives.map((entry) => entry.components[key]));
    summary.positiveTarget[key] = average(positives.map((entry) => entry.targets[key]));
    summary.negativeTarget[key] = average(negatives.map((entry) => entry.targets[key]));
  }

  return summary;
}

interface ItemScoreSummary {
  components: Record<ImpactComponentKey, number>;
  targets: Record<ImpactComponentKey, number>;
  label: 0 | 1;
}

function scoreItem(item: ScoredItem, label: 0 | 1): ItemScoreSummary {
  const components = item.impact.components;
  const actionability = ACTIONABLE_CLASSES.has(item.eventClass) || item.impact.impact >= 0.72 ? 1 : 0;
  const novelty = components.graphNovelty >= 0.55 ? 1 : 0;
  const india = components.indiaTie >= 0.6 ? 1 : 0;
  const momentum = components.momentum >= 0.5 ? 1 : 0;
  const reach = (item.clusterImpact?.surfaceReach ?? components.surfaceReach) >= 0.6 ? 1 : 0;

  const targets: Record<ImpactComponentKey, number> = {
    recency: actionability,
    surfaceReach: reach,
    graphNovelty: novelty,
    authority: actionability,
    commerceTie: UPSIDE_CLASSES.has(item.eventClass) ? 1 : 0,
    indiaTie: india,
    momentum,
  };

  return {
    components,
    targets,
    label,
  };
}

function zeroComponentRecord(): Record<ImpactComponentKey, number> {
  return {
    recency: 0,
    surfaceReach: 0,
    graphNovelty: 0,
    authority: 0,
    commerceTie: 0,
    indiaTie: 0,
    momentum: 0,
  };
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  const sum = values.reduce((acc, value) => acc + value, 0);
  return sum / values.length;
}

function clampDelta(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value > MAX_DELTA) return MAX_DELTA;
  if (value < -MAX_DELTA) return -MAX_DELTA;
  return value;
}

function normalizeWeights(weights: ImpactWeights): ImpactWeights {
  const total = Object.values(weights).reduce((acc, val) => acc + (Number.isFinite(val) ? val : 0), 0);
  if (total <= 0) {
    return { ...weights };
  }
  const normalized: ImpactWeights = { ...weights };
  for (const key of Object.keys(weights) as ImpactComponentKey[]) {
    const value = weights[key];
    normalized[key] = Number.isFinite(value) && value >= 0 ? value / total : 0;
  }
  return normalized;
}

async function persistWeights(weights: ImpactWeights, entry: CalibrationHistoryEntry): Promise<void> {
  const file = (await readWeightsFile()) ?? (impactConfig as ImpactWeightsConfigFile);
  const history = Array.isArray(file.history) ? [...file.history] : [];
  history.push(entry);

  const output: ImpactWeightsConfigFile = {
    ...file,
    version: (file.version || 0) + 1,
    updatedAt: entry.timestamp,
    weights,
    history: history.slice(-50),
  };

  await fs.writeFile(WEIGHTS_PATH, JSON.stringify(output, null, 2), 'utf8');
}

async function readWeightsFile(): Promise<ImpactWeightsConfigFile | undefined> {
  try {
    const raw = await fs.readFile(WEIGHTS_PATH, 'utf8');
    return JSON.parse(raw) as ImpactWeightsConfigFile;
  } catch {
    return undefined;
  }
}

function buildReasons(labels: LabelSummary, deltas: Record<ImpactComponentKey, number>): string[] {
  const reasons: string[] = [];
  for (const [component, delta] of Object.entries(deltas) as Array<[ImpactComponentKey, number]>) {
    if (delta === 0) continue;
    const posGap = labels.positiveTarget[component] - labels.positivePrediction[component];
    const negGap = labels.negativePrediction[component] - labels.negativeTarget[component];
    const blended = posGap * 0.7 + negGap * 0.3;
    if (Math.abs(blended) < 0.01) continue;
    reasons.push(
      `${component}:gap=${blended.toFixed(2)} -> adjusted ${(delta >= 0 ? '+' : '')}${delta.toFixed(3)}`
    );
  }
  if (reasons.length === 0) reasons.push('No significant component gaps detected');
  return reasons;
}
