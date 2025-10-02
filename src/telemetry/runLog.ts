import path from 'node:path';
import { promises as fs } from 'node:fs';

import { ScoredItem, ClusterContext, EventClass, SourceProvider } from '../types';
import type { PipelineResult } from '../analysis/pipeline';
import type { CalibrationResult } from '../agent/calibrator';

export interface LlmRunStats {
  attempts: number;
  fallbacks: number;
  failures: number;
  providerBreakdown: Record<string, number>;
}

export interface RunLogProps {
  batchId: string;
  pipeline: PipelineResult;
  shortlisted: ScoredItem[];
  allItems: ScoredItem[];
  clusters: ClusterContext[];
  llm?: LlmRunStats;
  calibration?: CalibrationResult;
}

export interface RunLogEntry {
  batchId: string;
  timestamp: string;
  counts: {
    scanned: number;
    classified: number;
    clusters: number;
    shortlisted: number;
    impactQualified: number;
    noveltyHits: number;
    providerCounts: Record<SourceProvider, number>;
  };
  impactMetrics: {
    average: number;
    stdDev: number;
    medianSurfaceReach: number;
    percentOther: number;
    topClasses: Array<{ className: EventClass; share: number }>;
  };
  llm?: LlmRunStats;
  calibration?: CalibrationResult;
  alerts: string[];
  tasks: string[];
  indiaFloor: number;
  indiaCount: number;
}

interface RunHistoryFile {
  runs: RunLogEntry[];
  indiaFloor: number;
  indiaShortfallCount: number;
}

const HISTORY_PATH = path.join(process.cwd(), 'output', 'run-history.json');
const DEFAULT_INDIA_FLOOR = 0.6;
const MIN_INDIA_ITEMS = 3;
const INDIA_FLOOR_STEP = 0.05;
const INDIA_FLOOR_MIN = 0.3;

export async function logRun(props: RunLogProps): Promise<RunLogEntry> {
  const history = await readHistory();
  const now = new Date().toISOString();
  const shortlisted = props.shortlisted;

  const avgImpact = average(shortlisted.map((item) => item.impact.impact));
  const stdImpact = stdDev(shortlisted.map((item) => item.impact.impact), avgImpact);
  const medianSurfaceReach = median(shortlisted.map((item) => item.clusterImpact?.surfaceReach ?? 0));

  const percentOther = props.pipeline.stats.classDistribution.OTHER / Math.max(1, props.pipeline.stats.scanned);
  const topClassMix = topClasses(shortlisted);

  const alerts: string[] = [];
  const tasks: string[] = [];

  if (percentOther > 0.25) {
    alerts.push(`%OTHER above threshold at ${(percentOther * 100).toFixed(1)}%`);
    tasks.push('expand taxonomy seeds');
  }

  const previousMedian = rollingMedian(history.runs, 7);
  if (previousMedian !== undefined && medianSurfaceReach < previousMedian * 0.75) {
    alerts.push('Surface reach median dropped >25% vs trailing baseline');
    tasks.push('source expansion check');
  }

  const indiaFloor = history.indiaFloor ?? DEFAULT_INDIA_FLOOR;
  const indiaCount = shortlisted.filter((item) => item.impact.components.indiaTie >= indiaFloor).length;
  let indiaShortfallCount = history.indiaShortfallCount ?? 0;
  let currentFloor = indiaFloor;

  if (indiaCount < MIN_INDIA_ITEMS) {
    indiaShortfallCount += 1;
    if (indiaShortfallCount >= 2) {
      currentFloor = Math.max(INDIA_FLOOR_MIN, parseFloat((currentFloor - INDIA_FLOOR_STEP).toFixed(2)));
      alerts.push(`India floor relaxed to ${currentFloor.toFixed(2)}`);
      tasks.push('review India corpus freshness');
      indiaShortfallCount = 0;
    }
  } else {
    indiaShortfallCount = 0;
  }

  const entry: RunLogEntry = {
    batchId: props.batchId,
    timestamp: now,
    counts: {
      scanned: props.pipeline.stats.scanned,
      classified: props.pipeline.stats.classified,
      clusters: props.pipeline.stats.clusters,
      shortlisted: shortlisted.length,
      impactQualified: props.pipeline.stats.impactQualified,
      noveltyHits: props.pipeline.stats.noveltyHits,
      providerCounts: props.pipeline.providerCounts,
    },
    impactMetrics: {
      average: avgImpact,
      stdDev: stdImpact,
      medianSurfaceReach,
      percentOther,
      topClasses: topClassMix,
    },
    llm: props.llm,
    calibration: props.calibration,
    alerts,
    tasks,
    indiaFloor: currentFloor,
    indiaCount,
  };

  const updated: RunHistoryFile = {
    runs: [...history.runs, entry].slice(-200),
    indiaFloor: currentFloor,
    indiaShortfallCount,
  };

  await ensureHistoryDir();
  await fs.writeFile(HISTORY_PATH, JSON.stringify(updated, null, 2), 'utf8');

  return entry;
}

async function readHistory(): Promise<RunHistoryFile> {
  try {
    const raw = await fs.readFile(HISTORY_PATH, 'utf8');
    const parsed = JSON.parse(raw) as RunHistoryFile;
    return {
      runs: Array.isArray(parsed.runs) ? parsed.runs : [],
      indiaFloor: parsed.indiaFloor ?? DEFAULT_INDIA_FLOOR,
      indiaShortfallCount: parsed.indiaShortfallCount ?? 0,
    };
  } catch {
    return { runs: [], indiaFloor: DEFAULT_INDIA_FLOOR, indiaShortfallCount: 0 };
  }
}

async function ensureHistoryDir(): Promise<void> {
  const dir = path.dirname(HISTORY_PATH);
  try {
    await fs.mkdir(dir, { recursive: true });
  } catch {
    /* noop */
  }
}

function average(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function stdDev(values: number[], mean: number): number {
  if (!values.length) return 0;
  const variance = values.reduce((acc, value) => {
    const diff = value - mean;
    return acc + diff * diff;
  }, 0) / values.length;
  return Math.sqrt(variance);
}

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function topClasses(items: ScoredItem[]): Array<{ className: EventClass; share: number }> {
  if (!items.length) return [];
  const counts = new Map<EventClass, number>();
  for (const item of items) {
    counts.set(item.eventClass, (counts.get(item.eventClass) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([className, count]) => ({ className, share: count / items.length }));
}

function rollingMedian(entries: RunLogEntry[], windowSize: number): number | undefined {
  if (!entries.length) return undefined;
  const recent = entries.slice(-windowSize);
  if (!recent.length) return undefined;
  const values = recent.map((entry) => entry.impactMetrics.medianSurfaceReach);
  if (!values.length) return undefined;
  return median(values);
}
