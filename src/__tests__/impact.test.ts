import { describe, expect, it } from 'vitest';
import { scoreItems } from '../analysis/impact';
import type { ClassifiedItem, ClusterImpactSummary } from '../types';

function makeClassified(id: string, overrides: Partial<ClassifiedItem> = {}): ClassifiedItem {
  return {
    id,
    title: overrides.title || `Item ${id}`,
    description: overrides.description || 'Fresh launch with new payment flow',
    url: overrides.url || `https://example.com/${id}`,
    canonicalUrl: overrides.canonicalUrl || `https://example.com/${id}`,
    publishedAt: overrides.publishedAt || new Date().toISOString(),
    source: overrides.source || 'Example',
    provider: overrides.provider || 'newsapi',
    domain: overrides.domain || 'example.com',
    eventClass: overrides.eventClass || 'PAYMENT_COMMERCE',
    classConfidence: overrides.classConfidence ?? 0.8,
    classSignals: overrides.classSignals || { lexicon: 0.7, embedding: 0.6, clusterVoting: 0 },
    clusterId: overrides.clusterId,
  };
}

describe('impact scoring', () => {
  it('boosts graph novelty for new org pair', () => {
    const novel = makeClassified('a', { domain: 'reuters.com', clusterId: 'cluster-1' });
    const stale = makeClassified('b', { domain: 'example.net', clusterId: 'cluster-1' });
    const clusterSignals: Record<string, ClusterImpactSummary> = {
      'cluster-1': {
        id: 'cluster-1',
        surfaceReach: 0.5,
        distinctDomains: 2,
        trustedDomains: 1,
        velocity: 0.45,
        totalItems: 2,
        windowHours: 72,
      },
    };
    const results = scoreItems([novel, stale], {
      graphNovelty: { a: true, b: false },
      clusterSignals,
    });
    const novelScore = results.find((r) => r.id === 'a')!.impact.impact;
    const staleScore = results.find((r) => r.id === 'b')!.impact.impact;
    expect(novelScore).toBeGreaterThan(staleScore);
    expect(novelScore).toBeGreaterThan(0.5);
  });

  it('penalises stale items without trusted reach', () => {
    const item = makeClassified('c', {
      domain: 'unknown.io',
      eventClass: 'TREND_ANALYSIS',
      description: 'Quarterly recap without clear action',
      publishedAt: new Date(Date.now() - 10 * 86400000).toISOString(),
    });
    const clusterSignals: Record<string, ClusterImpactSummary> = {
      c: {
        id: 'c',
        surfaceReach: 0,
        distinctDomains: 1,
        trustedDomains: 0,
        velocity: 0.05,
        totalItems: 1,
        windowHours: 72,
      },
    };
    const [scored] = scoreItems([item], {
      graphNovelty: { c: false },
      clusterSignals,
    });
    expect(scored.impact.impact).toBeLessThan(0.4);
  });
});
