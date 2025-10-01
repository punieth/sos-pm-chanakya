import { describe, expect, it } from 'vitest';
import { scoreImpact } from '../analysis/impact';
import type { ClassifiedItem } from '../types';

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
		eventClass: overrides.eventClass || 'PAYMENTS',
		classConfidence: overrides.classConfidence ?? 0.8,
		classSignals: overrides.classSignals || { lexicon: 0.7, embedding: 0.6 },
		clusterId: overrides.clusterId,
	};
}

describe('impact scoring', () => {
	it('boosts graph novelty for new org pair', () => {
		const novel = makeClassified('a', { domain: 'reuters.com', clusterId: 'cluster-1' });
		const stale = makeClassified('b', { domain: 'example.net', clusterId: 'cluster-1' });
		const results = scoreImpact([novel, stale], {
			graphNovelty: { a: true, b: false },
			clusterDomainCounts: { 'cluster-1': 1 },
		});
		const novelScore = results.find((r) => r.id === 'a')!.impactScore;
		const staleScore = results.find((r) => r.id === 'b')!.impactScore;
		expect(novelScore).toBeGreaterThan(staleScore);
		expect(novelScore).toBeGreaterThan(0.5);
	});

	it('penalises stale items without trusted reach', () => {
		const item = makeClassified('c', {
			domain: 'unknown.io',
			eventClass: 'OTHER',
			description: 'Quarterly recap without clear action',
		});
		const [scored] = scoreImpact([item], {
			graphNovelty: { c: false },
			clusterDomainCounts: { c: 0 },
		});
		expect(scored.impactScore).toBeLessThan(0.4);
	});
});
