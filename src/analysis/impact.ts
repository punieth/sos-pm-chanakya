import { ClassifiedItem, ImpactResult } from '../types';
import { DAY_MS, nowUtc, parseUtc } from '../utils/time';
import { isTrustedDomain } from './domains';

const MARKET_LEXICON = ['payment', 'payments', 'wallet', 'checkout', 'commerce', 'transaction', 'merchant', 'upi'];

export interface ImpactOptions {
	now?: Date;
	graphNovelty?: Record<string, boolean>;
	clusterDomainCounts?: Record<string, number>;
}

const RECENCY_WINDOW_MS = 3 * DAY_MS;

export function scoreImpact(items: ClassifiedItem[], options: ImpactOptions = {}): ImpactResult[] {
	return items.map((item) => computeImpact(item, options));
}

export function computeImpact(item: ClassifiedItem, options: ImpactOptions = {}): ImpactResult {
	const now = options.now || nowUtc();
	const graphNoveltyMap = options.graphNovelty || {};
	const clusterDomains = options.clusterDomainCounts || {};

	const recency = recencyScore(item.publishedAt, now);
	const graphNovelty = graphNoveltyMap[item.id] ? 1 : 0;
	const surfaceReach = surfaceReachScore(item, clusterDomains);
	const marketTie = marketTieScore(item);

	const impact = clamp(recency * 0.45 + graphNovelty * 0.25 + surfaceReach * 0.2 + marketTie * 0.1);

	return {
		...item,
		impactScore: impact,
		impactBreakdown: { recency, graphNovelty, surfaceReach, marketTie },
		trustedDomainCount: clusterDomains[item.clusterId || item.id] || (isTrustedDomain(item.domain) ? 1 : 0),
		graphNovelty,
	};
}

function recencyScore(publishedAt: string, now: Date): number {
	const published = parseUtc(publishedAt);
	if (!published) return 0.1;
	const diff = now.getTime() - published.getTime();
	if (diff <= 0) return 1;
	const score = Math.exp(-diff / RECENCY_WINDOW_MS);
	return clamp(score);
}

function surfaceReachScore(item: ClassifiedItem, counts: Record<string, number>): number {
	const key = item.clusterId || item.id;
	const count = counts[key];
	if (typeof count === 'number') {
		return clamp(count / 3);
	}
	return isTrustedDomain(item.domain) ? 0.4 : 0.2;
}

function marketTieScore(item: ClassifiedItem): number {
	if (item.eventClass === 'PAYMENTS') return 1;
	const text = `${item.title} ${item.description || ''}`.toLowerCase();
	return MARKET_LEXICON.some((word) => text.includes(word)) ? 1 : 0;
}

function clamp(n: number): number {
	if (Number.isNaN(n)) return 0;
	if (n < 0) return 0;
	if (n > 1) return 1;
	return Number.isFinite(n) ? n : 0;
}
