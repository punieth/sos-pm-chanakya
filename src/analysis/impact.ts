import { ClassifiedItem, ImpactResult } from '../types';
import { DAY_MS, nowUtc, parseUtc } from '../utils/time';
import { isTrustedDomain } from './domains';
import { indiaRelevanceScore, regulatorSignalScore } from '../utils/india';

const COMMERCE_LEXICON = [
	'payment',
	'payments',
	'wallet',
	'checkout',
	'commerce',
	'transaction',
	'merchant',
	'upi',
	'pricing',
	'fee',
	'subscription',
	'billing',
];

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
	const commerceTie = commerceTieScore(item);
	const indiaTie = indiaRelevanceScore({
		title: item.title,
		description: item.description,
		source: item.source,
		domain: item.domain,
	});
	const momentum = eventMomentumScore(item.eventClass);
	const authorityTie = regulatorSignalScore({
		title: item.title,
		description: item.description,
		source: item.source,
		domain: item.domain,
	});

	let impact =
		recency * 0.22 +
		graphNovelty * 0.17 +
		surfaceReach * 0.13 +
		commerceTie * 0.1 +
		indiaTie * 0.22 +
		momentum * 0.05 +
		authorityTie * 0.11;

	if (authorityTie >= 0.45) {
		impact += 0.12;
	} else if (authorityTie >= 0.3) {
		impact += 0.06;
	}

	if (indiaTie >= 0.65) impact += 0.04;

	impact = clamp(impact);

	return {
		...item,
		impactScore: impact,
		impactBreakdown: {
			recency,
			graphNovelty,
			surfaceReach,
			commerceTie,
			indiaTie,
			momentum,
			authority: authorityTie,
		},
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

function commerceTieScore(item: ClassifiedItem): number {
	if (item.eventClass === 'COMMERCE') return 1;
	const text = `${item.title} ${item.description || ''}`.toLowerCase();
	return COMMERCE_LEXICON.some((word) => text.includes(word)) ? 1 : 0;
}

function eventMomentumScore(eventClass: ClassifiedItem['eventClass']): number {
	if (eventClass === 'LAUNCH') return 0.9;
	if (eventClass === 'PARTNERSHIP') return 0.7;
	if (eventClass === 'POLICY') return 0.85;
	if (eventClass === 'COMMERCE') return 0.8;
	return 0.4;
}

function clamp(n: number): number {
	if (Number.isNaN(n)) return 0;
	if (n < 0) return 0;
	if (n > 1) return 1;
	return Number.isFinite(n) ? n : 0;
}
