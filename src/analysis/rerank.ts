import { ClassifiedItem, ImpactResult } from '../types';
import { canonicalizeUrl } from '../utils/url';
import { tokenize } from '../utils/text';
import { isTrustedDomain } from './domains';

const SIMILARITY_THRESHOLD = 0.68;
const MMR_LAMBDA = 0.65;

interface ItemProfile {
	tokens: Set<string>;
	vector: Record<string, number>;
	canonicalUrl: string;
}

interface ClusterState {
	id: string;
	items: ClassifiedItem[];
	profile: ItemProfile;
	domainSet: Set<string>;
}

export interface ClusterPreparation {
	clusters: Record<string, ClusterState>;
	clusterDomainCounts: Record<string, number>;
	profiles: Record<string, ItemProfile>;
}

export function prepareClusters(items: ClassifiedItem[]): ClusterPreparation {
	const clusters: Record<string, ClusterState> = {};
	const profiles: Record<string, ItemProfile> = {};
	let clusterIndex = 0;

	for (const item of items) {
		const profile = buildProfile(item);
		profiles[item.id] = profile;

		const { bestClusterId, score } = findBestCluster(profile, clusters);
		if (!bestClusterId || score < SIMILARITY_THRESHOLD) {
			clusterIndex++;
			const id = `cluster-${clusterIndex}`;
			clusters[id] = createCluster(id, item, profile);
			item.clusterId = id;
		} else {
			const cluster = clusters[bestClusterId];
			cluster.items.push(item);
			cluster.domainSet.add(item.domain);
			cluster.profile = mergeProfiles(cluster.profile, profile);
			item.clusterId = cluster.id;
		}
	}

	const clusterDomainCounts: Record<string, number> = {};
	for (const [clusterId, cluster] of Object.entries(clusters)) {
		const trusted = Array.from(cluster.domainSet).filter(isTrustedDomain);
		clusterDomainCounts[clusterId] = trusted.length;
	}

	return { clusters, clusterDomainCounts, profiles };
}

export function rerankPrimaries(
	items: ImpactResult[],
	prep: ClusterPreparation,
	lambda = MMR_LAMBDA
): ImpactResult[] {
	const byCluster = new Map<string, ImpactResult[]>();
	for (const item of items) {
		const clusterId = item.clusterId || 'singles';
		if (!byCluster.has(clusterId)) byCluster.set(clusterId, []);
		byCluster.get(clusterId)!.push(item);
	}

	const primaries: ImpactResult[] = [];
	for (const [clusterId, clusterItems] of byCluster.entries()) {
		if (clusterItems.length === 0) continue;
		const ordered = mmrOrder(clusterItems, prep.profiles, lambda);
		const [head, ...rest] = ordered;
		if (!head) continue;
		head.duplicateUrls = rest.map((item) => item.url);
		head.impactBreakdown.surfaceReach = Math.min(
			1,
			head.impactBreakdown.surfaceReach + rest.length * 0.05
		);
		primaries.push(head);
	}

	return primaries.sort((a, b) => b.impactScore - a.impactScore);
}

function createCluster(id: string, item: ClassifiedItem, profile: ItemProfile): ClusterState {
	return {
		id,
		items: [item],
		profile,
		domainSet: new Set([item.domain]),
	};
}

function findBestCluster(profile: ItemProfile, clusters: Record<string, ClusterState>): {
	bestClusterId: string | null;
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
	return { bestClusterId, score: bestScore };
}

function mergeProfiles(a: ItemProfile, b: ItemProfile): ItemProfile {
	const tokens = new Set([...a.tokens, ...b.tokens]);
	const vector: Record<string, number> = {};
	for (const [term, weight] of Object.entries(a.vector)) {
		vector[term] = (vector[term] || 0) + weight;
	}
	for (const [term, weight] of Object.entries(b.vector)) {
		vector[term] = (vector[term] || 0) + weight;
	}
	const magnitude = Math.sqrt(Object.values(vector).reduce((acc, v) => acc + v * v, 0)) || 1;
	for (const key of Object.keys(vector)) vector[key] /= magnitude;
	return { tokens, vector, canonicalUrl: a.canonicalUrl || b.canonicalUrl };
}

function buildProfile(item: ClassifiedItem): ItemProfile {
	const canonicalUrl = canonicalizeUrl(item.canonicalUrl || item.url);
	const tokens = new Set(tokenize(item.title).filter(Boolean));
	const vector = buildVector(tokenize(`${item.title} ${item.description || ''}`));
	return { tokens, vector, canonicalUrl };
}

function buildVector(tokens: string[]): Record<string, number> {
	const vector: Record<string, number> = {};
	for (const token of tokens) {
		vector[token] = (vector[token] || 0) + 1;
	}
	const magnitude = Math.sqrt(Object.values(vector).reduce((acc, v) => acc + v * v, 0)) || 1;
	for (const key of Object.keys(vector)) {
		vector[key] /= magnitude;
	}
	return vector;
}

function similarity(a: ItemProfile, b: ItemProfile): number {
	if (a.canonicalUrl === b.canonicalUrl) return 1;
	const jaccard = jaccardSimilarity(a.tokens, b.tokens);
	const cosine = cosineSimilarity(a.vector, b.vector);
	return 0.6 * jaccard + 0.4 * cosine;
}

function jaccardSimilarity(a: Set<string>, b: Set<string>): number {
	if (a.size === 0 && b.size === 0) return 1;
	let intersection = 0;
	for (const token of a) {
		if (b.has(token)) intersection++;
	}
	const union = a.size + b.size - intersection;
	return union === 0 ? 0 : intersection / union;
}

function cosineSimilarity(a: Record<string, number>, b: Record<string, number>): number {
	let dot = 0;
	let magA = 0;
	let magB = 0;
	for (const weight of Object.values(a)) magA += weight * weight;
	for (const weight of Object.values(b)) magB += weight * weight;
	const magnitude = Math.sqrt(magA) * Math.sqrt(magB) || 1;
	for (const [term, weight] of Object.entries(a)) {
		dot += weight * (b[term] || 0);
	}
	return magnitude === 0 ? 0 : Math.min(1, dot / magnitude);
}

function mmrOrder(
	items: ImpactResult[],
	profiles: Record<string, ItemProfile>,
	lambda: number
): ImpactResult[] {
	const remaining = [...items];
	const selected: ImpactResult[] = [];
	while (remaining.length > 0) {
		let bestIndex = 0;
		let bestScore = -Infinity;
		for (let i = 0; i < remaining.length; i++) {
			const candidate = remaining[i];
			const relevance = candidate.impactScore;
			const diversity = selected.length === 0 ? 0 : maxSimilarity(candidate, selected, profiles);
			const score = lambda * relevance - (1 - lambda) * diversity;
			if (score > bestScore) {
				bestScore = score;
				bestIndex = i;
			}
		}
		selected.push(remaining.splice(bestIndex, 1)[0]);
	}
	return selected;
}

function maxSimilarity(candidate: ImpactResult, selected: ImpactResult[], profiles: Record<string, ItemProfile>): number {
	const candProfile = profiles[candidate.id] || buildFallbackProfile(candidate);
	let max = 0;
	for (const item of selected) {
		const profile = profiles[item.id] || buildFallbackProfile(item);
		const score = similarity(candProfile, profile);
		max = Math.max(max, score);
	}
	return max;
}

function buildFallbackProfile(item: ImpactResult): ItemProfile {
	return buildProfile(item);
}
