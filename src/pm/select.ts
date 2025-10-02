import { ImpactResult } from '../types';
import { sanitize, tokenize } from '../utils/text';
import { indiaRelevanceScore, isIndiaRelevant } from '../utils/india';
import { resolvePmTuning, type PmTuning, type TopicKey } from '../config/pmTuning';

const STOPWORDS = new Set(['india', 'indian', 'update', 'policy', 'report', 'launch', 'new', 'latest']);

const COMMON_STOPWORDS = new Set([
	'a',
	'an',
	'and',
	'are',
	'as',
	'at',
	'be',
	'because',
	'been',
	'before',
	'being',
	'between',
	'but',
	'by',
	'can',
	'could',
	'did',
	'do',
	'does',
	'done',
	'during',
	'each',
	'for',
	'from',
	'has',
	'have',
	'having',
	'he',
	'her',
	'here',
	'him',
	'his',
	'how',
	'i',
	'if',
	'in',
	'into',
	'is',
	'it',
	'its',
	'just',
	'like',
	'made',
	'make',
	'many',
	'may',
	'might',
	'much',
	'must',
	'no',
	'not',
	'now',
	'of',
	'on',
	'once',
	'one',
	'only',
	'or',
	'other',
	'our',
	'out',
	'over',
	'said',
	'say',
	'says',
	'should',
	'so',
	'some',
	'such',
	'than',
	'that',
	'the',
	'their',
	'them',
	'then',
	'there',
	'these',
	'they',
	'this',
	'those',
	'through',
	'to',
	'under',
	'until',
	'up',
	'was',
	'were',
	'what',
	'when',
	'where',
	'which',
	'while',
	'who',
	'will',
	'with',
	'within',
	'without',
	'would',
	'you',
	'your',
	'news',
	'latest',
	'live',
	'update',
	'updates',
	'report',
	'reports',
	'article',
	'coverage',
	'analysis',
	'breaking',
	'today',
	'tonight',
	'morning',
	'evening',
	'watch',
	'video',
	'page',
	'full',
	'joins',
	'round',
	'press',
	'wire',
	'new',
	'india',
]);

const TOKEN_CATEGORY_PATTERNS: Array<{ label: string; regex: RegExp }> = [
	{ label: 'pricing', regex: /^(fee|feeless|charg|levy|tariff|pric|cost|surcharge|commission|duty|tax|waiv|free)$/ },
	{ label: 'security', regex: /^(secur|fraud|verif|risk|guard|phish|scam|auth|otp|token|breach)$/ },
	{ label: 'policy', regex: /^(policy|circular|mandate|regulat|guideline)$/ },
	{ label: 'liquidity', regex: /^(liquid|cash|capital|buffer)$/ },
	{ label: 'interest', regex: /^(rate|repo|reverse|ibor|yield|interest)$/ },
	{ label: 'governance', regex: /^(board|govern|chair|committee)$/ },
	{ label: 'payments', regex: /^(payment|settle|upi|npci|remit|transaction|merchant)$/ },
];

const CLASS_WEIGHT: Record<ImpactResult['eventClass'], number> = {
	LAUNCH: 0.12,
	PARTNERSHIP: 0.08,
	POLICY: 0.15,
	COMMERCE: 0.14,
	TREND: 0,
	OTHER: 0,
};

const SUPPORTED_LANGS = new Set(['en', 'en-us', 'en-gb', 'hi', 'en_in', 'en-in']);

const ENTERTAINMENT_KEYWORDS = new Set([
	'cricket',
	'match',
	'tournament',
	'league',
	'goal',
	'score',
	'anime',
	'manga',
	'celebrity',
	'actor',
	'actress',
	'singer',
	'music',
	'film',
	'movie',
	'hollywood',
	'bollywood',
	'wedding',
	'twins',
	'baby',
	'coach',
	'player',
	'world cup',
	'series',
]);

const MARKET_NOISE_PATTERNS = [
	'ahead of market',
	'stocks to watch',
	'pre-market',
	'opening bell',
	'closing bell',
	'market wrap',
	'market live updates',
	'midday market check',
];

const IPO_KEYWORDS = [
	' ipo',
	'initial public offer',
	'initial public offering',
	'public issue',
	'shares list',
	'shares listing',
	'lists on bse',
	'lists on nse',
	'listing on bse',
	'listing on nse',
	'stock market debut',
];

function clamp(value: number, min: number, max: number): number {
	if (Number.isNaN(value) || !Number.isFinite(value)) return min;
	return Math.max(min, Math.min(max, value));
}

export interface RankedCandidate {
	item: ImpactResult;
	baseScore: number;
	finalScore: number;
	indiaScore: number;
	productScore: number;
	topicScores: Record<TopicKey, number>;
	dominantTopic: TopicKey;
}

export interface ShortlistedCandidate {
	item: ImpactResult;
	pmScore: number;
	indiaScore: number;
	productScore: number;
	finalScore: number;
	topicScores: Record<TopicKey, number>;
	topic: TopicKey;
	signals: string[];
	urgency: '⚡' | '🛑' | '🧩';
}

export function selectPmStories(
	items: ImpactResult[],
	limit: number,
	tuningOverrides?: Partial<PmTuning>
): ShortlistedCandidate[] {
	const tuning = resolvePmTuning(tuningOverrides);
	const supported = items.filter(isSupportedLanguage);
	const ranked = rankForPm(supported, tuning);
	console.log('PM_TOP_PRE', ranked.slice(0, 10).map((r) => ({
		title: r.item.title,
		eventClass: r.item.eventClass,
		baseScore: r.baseScore.toFixed(3),
		finalScore: r.finalScore.toFixed(3),
		topic: r.dominantTopic,
		indiaScore: r.indiaScore.toFixed(3),
		topicScores: r.topicScores,
	}))); 
	const maxItems = Math.max(1, Math.min(limit, tuning.maxShortlist));
	const deduped = dedupeByCluster(ranked);
	const topicFiltered = dedupeByTopic(deduped);
	const withQuotas = applyTopicQuotas(topicFiltered, tuning, maxItems);
	const withEnglish = dedupeByUrl(withQuotas)
		.filter(({ item }) => !isMarketNoise(item))
		.filter(({ item }) => !isEntertainment(item));
	const shortlist = withEnglish
		.slice(0, maxItems)
		.map((entry) => ({
			item: entry.item,
			pmScore: entry.baseScore,
			indiaScore: entry.indiaScore,
			productScore: entry.topicScores.product,
			finalScore: entry.finalScore,
			topicScores: entry.topicScores,
			topic: entry.dominantTopic,
			signals: deriveSignals(entry.item),
			urgency: deriveUrgency(entry.baseScore, entry.item, entry.topicScores, tuning),
		}));
	console.log('PM_TOP_POST', shortlist.map((r) => ({
		title: r.item.title,
		eventClass: r.item.eventClass,
		baseScore: r.pmScore.toFixed(3),
		finalScore: r.finalScore.toFixed(3),
		indiaScore: r.indiaScore.toFixed(3),
		topic: r.topic,
	}))); 
	return shortlist;
}

export function rankForPm(items: ImpactResult[], tuning: PmTuning): RankedCandidate[] {
	const weightFractions = getWeightFractions(tuning.topicWeights);
	return items
		.map((item) => {
			const indiaScore = indiaRelevanceScore({
				title: item.title,
				description: item.description,
				source: item.source,
				domain: item.domain,
			});
			const topicScores = computeTopicScores(item);
			const baseScore = computeBaseScore(item, indiaScore);
			const weightedTopicScore =
				topicScores.regulation * weightFractions.regulation +
				topicScores.product * weightFractions.product +
				topicScores.ai * weightFractions.ai +
				topicScores.other * weightFractions.other;
			const baseFactor = 0.2 + 0.8 * weightFractions.regulation;
			const finalScore = clamp(baseScore * baseFactor + weightedTopicScore, 0, 1);
			const dominantTopic = resolveDominantTopic(topicScores);
			const ranked: RankedCandidate = {
				item,
				baseScore,
				finalScore,
				indiaScore,
				productScore: topicScores.product,
				topicScores,
				dominantTopic,
			};
			console.log('PM_RANK', {
				title: item.title,
				eventClass: item.eventClass,
				impact: item.impactScore.toFixed(3),
				base: baseScore.toFixed(3),
				final: finalScore.toFixed(3),
				topicScores,
				dominantTopic,
			});
			return ranked;
		})
		.sort((a, b) => b.finalScore - a.finalScore);
}

function computeBaseScore(item: ImpactResult, indiaScore: number): number {
	let score = item.impactScore;
	score += indiaScore * 0.2;
	if (indiaScore < 0.2) score -= 0.1;
	score += CLASS_WEIGHT[item.eventClass] || 0;
	if ((item.trustedDomainCount || 0) > 1) score += 0.05;
	const authority = item.impactBreakdown.authority || 0;
	if (authority >= 0.45) score += 0.08;
	else if (authority >= 0.3) score += 0.04;
	return clamp(score, 0, 1);
}

function deriveSignals(item: ImpactResult): string[] {
	const tokens = tokenize(sanitize(`${item.title || ''} ${item.description || ''}`));
	const counts: Record<string, number> = {};
	for (const token of tokens) {
		if (token.length < 4) continue;
		if (STOPWORDS.has(token)) continue;
		counts[token] = (counts[token] || 0) + 1;
	}
	return Object.entries(counts)
		.sort((a, b) => b[1] - a[1])
		.map(([token]) => token)
		.slice(0, 2);
}

function deriveUrgency(
	pmScore: number,
	item: ImpactResult,
	topicScores: Record<TopicKey, number>,
	tuning: PmTuning
): '⚡' | '🛑' | '🧩' {
	if (item.eventClass === 'POLICY' && pmScore >= 0.6) return '🛑';
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase();
	if (item.eventClass === 'LAUNCH' && isIpoStory(text)) return '⚡';
	if (topicScores.product >= 0.5 && pmScore >= 0.55) return '⚡';
	if (topicScores.ai >= 0.6 && pmScore >= 0.5) return '⚡';
	if (pmScore >= 0.72) return '⚡';
	return '🧩';
}

function isIpoStory(text: string): boolean {
	if (!text) return false;
	return IPO_KEYWORDS.some((keyword) => text.includes(keyword));
}

function computeTopicScores(item: ImpactResult): Record<TopicKey, number> {
	const authority = clamp(item.impactBreakdown?.authority || 0, 0, 1);
	const product = productSignalScore(item);
	const ai = aiSignalScore(item);
	const momentum = clamp(item.impactBreakdown?.momentum || 0, 0, 1);
	const novelty = clamp(item.impactBreakdown?.graphNovelty || 0, 0, 1);
	const otherBase = clamp(item.impactScore, 0, 1);
	let other = clamp(otherBase * 0.4 + momentum * 0.2 + novelty * 0.1, 0, 0.6);
	if (authority >= 0.35) {
		other = Math.min(other, 0.3);
	}
	return {
		regulation: authority,
		product,
		ai,
		other,
	};
}

function resolveDominantTopic(topicScores: Record<TopicKey, number>): TopicKey {
	if (topicScores.regulation >= 0.35) return 'regulation';
	let best: TopicKey = 'other';
	let bestScore = -Infinity;
	for (const topic of Object.keys(topicScores) as TopicKey[]) {
		const value = topicScores[topic];
		if (value > bestScore) {
			bestScore = value;
			best = topic;
		}
	}
	return best;
}

function getWeightFractions(weights: Record<TopicKey, number>): Record<TopicKey, number> {
	const total = Object.values(weights).reduce((sum, value) => sum + value, 0) || 1;
	return {
		regulation: weights.regulation / total,
		product: weights.product / total,
		ai: weights.ai / total,
		other: weights.other / total,
	};
}

function applyTopicQuotas(list: RankedCandidate[], tuning: PmTuning, maxItems: number): RankedCandidate[] {
	if (list.length <= maxItems) return list;
	const quotas = computeTopicQuotas(tuning.topicWeights, maxItems);
	const counts: Record<TopicKey, number> = { regulation: 0, product: 0, ai: 0, other: 0 };
	const selected: RankedCandidate[] = [];
	const leftovers: RankedCandidate[] = [];
	const seen = new Set<string>();
	for (const entry of list) {
		const topic = entry.dominantTopic;
		if (counts[topic] < quotas[topic]) {
			selected.push(entry);
			counts[topic]++;
			seen.add(entry.item.id);
		} else {
			leftovers.push(entry);
		}
		if (selected.length >= maxItems) break;
	}
	if (selected.length < maxItems) {
		for (const entry of leftovers) {
			if (selected.length >= maxItems) break;
			if (seen.has(entry.item.id)) continue;
			selected.push(entry);
			seen.add(entry.item.id);
		}
	}
	if (selected.length < maxItems) {
		for (const entry of list) {
			if (selected.length >= maxItems) break;
			if (seen.has(entry.item.id)) continue;
			selected.push(entry);
			seen.add(entry.item.id);
		}
	}
	return selected.sort((a, b) => b.finalScore - a.finalScore);
}

function computeTopicQuotas(weights: Record<TopicKey, number>, maxItems: number): Record<TopicKey, number> {
	const fractions = getWeightFractions(weights);
	const raw: Record<TopicKey, number> = {
		regulation: fractions.regulation * maxItems,
		product: fractions.product * maxItems,
		ai: fractions.ai * maxItems,
		other: fractions.other * maxItems,
	};
	const quotas: Record<TopicKey, number> = {
		regulation: Math.floor(raw.regulation),
		product: Math.floor(raw.product),
		ai: Math.floor(raw.ai),
		other: Math.floor(raw.other),
	};
	let allocated = Object.values(quotas).reduce((sum, value) => sum + value, 0);
	const topics = (['regulation', 'product', 'ai', 'other'] as TopicKey[]).filter((topic) => weights[topic] > 0);
	for (const topic of topics) {
		if (quotas[topic] === 0) {
			quotas[topic] = 1;
			allocated++;
		}
	}
	if (allocated > maxItems) {
		while (allocated > maxItems) {
			const topic = topics.sort((a, b) => quotas[b] - quotas[a])[0];
			if (quotas[topic] > 0) {
				quotas[topic]--;
				allocated--;
			} else {
				break;
			}
		}
	} else if (allocated < maxItems) {
		const remainders = (['regulation', 'product', 'ai', 'other'] as TopicKey[])
			.map((topic) => ({ topic, remainder: raw[topic] - quotas[topic] }))
			.sort((a, b) => b.remainder - a.remainder);
		let idx = 0;
		while (allocated < maxItems && idx < remainders.length) {
			const topic = remainders[idx].topic;
			quotas[topic]++;
			allocated++;
			idx++;
		}
	}
	return quotas;
}

function dedupeByUrl(list: RankedCandidate[]): RankedCandidate[] {
	const seen = new Set<string>();
	const out: RankedCandidate[] = [];
	for (const entry of list) {
		const key = entry.item.canonicalUrl || entry.item.url;
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(entry);
	}
	return out;
}

function dedupeByCluster(list: RankedCandidate[]): RankedCandidate[] {
	const seen = new Set<string>();
	const out: RankedCandidate[] = [];
	for (const entry of list) {
		const clusterKey = entry.item.clusterId;
		if (clusterKey && seen.has(clusterKey)) continue;
		if (clusterKey) seen.add(clusterKey);
		out.push(entry);
	}
	return out;
}

function dedupeByTopic(list: RankedCandidate[]): RankedCandidate[] {
	if (list.length <= 1) return list;
	const profiles = buildTopicProfiles(list);
	const out: RankedCandidate[] = [];
	for (const entry of list) {
		const profile = profiles.get(entry.item.id);
		if (!profile) {
			out.push(entry);
			continue;
		}
		let duplicate = false;
		for (const existing of out) {
			const existingProfile = profiles.get(existing.item.id);
			if (!existingProfile) continue;
			if (isSameTopic(profile, existingProfile)) {
				duplicate = true;
				break;
			}
		}
		if (!duplicate) out.push(entry);
	}
	return out;
}

interface TopicProfile {
	vector: Map<string, number>;
	norm: number;
	topTokens: string[];
	simhash: bigint;
}

function buildTopicProfiles(list: RankedCandidate[]): Map<string, TopicProfile> {
	const documents = new Map<string, Map<string, number>>();
	const documentFrequency = new Map<string, number>();
	const totalDocs = list.length || 1;

	for (const entry of list) {
		const counts = new Map<string, number>();
		const unique = new Set<string>();
		const text = buildTopicText(entry.item);
		if (text) {
			const rawTokens = tokenize(text);
			const contentTokens: string[] = [];
			for (const raw of rawTokens) {
				const norm = normalizeTopicToken(raw);
				if (!norm) continue;
				if (COMMON_STOPWORDS.has(norm)) continue;
				if (norm.length <= 2 && !/^\d+$/.test(norm)) continue;
				contentTokens.push(norm);
				incrementTerm(norm, 1);
				const category = categorizeToken(norm);
				if (category) incrementTerm(category, 0.9);
			}

			addShingles(contentTokens, 2, 1.4);
			addShingles(contentTokens, 3, 1.2);

			const acronyms = text.match(/\b[A-Z0-9]{3,}\b/g) || [];
			for (const acronym of acronyms) {
				const norm = normalizeTopicToken(acronym.toLowerCase());
				if (!norm) continue;
				if (COMMON_STOPWORDS.has(norm)) continue;
				incrementTerm(norm, 2);
				const category = categorizeToken(norm);
				if (category) incrementTerm(category, 1.8);
			}
		}
		documents.set(entry.item.id, counts);

		function incrementTerm(term: string, weight: number) {
			counts.set(term, (counts.get(term) || 0) + weight);
			if (!unique.has(term)) {
				unique.add(term);
				documentFrequency.set(term, (documentFrequency.get(term) || 0) + 1);
			}
		}

		function addShingles(tokens: string[], size: number, weight: number) {
			if (tokens.length < size) return;
			for (let i = 0; i <= tokens.length - size; i++) {
				const parts = tokens.slice(i, i + size);
				if (parts.some((part) => part.length <= 2)) continue;
				const shingle = `#${parts.join('_')}`;
				incrementTerm(shingle, weight);
			}
		}
	}

	const profiles = new Map<string, TopicProfile>();
	const highFrequencyCutoff = Math.max(2, Math.ceil(list.length * 0.6));
	for (const entry of list) {
		const counts = documents.get(entry.item.id) || new Map<string, number>();
		const vector = new Map<string, number>();
		const ranked: Array<{ token: string; weight: number }> = [];
		let normSq = 0;
		const weightedTokens: Array<{ token: string; weight: number }> = [];
		for (const [token, tf] of counts.entries()) {
			if (tf <= 0) continue;
			const df = documentFrequency.get(token) || 1;
			if (df >= highFrequencyCutoff && !token.startsWith('@') && token.length > 3 && !/^\d+$/.test(token)) continue;
			const idf = Math.log(1 + totalDocs / df);
			const weight = tf * idf;
			if (weight <= 0) continue;
			vector.set(token, weight);
			normSq += weight * weight;
			ranked.push({ token, weight });
			weightedTokens.push({ token, weight });
		}
		ranked.sort((a, b) => {
			if (b.weight !== a.weight) return b.weight - a.weight;
			return a.token < b.token ? -1 : a.token > b.token ? 1 : 0;
		});
		const topTokens = ranked.slice(0, 6).map(({ token }) => token);
		const simhash = computeSimhash(weightedTokens);
		profiles.set(entry.item.id, {
			vector,
			norm: normSq > 0 ? Math.sqrt(normSq) : 0,
			topTokens,
			simhash,
		});
	}

	return profiles;
}

function buildTopicText(item: ImpactResult): string {
	const parts: string[] = [];
	if (item.title) parts.push(stripSourceSuffix(item.title));
	if (item.description) parts.push(item.description);
	return sanitize(parts.join(' '));
}

function stripSourceSuffix(title: string): string {
	const separators = [' - ', ' | ', ' — ', ' – '];
	for (const separator of separators) {
		const index = title.lastIndexOf(separator);
		if (index > 0 && index >= title.length * 0.5) {
			return title.slice(0, index);
		}
	}
	return title;
}

function normalizeTopicToken(token: string): string {
	let value = token.trim().toLowerCase();
	value = value.replace(/^[^a-z0-9]+|[^a-z0-9]+$/g, '');
	if (!value) return '';
	if (value.length > 4) {
		if (value.endsWith('ies')) value = `${value.slice(0, -3)}y`;
		else if (value.endsWith('ing')) value = value.slice(0, -3);
		else if (value.endsWith('ied')) value = `${value.slice(0, -3)}y`;
		else if (value.endsWith('ed')) value = value.slice(0, -2);
		else if (value.endsWith('es')) value = value.slice(0, -2);
	}
	if (value.endsWith('s') && value.length > 3 && !value.endsWith('ss')) value = value.slice(0, -1);
	return value;
}

function categorizeToken(token: string): string | null {
	for (const { label, regex } of TOKEN_CATEGORY_PATTERNS) {
		if (regex.test(token)) return `@${label}`;
	}
	return null;
}

function productSignalScore(item: ImpactResult): number {
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase();
	if (!text) return 0;
	let score = 0;
	const CORE_TERMS = ['launch', 'launched', 'introduce', 'unveil', 'deploy', 'release', 'preview', 'beta', 'ship'];
	const PRODUCT_TERMS = ['platform', 'suite', 'feature', 'product', 'tool', 'sdk', 'api', 'integration', 'workflow', 'stack', 'module'];
	for (const term of CORE_TERMS) {
		if (text.includes(term)) score += 0.08;
	}
	for (const term of PRODUCT_TERMS) {
		if (text.includes(term)) score += 0.09;
	}
	if (text.includes('model') || text.includes('models')) score += 0.08;
	if (text.includes('upgrade') || text.includes('update')) score += 0.05;
	if (text.includes('developer') || text.includes('developers')) score += 0.05;
	if (text.includes('automation')) score += 0.05;
	if (/\bgen(?:\s|-)ai\b/.test(text)) score += 0.12;
	if (text.includes('api')) score += 0.05;
	if (text.includes('sdk')) score += 0.05;
	if (text.includes('india')) score += 0.04;
	return Math.min(1, score);
}

function aiSignalScore(item: ImpactResult): number {
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase();
	if (!text) return 0;
	let score = 0;
	const CORE_AI = ['ai', 'artificial intelligence', 'generative ai', 'gen ai', 'machine learning', 'ml ', ' llm', 'foundation model', 'autogen', 'agent', 'copilot'];
	const ACTIONS = ['launch', 'unveil', 'introduce', 'ship', 'preview', 'beta', 'general availability', 'ga', 'update'];
	for (const term of CORE_AI) {
		if (text.includes(term)) score += 0.12;
	}
	for (const term of ACTIONS) {
		if (text.includes(term)) score += 0.05;
	}
	if (text.includes('open source')) score += 0.05;
	if (text.includes('api')) score += 0.05;
	if (text.includes('sdk')) score += 0.05;
	return Math.min(1, score);
}

function isSameTopic(a: TopicProfile, b: TopicProfile): boolean {
	if (a.topTokens.length === 0 || b.topTokens.length === 0) return false;
	const setA = new Set(a.topTokens);
	const setB = new Set(b.topTokens);
	let intersection = 0;
	for (const token of setA) {
		if (setB.has(token)) intersection++;
	}
	const unionSize = new Set([...setA, ...setB]).size;
	const jaccard = unionSize === 0 ? 0 : intersection / unionSize;

	let cosine = 0;
	if (a.norm > 0 && b.norm > 0) {
		const [shorter, longer] = a.vector.size <= b.vector.size ? [a.vector, b.vector] : [b.vector, a.vector];
		for (const [token, weight] of shorter.entries()) {
			const otherWeight = longer.get(token);
			if (otherWeight !== undefined) cosine += weight * otherWeight;
		}
		cosine /= a.norm * b.norm;
	}

	const shared = intersection;
	const hamming = hammingDistance(a.simhash, b.simhash);
	if (hamming >= 0 && hamming <= 12) return true;
	if (shared >= 3) return true;
	if (jaccard >= 0.62 || cosine >= 0.9) return true;
	if (shared >= 2 && (cosine >= 0.58 || jaccard >= 0.32 || hamming <= 16)) return true;
	return jaccard >= 0.45 && cosine >= 0.75;
}

function computeSimhash(tokens: Array<{ token: string; weight: number }>): bigint {
	if (tokens.length === 0) return 0n;
	const bits = new Array<number>(64).fill(0);
	for (const { token, weight } of tokens) {
		const hash = fnv1a64(token);
		for (let i = 0; i < 64; i++) {
			const bit = Number((hash >> BigInt(i)) & 1n);
			bits[i] += bit === 1 ? weight : -weight;
		}
	}
	let result = 0n;
	for (let i = 0; i < 64; i++) {
		if (bits[i] > 0) result |= 1n << BigInt(i);
	}
	return result;
}

function fnv1a64(text: string): bigint {
	let hash = 0xcbf29ce484222325n;
	for (let i = 0; i < text.length; i++) {
		hash ^= BigInt(text.charCodeAt(i));
		hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
	}
	return hash;
}

function hammingDistance(a: bigint, b: bigint): number {
	let x = a ^ b;
	let count = 0;
	while (x) {
		x &= x - 1n;
		count++;
	}
	return count;
}

function isEntertainment(item: ImpactResult): boolean {
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase()
		.replace(/[^a-z0-9\s]/g, ' ');
	if (!text) return false;
	for (const keyword of ENTERTAINMENT_KEYWORDS) {
		if (text.includes(keyword)) return true;
	}
	return false;
}

function isMarketNoise(item: ImpactResult): boolean {
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase()
		.trim();
	if (!text) return false;
	return MARKET_NOISE_PATTERNS.some((pattern) => text.includes(pattern));
}
function isSupportedLanguage(item: ImpactResult): boolean {
	const lang = (item.language || '').toLowerCase();
	if (!lang) {
		return /[a-z]/i.test(`${item.title || ''} ${item.description || ''}`);
	}
	return SUPPORTED_LANGS.has(lang);
}
