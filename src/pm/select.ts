import { ImpactResult } from '../types';
import { sanitize, tokenize } from '../utils/text';
import { indiaRelevanceScore, isIndiaRelevant } from '../utils/india';

const MIN_PM_IMPACT = 0.6;
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
};

const ALLOWED_EVENT_CLASSES = new Set<ImpactResult['eventClass']>(['LAUNCH', 'PARTNERSHIP', 'POLICY', 'COMMERCE']);

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

export interface RankedCandidate {
	item: ImpactResult;
	pmScore: number;
	indiaScore: number;
}

export interface ShortlistedCandidate {
	item: ImpactResult;
	pmScore: number;
	indiaScore: number;
	signals: string[];
	urgency: '⚡' | '🛑' | '🧩';
}

export function selectPmStories(items: ImpactResult[], limit: number): ShortlistedCandidate[] {
	return dedupeByUrl(dedupeByTopic(dedupeByCluster(rankForPm(items))))
		.filter(({ item }) => ALLOWED_EVENT_CLASSES.has(item.eventClass))
		.filter(({ item }) => !isMarketNoise(item))
		.filter(({ item }) => !isEntertainment(item))
		.filter(({ item, pmScore, indiaScore }) => {
			const indiaCut = indiaScore >= 0.45;
			if (!indiaCut) return false;
			return pmScore >= MIN_PM_IMPACT;
		})
		.slice(0, Math.max(1, limit))
		.map(({ item, pmScore, indiaScore }) => ({
		item,
		pmScore,
		indiaScore,
		signals: deriveSignals(item),
		urgency: deriveUrgency(pmScore, item),
		}));
}

export function rankForPm(items: ImpactResult[]): RankedCandidate[] {
	return items
		.map((item) => {
			const indiaScore = indiaRelevanceScore({
				title: item.title,
				description: item.description,
				source: item.source,
				domain: item.domain,
			});
			return {
				item,
				pmScore: computePmScore(item, indiaScore),
				indiaScore,
			};
		})
		.sort((a, b) => b.pmScore - a.pmScore);
}

function computePmScore(item: ImpactResult, indiaScore: number): number {
	let score = item.impactScore;
	score += indiaScore * 0.25;
	if (indiaScore < 0.2) score -= 0.18;
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

function deriveUrgency(pmScore: number, item: ImpactResult): '⚡' | '🛑' | '🧩' {
	if (item.eventClass === 'POLICY' && pmScore >= 0.6) return '🛑';
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase();
	if (item.eventClass === 'LAUNCH' && isIpoStory(text)) return '⚡';
	if (pmScore >= 0.72) return '⚡';
	return '🧩';
}

function isIpoStory(text: string): boolean {
	if (!text) return false;
	return IPO_KEYWORDS.some((keyword) => text.includes(keyword));
}

function clamp(value: number, min: number, max: number): number {
	if (Number.isNaN(value)) return min;
	return Math.max(min, Math.min(max, value));
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
		const clusterKey = entry.item.clusterId || entry.item.id;
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
