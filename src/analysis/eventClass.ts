import nlp from 'compromise';
import { EventClassName, NormalizedItem, ClassifiedItem } from '../types';
import { sanitize, tokenize } from '../utils/text';

const ARCHETYPES: EventClassName[] = ['LAUNCH', 'PARTNERSHIP', 'POLICY', 'COMMERCE', 'TREND', 'OTHER'];

const KEYWORDS: Record<EventClassName, string[]> = {
	LAUNCH: ['launch', 'unveil', 'introduce', 'ship', 'debut', 'general availability', 'ga', 'go live', 'beta'],
	PARTNERSHIP: ['partner', 'integration', 'joins forces', 'collaborate', 'alignment', 'ecosystem partner', 'merge'],
	POLICY: ['policy', 'regulation', 'compliance', 'license', 'mandate', 'guideline', 'circular', 'order', 'notice'],
	COMMERCE: ['checkout', 'payments', 'wallet', 'merchant', 'commerce', 'billing', 'upi', 'pos'],
	TREND: [],
	OTHER: [],
};

const SIGNAL_BOOSTS: Partial<Record<EventClassName, string[]>> = {
	LAUNCH: ['upgrade', 'build', 'feature'],
	PARTNERSHIP: ['alliance', 'integration', 'sdk'],
	POLICY: ['ban', 'stipulation', 'compliance'],
	COMMERCE: ['settlement', 'payout', 'transaction'],
};

export interface ClassificationResult {
	class: EventClassName;
	confidence: number;
	lexiconScore: number;
}

export function classifyEvent(item: NormalizedItem): ClassificationResult {
	const corpus = buildCorpus(item);
	const verbs = gatherVerbs(item, corpus);
	const tokens = tokenize(corpus);

	let bestClass: EventClassName = 'TREND';
	let bestScore = 0;
	let bestLex = 0;

	for (const archetype of ARCHETYPES) {
		const lexScore = lexicalScore(verbs, tokens, corpus, archetype);
		if (lexScore > bestScore) {
			bestClass = archetype;
			bestScore = lexScore;
			bestLex = lexScore;
		}
	}

	if (isMarketWrap(item)) {
		bestClass = 'TREND';
		bestScore = Math.min(bestScore, 0.18);
	}

	if (bestClass === 'TREND' && bestScore < 0.15) {
		bestScore = 0.12;
	}

	return { class: bestClass, confidence: bestScore, lexiconScore: bestLex };
}

export function applyClassification(item: NormalizedItem): ClassifiedItem {
	const result = classifyEvent(item);
	return {
		...item,
		eventClass: result.class,
		classConfidence: result.confidence,
		classSignals: { lexicon: result.lexiconScore, embedding: 0 },
	};
}

function buildCorpus(item: NormalizedItem): string {
	return sanitize(`${item.title || ''} ${item.description || ''}`);
}

function gatherVerbs(item: NormalizedItem, corpus: string): string[] {
	const fromExtraction = Array.isArray(item.verbs) ? item.verbs : [];
	const fallback = fallbackVerbsFromText(corpus);
	return dedupeStrings([...fromExtraction, ...fallback].map(normalizeVerb));
}

function fallbackVerbsFromText(text: string): string[] {
	const tokens = tokenize(text);
	const verbs: string[] = [];
	for (const token of tokens) {
		if (token.length < 4) continue;
		if (/(ing|ed|es|s)$/.test(token)) {
			verbs.push(token);
			const stripped = stripSuffix(token);
			if (stripped !== token) verbs.push(stripped);
			continue;
		}
		if (SEED_HINTS.has(token)) verbs.push(token);
	}
	return verbs;
}

const SEED_HINTS = new Set(
	Object.values(KEYWORDS)
		.flat()
		.map((phrase) => phrase.split(' '))
		.flat()
		.map(toToken)
		.filter(Boolean)
);

function stripSuffix(token: string): string {
	if (token.endsWith('ies') && token.length > 4) return `${token.slice(0, -3)}y`;
	if (token.endsWith('ing') && token.length > 4) return token.slice(0, -3);
	if (token.endsWith('ed') && token.length > 4) return token.slice(0, -2);
	if (token.endsWith('es') && token.length > 4) return token.slice(0, -2);
	if (token.endsWith('s') && token.length > 3) return token.slice(0, -1);
	return token;
}

function normalizeVerb(verb: string): string {
	const text = sanitize(verb).toLowerCase();
	if (!text) return '';
	const doc = nlp(text);
	const normalized = doc.verbs().toInfinitive().out('text');
	return toToken(normalized || text);
}

function toToken(text: string): string {
	return sanitize(text)
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, ' ')
		.trim()
		.replace(/\s+/g, ' ');
}

function lexicalScore(verbs: string[], tokens: string[], corpus: string, archetype: EventClassName): number {
	if (archetype === 'TREND') return 0.08;
	const keywords = (KEYWORDS[archetype] || []).map((term) => term.toLowerCase());
	const boosters = (SIGNAL_BOOSTS[archetype] || []).map((term) => term.toLowerCase());
	const text = corpus.toLowerCase();
	const tokenSet = new Set(tokens);
	const normalizedVerbs = verbs.map(toToken).filter(Boolean);

	let keywordHits = 0;
	for (const keyword of keywords) {
		if (!keyword) continue;
		if (text.includes(keyword)) keywordHits += 1.2;
		if (tokenSet.has(keyword)) keywordHits += 1;
		if (normalizedVerbs.includes(keyword)) keywordHits += 1.5;
	}

	let boosterHits = 0;
	for (const boost of boosters) {
		if (!boost) continue;
		if (text.includes(boost)) boosterHits += 0.6;
		if (tokenSet.has(boost)) boosterHits += 0.5;
	}

	if (archetype === 'LAUNCH' && normalizedVerbs.some((verb) => verb.startsWith('launch'))) {
		keywordHits += 0.8;
}
	if (archetype === 'PARTNERSHIP' && text.includes('joins forces')) {
		keywordHits += 1.2;
	}

	const raw = keywordHits * 0.18 + boosterHits * 0.12;
	const base = keywordHits > 0 ? 0.25 : 0;
	return Math.min(1, raw + base);
}

function dedupeStrings(values: string[]): string[] {
	const seen = new Set<string>();
	const output: string[] = [];
	for (const value of values) {
		const token = toToken(value);
		if (!token || seen.has(token)) continue;
		seen.add(token);
		output.push(token);
	}
	return output;
}

function isMarketWrap(item: NormalizedItem): boolean {
	const text = sanitize(`${item.title || ''} ${item.description || ''}`)
		.toLowerCase()
		.trim();
	if (!text) return false;
	return MARKET_NOISE_PATTERNS.some((pattern) => text.includes(pattern));
}

const MARKET_NOISE_PATTERNS = [
	'ahead of market',
	'pre-market',
	'stocks to watch',
	'market wrap',
	'closing bell',
	'opening bell',
	'market live updates',
	'trades to watch',
];