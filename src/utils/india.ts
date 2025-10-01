import { sanitize, tokenize } from './text';

const INDIA_KEYWORDS = new Set([
	'india',
	'indian',
	'indians',
	'bharat',
	'bengaluru',
	'bangalore',
	'mumbai',
	'delhi',
	'hyderabad',
	'kolkata',
	'chennai',
	'noida',
	'pune',
	'npci',
	'upi',
	'uidai',
	'meity',
	'sebi',
	'rbi',
	'trai',
	'ongc',
	'jio',
	'paytm',
	'razorpay',
	'phonepe',
	'lic',
	'gst',
	'cbic',
	'cbdt',
	'odia',
	'karnataka',
	'maharashtra',
	'rajasthan',
	'uttar',
	'gujarat',
	'bihar',
	'kerala',
	'telangana',
]);

const PRIORITY_DOMAINS = ['.in', 'npci.org', 'rbi.org', 'trai.gov', 'uidai.gov', 'meity.gov'];

const PRIORITY_SOURCES = ['npci', 'rbi', 'trai', 'uidai', 'meity', 'jio', 'paytm', 'razorpay', 'phonepe'];

const REGULATOR_DOMAINS = [
	'rbi.org',
	'sebi.gov',
	'trai.gov',
	'meity.gov',
	'uidai.gov',
	'npci.org',
	'gstcouncil.gov',
];

const REGULATOR_SOURCES = [
	'reserve bank of india',
	'rbi',
	'securities and exchange board of india',
	'sebi',
	'telecom regulatory authority of india',
	'trai',
	'ministry of electronics and information technology',
	'meity',
	'national payments corporation of india',
	'npci',
	'unique identification authority of india',
	'uidai',
	'gst council',
	'finance ministry',
	'ministry of finance',
];

const REGULATOR_TOKEN_BONUS: Record<string, number> = {
	'rbi': 0.5,
	'reserve bank': 0.5,
	'sebi': 0.45,
	'trai': 0.4,
	'meity': 0.35,
	'npci': 0.33,
	'uidai': 0.33,
	'gst council': 0.35,
	'finance ministry': 0.32,
	'ministry of finance': 0.32,
	'monetary policy committee': 0.3,
};

export interface IndiaRelevanceInput {
	title?: string;
	description?: string;
	source?: string;
	domain?: string;
}

export function indiaRelevanceScore({ title, description, source, domain }: IndiaRelevanceInput): number {
	let corpus = sanitize(`${title || ''} ${description || ''}`)
		.toLowerCase()
		.trim();
	const src = sanitize(source || '').toLowerCase();
	const dom = (domain || '').toLowerCase();

	let score = 0;

	if (PRIORITY_DOMAINS.some((needle) => dom.includes(needle))) score += 0.6;
	if (PRIORITY_SOURCES.some((needle) => src.includes(needle))) score += 0.2;

	if (!corpus) corpus = src || dom;

	const tokens = tokenize(corpus);
	const tokenSet = new Set(tokens);
	let hits = 0;
	for (const token of tokens) {
		if (INDIA_KEYWORDS.has(token)) hits++;
	}
	if (hits > 0) score += Math.min(0.3, hits * 0.12);

	const regulatorScore = regulatorSignalScore({ title, description, source, domain }, corpus, tokenSet);
	if (regulatorScore > 0) score += regulatorScore;

	if (corpus.includes('asia pacific') || corpus.includes('south asia')) score += 0.1;

	return clamp(score, 0, 1);
}

export function regulatorSignalScore(
	input: IndiaRelevanceInput,
	precomputedCorpus?: string,
	precomputedTokens?: Set<string>
): number {
	const corpus = (precomputedCorpus || sanitize(`${input.title || ''} ${input.description || ''}`))
		.toLowerCase()
		.trim();
	const tokens = precomputedTokens || new Set(tokenize(corpus));
	const dom = (input.domain || '').toLowerCase();
	const src = sanitize(input.source || '').toLowerCase();

	let score = 0;

	if (REGULATOR_DOMAINS.some((needle) => dom.includes(needle))) score = Math.max(score, 0.75);

	if (REGULATOR_SOURCES.some((needle) => src.includes(needle))) score = Math.max(score, 0.6);

	if (!corpus && !tokens.size) return clamp(score, 0, 1);

	const text = corpus || src || dom;
	for (const [needle, bonus] of Object.entries(REGULATOR_TOKEN_BONUS)) {
		if (!needle) continue;
		if (needle.includes(' ')) {
			if (text.includes(needle)) score = Math.max(score, bonus);
			continue;
		}
		if (tokens.has(needle)) score = Math.max(score, bonus);
	}

	return clamp(score, 0, 1);
}

export function isIndiaRelevant(input: IndiaRelevanceInput): boolean {
	return indiaRelevanceScore(input) >= 0.45;
}

function clamp(value: number, min: number, max: number): number {
	if (Number.isNaN(value)) return min;
	return Math.max(min, Math.min(max, value));
}
