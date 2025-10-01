import nlp from 'compromise';
import vectors from './vectors.json';
import { EventClassName, NormalizedItem, ClassifiedItem } from '../types';
import { sanitize, tokenize } from '../utils/text';

const LEXICONS: Record<EventClassName, string[]> = {
	PARTNERSHIP: ['partners', 'integrates', 'adds support', 'powered by', 'joins forces'],
	PAYMENTS: ['checkout', 'pay with', 'embedded payments', 'in-chat purchase', 'wallet'],
	PLATFORM_POLICY: ['policy', 'pricing', 'fees', 'commission', 'guideline', 'terms'],
	MODEL_LAUNCH: ['launches', 'releases', 'announces', 'rolls out', 'general availability'],
	OTHER: [],
};

const CLASSES: EventClassName[] = ['PARTNERSHIP', 'PAYMENTS', 'PLATFORM_POLICY', 'MODEL_LAUNCH', 'OTHER'];

type PrototypeVectors = Record<Exclude<EventClassName, 'OTHER'>, Record<string, number>>;

const PROTOTYPES = vectors as PrototypeVectors;

export interface ClassificationResult {
	class: EventClassName;
	confidence: number;
	lexiconScore: number;
	embeddingScore: number;
}

export function classifyEvent(item: NormalizedItem): ClassificationResult {
	const text = buildCorpus(item);
	const normalized = normalizeVerbs(text);
	const tokens = tokenize(normalized);
	const vector = buildTextVector(tokens);

	let bestClass: EventClassName = 'OTHER';
	let bestScore = 0;
	let bestLex = 0;
	let bestEmbed = 0;

	for (const cls of CLASSES) {
		if (cls === 'OTHER') continue;
		const lexScore = lexicalScore(normalized, LEXICONS[cls]);
		const embedScore = cosineSimilarity(vector, PROTOTYPES[cls] || {});
		const combined = combineScores(lexScore, embedScore);
		if (combined > bestScore) {
			bestClass = cls;
			bestScore = combined;
			bestLex = lexScore;
			bestEmbed = embedScore;
		}
	}

	if (bestScore < 0.25) {
		return { class: 'OTHER', confidence: bestScore, lexiconScore: bestLex, embeddingScore: bestEmbed };
	}

	return { class: bestClass, confidence: bestScore, lexiconScore: bestLex, embeddingScore: bestEmbed };
}

export function applyClassification(item: NormalizedItem): ClassifiedItem {
	const result = classifyEvent(item);
	return {
		...item,
		eventClass: result.class,
		classConfidence: result.confidence,
		classSignals: { lexicon: result.lexiconScore, embedding: result.embeddingScore },
	};
}

function buildCorpus(item: NormalizedItem): string {
	return sanitize(`${item.title || ''}. ${item.description || ''}`);
}

function normalizeVerbs(text: string): string {
	if (!text) return '';
	const doc = nlp(text);
	return doc.verbs().toInfinitive().out('text') || text;
}

function lexicalScore(text: string, lexicon: string[]): number {
	if (!lexicon || lexicon.length === 0) return 0;
	const lowered = text.toLowerCase();
	let hits = 0;
	for (const phrase of lexicon) {
		if (lowered.includes(phrase)) hits++;
	}
	return Math.min(1, hits / lexicon.length + (hits > 0 ? 0.2 : 0));
}

function buildTextVector(tokens: string[]): Record<string, number> {
	const vector: Record<string, number> = {};
	for (const token of tokens) {
		if (!token) continue;
		vector[token] = (vector[token] || 0) + 1;
	}
	const magnitude = Math.sqrt(Object.values(vector).reduce((sum, val) => sum + val * val, 0)) || 1;
	for (const key of Object.keys(vector)) {
		vector[key] = vector[key] / magnitude;
	}
	return vector;
}

function cosineSimilarity(textVector: Record<string, number>, prototype: Record<string, number>): number {
	let dot = 0;
	let prototypeMagnitudeSq = 0;
	for (const [term, weight] of Object.entries(prototype)) {
		const textWeight = textVector[term] || 0;
		dot += textWeight * weight;
		prototypeMagnitudeSq += weight * weight;
	}
	if (prototypeMagnitudeSq === 0) return 0;
	const prototypeMagnitude = Math.sqrt(prototypeMagnitudeSq);
	return Math.max(0, Math.min(1, dot / prototypeMagnitude));
}

function combineScores(lexScore: number, embedScore: number): number {
	const weighted = lexScore * 0.6 + embedScore * 0.4;
	return Math.max(0, Math.min(1, weighted));
}
