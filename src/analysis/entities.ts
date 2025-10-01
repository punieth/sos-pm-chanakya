import nlp from 'compromise';
import { NormalizedItem, EntityExtraction, EntityEdge, EntityGraphSnapshot, EntityNode } from '../types';
import { hashId } from '../utils/url';
import { DAY_MS } from '../utils/time';
import { sanitize, tokenize } from '../utils/text';

type Logger = (phase: string, details: Record<string, unknown>) => void;

const EDGE_TTL_SECONDS = 60 * 60 * 24 * 90; // 90 days
const NOVELTY_WINDOW_MS = 30 * DAY_MS;
const VERB_BUCKET_LIMIT = 5;
const DEFAULT_LOGGER: Logger = () => {};

export interface GraphUpdateOptions {
	kv: KVNamespace;
	logger?: Logger;
}

export interface GraphUpdateResult extends EntityGraphSnapshot {
	itemNovelty: Record<string, boolean>;
}

export async function updateEntityGraph(
	items: NormalizedItem[],
	{ kv, logger = DEFAULT_LOGGER }: GraphUpdateOptions
): Promise<GraphUpdateResult> {
	const snapshot: EntityGraphSnapshot = { nodes: {}, edges: {} };
	const novelty: Record<string, boolean> = {};

	for (const item of items) {
		const extraction = extractEntities(`${item.title}. ${item.description || ''}`);
		item.entities = extraction;
		item.verbs = extraction.verbs;

		const nodes = buildNodeMap(extraction);
		const nodeEntries = Array.from(nodes.values());
		if (nodeEntries.length === 0) {
			novelty[item.id] = false;
			continue;
		}

		const now = Date.now();
		const nodeIds = nodeEntries.map((entry) => entry.id);

		await Promise.all(
			nodeEntries.map(async (entry) => {
				const key = nodeKey(entry.id);
				const existing = await kv.get<EntityNode>(key, 'json');
				const degreeBoost = nodeIds.length - 1;
				const updated: EntityNode = {
					id: entry.id,
					label: entry.label,
					type: entry.type,
					degree: (existing?.degree || 0) + Math.max(0, degreeBoost),
					lastSeen: now,
				};
				snapshot.nodes[entry.id] = updated;
				await kv.put(key, JSON.stringify(updated), { expirationTtl: EDGE_TTL_SECONDS });
			})
		);

		const verbBuckets = buildVerbBuckets(extraction.verbs);
		const pairs = buildPairs(nodeEntries);
		let itemHasNovelty = false;

		for (const pair of pairs) {
			for (const bucket of verbBuckets) {
				const edgeId = hashId(pair.source, pair.target, bucket);
				const key = edgeKey(pair.source, pair.target, bucket);
				const existing = await kv.get<EntityEdge>(key, 'json');
				const updated: EntityEdge = {
					id: edgeId,
					source: pair.source,
					target: pair.target,
					verbBucket: bucket,
					count: (existing?.count || 0) + 1,
					lastSeen: now,
				};
				snapshot.edges[edgeId] = updated;
				await kv.put(key, JSON.stringify(updated), { expirationTtl: EDGE_TTL_SECONDS });

				const isNovel = !existing || now - (existing.lastSeen || 0) > NOVELTY_WINDOW_MS;
				if (isNovel) itemHasNovelty = true;
			}
		}

		novelty[item.id] = itemHasNovelty;
		logger('entity_graph_update', {
			itemId: item.id,
			nodeCount: nodeEntries.length,
			pairs: pairs.length,
			verbBuckets: verbBuckets.length,
			novelty: itemHasNovelty,
		});
	}

	return { ...snapshot, itemNovelty: novelty };
}

export function extractEntities(text: string): EntityExtraction {
	const clean = sanitize(text);
	const doc = nlp(clean);
	const verbs = (doc.verbs().toInfinitive().out('array') as string[]).slice(0, VERB_BUCKET_LIMIT);

	const properNouns = dedupe(
		(doc
			.nouns()
			.if('#TitleCase')
			.out('array') as string[])
	);

	const orgs: string[] = [];
	const products: string[] = [];

	for (const noun of properNouns) {
		const normalized = noun.trim();
		if (!normalized) continue;
		if (looksLikeOrg(normalized)) orgs.push(normalized);
		else if (looksLikeProduct(normalized)) products.push(normalized);
		else if (normalized.length > 3) orgs.push(normalized);
	}

	const fallbackVerbs = verbs.length > 0 ? verbs : fallbackVerbBuckets(tokenize(clean));

	return {
		orgs: dedupe(orgs).slice(0, 6),
		products: dedupe(products).slice(0, 6),
		verbs: dedupe(fallbackVerbs).slice(0, VERB_BUCKET_LIMIT),
	};
}

function buildNodeMap(extraction: EntityExtraction) {
	const map = new Map<string, EntityNode>();
	for (const org of extraction.orgs) {
		const norm = normalizeLabel(org);
		const id = hashId('org', norm);
		map.set(id, { id, label: org, degree: 0, lastSeen: Date.now(), type: 'ORG' });
	}
	for (const product of extraction.products) {
		const norm = normalizeLabel(product);
		const id = hashId('product', norm);
		if (!map.has(id)) {
			map.set(id, { id, label: product, degree: 0, lastSeen: Date.now(), type: 'PRODUCT' });
		}
	}
	return map;
}

function buildPairs(nodes: EntityNode[]): Array<{ source: string; target: string }> {
	const pairs: Array<{ source: string; target: string }> = [];
	for (let i = 0; i < nodes.length; i++) {
		for (let j = i + 1; j < nodes.length; j++) {
			const a = nodes[i];
			const b = nodes[j];
			const source = a.id < b.id ? a.id : b.id;
			const target = a.id < b.id ? b.id : a.id;
			pairs.push({ source, target });
		}
	}
	return pairs;
}

function buildVerbBuckets(verbs: string[]): string[] {
	if (!verbs || verbs.length === 0) return ['observe'];
	return verbs
		.map((v) => v.toLowerCase().trim())
		.filter(Boolean)
		.slice(0, VERB_BUCKET_LIMIT);
}

function fallbackVerbBuckets(tokens: string[]): string[] {
	const verbs: string[] = [];
	for (const token of tokens) {
		if (/ing$|ed$/.test(token)) {
			verbs.push(token.replace(/ing$|ed$/, ''));
		}
	}
	return verbs.length > 0 ? verbs : ['announce'];
}

function looksLikeOrg(noun: string): boolean {
	const lowered = noun.toLowerCase();
	return /inc\.?$|corp\.?$|ltd\.?$|technologies|labs|systems|network|ventures|capital/.test(lowered);
}

function looksLikeProduct(noun: string): boolean {
	const lowered = noun.toLowerCase();
	return /(app|platform|suite|service|pay|checkout|wallet|ai|model|engine)$/.test(lowered);
}

function normalizeLabel(label: string): string {
	return label.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function dedupe<T>(values: T[]): T[] {
	return Array.from(new Set(values));
}

function nodeKey(id: string): string {
	return `graph:node:${id}`;
}

function edgeKey(source: string, target: string, bucket: string): string {
	return `graph:edge:${source}:${target}:${bucket}`;
}
