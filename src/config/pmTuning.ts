export type TopicKey = 'regulation' | 'product' | 'ai' | 'other';

export interface TopicWeightsConfig {
	regulation?: number;
	product?: number;
	ai?: number;
	other?: number;
}

export interface PmTuning {
	topicWeights: Record<TopicKey, number>;
	maxShortlist: number;
	rawWeights: Record<TopicKey, number>;
}

const DEFAULT_TOPIC_WEIGHTS: Record<TopicKey, number> = {
	regulation: 0,
	product: 0,
	ai: 90,
	other: 10,
};

export const DEFAULT_PM_TUNING: PmTuning = {
	topicWeights: DEFAULT_TOPIC_WEIGHTS,
	maxShortlist: 10,
	rawWeights: DEFAULT_TOPIC_WEIGHTS,
};

export function resolvePmTuning(overrides?: Partial<PmTuning>): PmTuning {
	if (!overrides) {
		return {
			...DEFAULT_PM_TUNING,
		};
	}
	const rawWeights = normalizeRawWeights(overrides.topicWeights || DEFAULT_TOPIC_WEIGHTS);
	const topicWeights = normalizeTopicWeights(rawWeights);
	const maxShortlist = Math.max(3, Math.min(15, overrides.maxShortlist ?? DEFAULT_PM_TUNING.maxShortlist));
	return {
		rawWeights,
		topicWeights,
		maxShortlist,
	};
}

export function parsePmTuning(raw?: string | null): Partial<PmTuning> | undefined {
	if (!raw) return undefined;
	try {
		const parsed = JSON.parse(raw);
		if (parsed && typeof parsed === 'object') {
			if ('topicWeights' in parsed || 'maxShortlist' in parsed) {
				return parsed as Partial<PmTuning>;
			}
			const direct: TopicWeightsConfig = {};
			for (const key of ['regulation', 'product', 'ai', 'other'] as TopicKey[]) {
				if (typeof parsed[key] === 'number') {
					direct[key] = parsed[key];
				}
			}
			const hasDirect = Object.keys(direct).length > 0;
			const result: Partial<PmTuning> = {};
			if (hasDirect) {
				const normalized = normalizeRawWeights(direct);
				result.topicWeights = normalized;
			}
			if (typeof parsed.maxShortlist === 'number') {
				result.maxShortlist = parsed.maxShortlist;
			}
			if (Object.keys(result).length > 0) return result;
		}
	} catch (err) {
		console.log('PM_TUNING_PARSE_ERROR', String(err));
	}
	return undefined;
}

function normalizeRawWeights(input: TopicWeightsConfig): Record<TopicKey, number> {
	return {
		regulation: Math.max(input.regulation ?? DEFAULT_TOPIC_WEIGHTS.regulation, 0),
		product: Math.max(input.product ?? DEFAULT_TOPIC_WEIGHTS.product, 0),
	ai: Math.max(input.ai ?? DEFAULT_TOPIC_WEIGHTS.ai, 0),
		other: Math.max(input.other ?? DEFAULT_TOPIC_WEIGHTS.other, 0),
	};
}

function normalizeTopicWeights(raw: Record<TopicKey, number>): Record<TopicKey, number> {
	const total = Object.values(raw).reduce((sum, value) => sum + value, 0);
	if (total <= 0) return { ...DEFAULT_TOPIC_WEIGHTS };
	return {
		regulation: (raw.regulation / total) * 100,
		product: (raw.product / total) * 100,
		ai: (raw.ai / total) * 100,
		other: (raw.other / total) * 100,
	};
}
