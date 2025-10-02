export type SourceProvider = 'newsapi' | 'gdelt' | 'google-rss' | 'registry';

export interface SourceItem {
	title: string;
	url: string;
	publishedAt: string;
	source: string;
	description?: string;
	language?: string;
	provider: SourceProvider;
	canonicalUrl?: string;
	authors?: string[];
	country?: string;
}

export interface NormalizedItem extends SourceItem {
	id: string;
	domain: string;
	embedding?: number[];
	verbs?: string[];
	entities?: EntityExtraction;
}

export type EventClassName = 'LAUNCH' | 'PARTNERSHIP' | 'POLICY' | 'COMMERCE' | 'TREND' | 'OTHER';

export interface ClassifiedItem extends NormalizedItem {
	eventClass: EventClassName;
	classConfidence: number;
	classSignals: {
		lexicon: number;
		embedding: number;
	};
	clusterId?: string;
}

export interface ImpactInput extends ClassifiedItem {
	clusterId?: string;
	duplicateUrls?: string[];
	trustedDomainCount?: number;
	graphNovelty?: number;
}

export interface ImpactResult extends ImpactInput {
	impactScore: number;
	impactBreakdown: {
		recency: number;
		graphNovelty: number;
		surfaceReach: number;
		commerceTie: number;
		indiaTie: number;
		momentum: number;
		authority: number;
	};
}

export interface EntityExtraction {
	orgs: string[];
	products: string[];
	verbs: string[];
}

export interface EntityGraphSnapshot {
	nodes: Record<string, EntityNode>;
	edges: Record<string, EntityEdge>;
}

export interface EntityNode {
	id: string;
	label: string;
	degree: number;
	lastSeen: number;
	type: 'ORG' | 'PRODUCT';
}

export interface EntityEdge {
	id: string;
	source: string;
	target: string;
	verbBucket: string;
	count: number;
	lastSeen: number;
}

export type PipelineItem = ImpactResult;

export interface FeatureFlagConfig {
	FEAT_GEMINI?: string;
	FEAT_LOCAL?: string;
	FEAT_COMPOSE_LLM?: string;
	GDELT_ENABLED?: string;
	BING_NEWS_ENABLED?: string;
}

export interface ScoreTelemetry {
	model_used: string;
	retries: number;
	status_code: number;
	provider?: string;
}
