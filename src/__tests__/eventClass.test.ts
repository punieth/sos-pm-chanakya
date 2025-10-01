import { describe, expect, it } from 'vitest';
import { classifyEvent } from '../analysis/eventClass';
import type { NormalizedItem } from '../types';

function makeItem(text: string): NormalizedItem {
	return {
		id: text,
		title: text,
		description: text,
		url: `https://example.com/${Math.random().toString(36).slice(2)}`,
		canonicalUrl: `https://example.com/${text.replace(/\s+/g, '-').toLowerCase()}`,
		publishedAt: new Date().toISOString(),
		source: 'Test Source',
		provider: 'newsapi',
		domain: 'example.com',
	};
}

describe('event classification', () => {
	it('detects partnerships via lexicon', () => {
		const item = makeItem('ToolSuite joins forces with CommerceFlow to integrate analytics');
		const result = classifyEvent(item);
		expect(result.class).toBe('PARTNERSHIP');
		expect(result.confidence).toBeGreaterThan(0.4);
	});

	it('detects payments scenarios', () => {
		const item = makeItem('CheckoutFlow adds instant wallet checkout across marketplaces');
		const result = classifyEvent(item);
		expect(result.class).toBe('PAYMENTS');
		expect(result.confidence).toBeGreaterThan(0.4);
	});

	it('detects platform policy changes', () => {
		const item = makeItem('PlatformHub revises commission policy for creator tools');
		const result = classifyEvent(item);
		expect(result.class).toBe('PLATFORM_POLICY');
		expect(result.confidence).toBeGreaterThan(0.3);
	});

	it('detects model launches', () => {
		const item = makeItem('ModelCore launches general availability of its new inference service');
		const result = classifyEvent(item);
		expect(result.class).toBe('MODEL_LAUNCH');
		expect(result.confidence).toBeGreaterThan(0.4);
	});
});
