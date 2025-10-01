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
	it('detects partnerships via verb signals', () => {
		const item = makeItem('ToolSuite joins forces with CommerceFlow to integrate analytics');
		const result = classifyEvent(item);
		expect(result.class).toBe('PARTNERSHIP');
		expect(result.confidence).toBeGreaterThan(0.25);
	});

	it('detects commerce shifts', () => {
		const item = makeItem('CheckoutFlow launches instant wallet checkout across marketplaces');
		const result = classifyEvent(item);
		expect(result.class).toBe('COMMERCE');
		expect(result.confidence).toBeGreaterThan(0.25);
	});

	it('detects policy updates', () => {
		const item = makeItem('PlatformHub revises commission policy for creator tools');
		const result = classifyEvent(item);
		expect(result.class).toBe('POLICY');
		expect(result.confidence).toBeGreaterThan(0.2);
	});

	it('detects launches', () => {
		const item = makeItem('VendorX launches AI roadmap copilot for Indian product teams');
		const result = classifyEvent(item);
		expect(result.class).toBe('LAUNCH');
		expect(result.confidence).toBeGreaterThan(0.2);
	});
});
