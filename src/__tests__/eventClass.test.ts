import { describe, expect, it } from 'vitest';
import { classifyEvent } from '../analysis/eventClass';
import type { NormalizedItem } from '../types';

function makeItem(title: string, description = ''): NormalizedItem {
  return {
    id: title.replace(/\s+/g, '-').toLowerCase(),
    title,
    description,
    url: `https://example.com/${Math.random().toString(36).slice(2)}`,
    canonicalUrl: `https://example.com/${title.replace(/\s+/g, '-').toLowerCase()}`,
    publishedAt: new Date().toISOString(),
    source: 'Test Source',
    provider: 'newsapi',
    domain: 'example.com',
  };
}

describe('event classification', () => {
  it('detects partnerships via verb signals', () => {
    const item = makeItem('ToolSuite signs deal to integrate CommerceFlow SDK', 'Vendors partner on embedded analytics program.');
    const result = classifyEvent(item);
    expect(result.className).toBe('PARTNERSHIP_INTEGRATION');
    expect(result.confidence).toBeGreaterThan(0.4);
  });

  it('detects payment and commerce shifts', () => {
    const item = makeItem('CheckoutFlow adds instant merchant settlement rail', 'Wallet checkout volume surge as merchants adopt the new payment rail.');
    const result = classifyEvent(item);
    expect(result.className).toBe('PAYMENT_COMMERCE');
    expect(result.confidence).toBeGreaterThan(0.35);
  });

  it('detects pricing policy updates', () => {
    const item = makeItem('PlatformHub revises commission fee policy for marketplace sellers');
    const result = classifyEvent(item);
    expect(result.className).toBe('PRICING_POLICY');
    expect(result.confidence).toBeGreaterThan(0.3);
  });

  it('detects launches', () => {
    const item = makeItem('VendorX launches AI roadmap copilot for product teams', 'The beta rollout ships this week with partner access.');
    const result = classifyEvent(item);
    expect(result.className).toBe('MODEL_LAUNCH');
    expect(result.confidence).toBeGreaterThan(0.3);
  });
});
