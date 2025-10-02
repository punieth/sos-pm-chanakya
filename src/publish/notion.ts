import type { ScoredItem } from '../types';

export function mapHotLaunchProperties(item: ScoredItem): Record<string, unknown> {
  const decisionWindow = item.impact.impact >= 0.8 ? '0–7d' : '7–30d';
  const props: Record<string, unknown> = {
    'Role Tag': 'Hot Launch',
    'Quick Action': 'Review macro-shift news',
    'Decision Window': decisionWindow,
    'Signal Score': Math.round(item.impact.impact * 100) / 10,
    Status: 'Keep',
    Why: buildHotLaunchWhy(item),
  };
  if (item.duplicateUrls && item.duplicateUrls.length > 0) {
    props['Citations'] = item.duplicateUrls.join('\n');
  }
  return props;
}

export function buildHotLaunchWhy(item: ScoredItem): string {
  const scorePct = Math.round(item.impact.impact * 100);
  const novelty = item.impact.components.graphNovelty >= 0.8 ? 'Novel signal detected. ' : '';
  const eventLabel = item.eventClass && item.eventClass !== 'OTHER' ? item.eventClass.replace(/_/g, ' ') : 'market shift';
  const domain = item.domain || 'source';
  return `${novelty}${eventLabel} surfaced with estimated impact ${scorePct}%. Domain: ${domain}.`;
}
