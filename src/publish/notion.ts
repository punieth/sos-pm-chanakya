import type { ImpactResult } from '../types';

export function mapHotLaunchProperties(item: ImpactResult): Record<string, unknown> {
	const decisionWindow = item.impactScore > 0.8 ? '0–7d' : '7–30d';
	const props: Record<string, unknown> = {
		'Role Tag': 'Hot Launch',
		'Quick Action': 'Review macro-shift news',
		'Decision Window': decisionWindow,
		'Signal Score': Math.round(item.impactScore * 100) / 10,
		Status: 'Keep',
		Why: buildHotLaunchWhy(item),
	};
	if (item.duplicateUrls && item.duplicateUrls.length > 0) {
		props['Citations'] = item.duplicateUrls.join('\n');
	}
	return props;
}

export function buildHotLaunchWhy(item: ImpactResult): string {
	const scorePct = Math.round(item.impactScore * 100);
	const novelty = item.graphNovelty && item.graphNovelty >= 1 ? 'Novel signal detected. ' : '';
	const eventLabel = item.eventClass && item.eventClass !== 'OTHER' ? item.eventClass.replace(/_/g, ' ') : 'market shift';
	const domain = item.domain || 'source';
	return `${novelty}${eventLabel} surfaced with estimated impact ${scorePct}%. Domain: ${domain}.`;
}
