const TRUSTED = new Set([
	'reuters.com',
	'bloomberg.com',
	'financialtimes.com',
	'techcrunch.com',
	'thewire.in',
	'inc42.com',
	'medianama.com',
	'moneycontrol.com',
	'economictimes.indiatimes.com',
	'the-ken.com',
	'thewirebusiness.com',
	'fortune.com',
]);

export function isTrustedDomain(domain: string): boolean {
	return TRUSTED.has(domain);
}

export function trustedDomains(): string[] {
	return Array.from(TRUSTED);
}
