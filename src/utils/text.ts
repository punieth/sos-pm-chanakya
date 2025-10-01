export function sanitize(text?: string | null): string {
	if (!text) return '';
	return text.replace(/\s+/g, ' ').trim();
}

export function decodeHtmlEntities(input: string): string {
	return input
		.replace(/&amp;/g, '&')
		.replace(/&lt;/g, '<')
		.replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"')
		.replace(/&#39;/g, "'");
}

export function tokenize(text: string): string[] {
	return sanitize(text)
		.toLowerCase()
		.split(/[^a-z0-9+]+/)
		.filter(Boolean);
}
