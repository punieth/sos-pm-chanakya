export async function sha256Hex(input: string): Promise<string> {
	const enc = new TextEncoder();
	const data = enc.encode(input);
	const digest = await crypto.subtle.digest('SHA-256', data);
	return Array.from(new Uint8Array(digest))
		.map((b) => b.toString(16).padStart(2, '0'))
		.join('');
}

export function normalizeExcerptForHash(text: string): string {
	return text.replace(/\s+/g, ' ').trim().toLowerCase();
}

export function canonicalUrlForHash(rawUrl: string): string {
	try {
		const url = new URL(rawUrl);
		url.hash = '';
		return url.toString();
	} catch {
		return rawUrl.trim();
	}
}
