export function canonicalizeUrl(input: string): string {
	try {
		const url = new URL(input);
		url.hash = '';
		if (url.protocol === 'http:') {
			url.protocol = 'https:';
		}
		if (url.pathname && url.pathname !== '/') {
			url.pathname = url.pathname.replace(/\/+/, '/');
			if (url.pathname.endsWith('/')) {
				url.pathname = url.pathname.slice(0, -1);
			}
		}
		return url.toString();
	} catch {
		return input.trim();
	}
}

export function getDomain(url: string): string {
	try {
		const { hostname } = new URL(url);
		return hostname.replace(/^www\./, '').toLowerCase();
	} catch {
		return 'unknown';
	}
}

export function hashId(...parts: string[]): string {
	const input = parts.join('||');
	let h1 = 0xdeadbeef ^ input.length;
	let h2 = 0x41c6ce57 ^ input.length;
	for (let i = 0, ch; i < input.length; i++) {
		ch = input.charCodeAt(i);
		h1 = Math.imul(h1 ^ ch, 2654435761);
		h2 = Math.imul(h2 ^ ch, 1597334677);
	}
	h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
	h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
	const x = 4294967296 * (2097151 & h2) + (h1 >>> 0);
	return x.toString(36);
}
