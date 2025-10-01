export const HOUR_MS = 60 * 60 * 1000;
export const DAY_MS = 24 * HOUR_MS;

export function nowUtc(): Date {
	return new Date();
}

export function subtractHours(date: Date, hours: number): Date {
	return new Date(date.getTime() - hours * HOUR_MS);
}

export function toIsoString(date: Date): string {
	return date.toISOString();
}

export function parseUtc(dateStr: string | undefined): Date | null {
	if (!dateStr) return null;
	const d = new Date(dateStr);
	return Number.isNaN(d.getTime()) ? null : d;
}

export function isWithinWindow(dateStr: string | undefined, windowMs: number, reference: Date = nowUtc()): boolean {
	const parsed = parseUtc(dateStr);
	if (!parsed) return false;
	return reference.getTime() - parsed.getTime() <= windowMs;
}
