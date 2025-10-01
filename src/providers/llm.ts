import type { ScoreTelemetry } from '../types';

const DEFAULT_GEMINI_MODELS = ['gemini-2.0-flash-lite', 'gemini-1.5-flash'];
const MAX_RETRIES = 2;

type Logger = (phase: string, details: Record<string, unknown>) => void;

export interface LLMEnv {
	SEEN: KVNamespace;
	FEAT_GEMINI?: string;
	FEAT_LOCAL?: string;
	GEMINI_API_KEY?: string;
	GEMINI_MODEL_FORCE?: string;
	LLM_MAX_OUTPUT_TOKENS?: string;
	DAILY_LLM_LIMIT?: string;
}

export interface LLMResult {
	ok: boolean;
	text?: string;
	reason?: string;
	telemetry: ScoreTelemetry;
}

const DEFAULT_LOGGER: Logger = () => {};

export async function callLLM(env: LLMEnv, prompt: string, logger: Logger = DEFAULT_LOGGER): Promise<LLMResult> {
	const geminiEnabled = flagEnabled(env.FEAT_GEMINI, true);
	if (geminiEnabled) {
		const gemini = await callGemini(env, prompt, logger);
		if (gemini.ok) return gemini;
		logger('llm_failover', { provider: 'gemini', reason: gemini.reason });
		if (!flagEnabled(env.FEAT_LOCAL, true)) {
			return gemini;
		}
	}

	if (flagEnabled(env.FEAT_LOCAL, true)) {
		return localFallback(prompt);
	}

	return {
		ok: false,
		reason: 'No LLM providers enabled',
		telemetry: { model_used: 'disabled', retries: 0, status_code: 0, provider: 'none' },
	};
}

async function callGemini(env: LLMEnv, prompt: string, logger: Logger): Promise<LLMResult> {
	const key = env.GEMINI_API_KEY;
	if (!key) {
		return {
			ok: false,
			reason: 'Missing GEMINI_API_KEY',
			telemetry: { model_used: 'gemini', retries: 0, status_code: 0, provider: 'gemini' },
		};
	}

	const telemetry: ScoreTelemetry = { model_used: '', retries: 0, status_code: 0, provider: 'gemini' };
	const models = env.GEMINI_MODEL_FORCE ? [env.GEMINI_MODEL_FORCE] : DEFAULT_GEMINI_MODELS;

	const maxOutput = clamp(parseInt(env.LLM_MAX_OUTPUT_TOKENS || '512', 10), 64, 2048);
	const dailyCap = clamp(parseInt(env.DAILY_LLM_LIMIT || '200', 10), 0, 5000);
	const todayKey = `llm:${new Date().toISOString().slice(0, 10)}`;

	if (dailyCap > 0) {
		const used = parseInt((await env.SEEN.get(todayKey)) || '0', 10);
		if (used >= dailyCap) {
			return {
				ok: false,
				reason: 'Daily LLM limit reached',
				telemetry,
			};
		}
		await env.SEEN.put(todayKey, String(used + 1), { expirationTtl: 60 * 60 * 26 });
	}

	let attempts = 0;
	let lastReason = 'unknown_error';

	for (const model of models) {
		telemetry.model_used = model;
		for (let retry = 0; retry <= MAX_RETRIES; retry++) {
			attempts++;
			try {
				const res = await fetch(
					`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
					{
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						body: JSON.stringify({
							contents: [{ role: 'user', parts: [{ text: prompt }] }],
							generationConfig: { temperature: 0.2, maxOutputTokens: maxOutput },
						}),
					}
				);
				telemetry.status_code = res.status;
				telemetry.retries = attempts - 1;
				if (res.ok) {
					const data: any = await res.json();
					const text = extractGeminiText(data);
					if (text) {
						return { ok: true, text, telemetry };
					}
					lastReason = 'empty_response';
					logger('llm_retry', { provider: 'gemini', model, retry, reason: lastReason });
				} else if (res.status === 429 || res.status === 503) {
					lastReason = `transient_${res.status}`;
					logger('llm_retry', { provider: 'gemini', model, retry, status: res.status });
					await waitMs(jitter(300 * Math.pow(2, retry), 2800));
					continue;
				} else {
					lastReason = `gemini_${res.status}`;
					const body = await safeSnippet(res);
					logger('llm_error', { provider: 'gemini', model, status: res.status, body });
					return { ok: false, reason: lastReason, telemetry };
				}
			} catch (err) {
				lastReason = `gemini_exception_${(err as Error).name || 'unknown'}`;
				logger('llm_retry_error', { provider: 'gemini', model, retry, error: String(err) });
				telemetry.status_code = 0;
				await waitMs(jitter(400 * Math.pow(2, retry), 2200));
			}
		}
	}

	return {
		ok: false,
		reason: lastReason,
		telemetry,
	};
}

function localFallback(prompt: string): LLMResult {
	const telemetry: ScoreTelemetry = {
		model_used: 'local-template',
		retries: 0,
		status_code: 200,
		provider: 'local',
	};
	const text = JSON.stringify({
		signal_score: 5,
		role_tag: 'Lead',
		quick_action: 'Review macro-shift news',
		why: 'Fallback scoring: automatic template used when LLM unavailable. Assign human reviewer.',
		decision_window: '7–30d',
		affected_steps: [],
		kpi_impact: [],
		status: 'Researching',
	});
	return { ok: true, text, telemetry };
}

function extractGeminiText(data: any): string | undefined {
	const parts = data?.candidates?.[0]?.content?.parts;
	if (Array.isArray(parts)) {
		const combined = parts
			.map((part: any) => part?.text)
			.filter((v: unknown): v is string => typeof v === 'string')
			.join('\n');
		if (combined) return combined;
	}
	return data?.candidates?.[0]?.content?.parts?.[0]?.text;
}

function flagEnabled(value: string | undefined, fallback = false): boolean {
	if (value === undefined) return fallback;
	return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}

function clamp(n: number, min: number, max: number): number {
	if (Number.isNaN(n)) return min;
	return Math.max(min, Math.min(max, n));
}

async function safeSnippet(res: Response): Promise<string> {
	try {
		return (await res.text()).slice(0, 180);
	} catch {
		return 'unavailable';
	}
}

function waitMs(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(base: number, max: number): number {
	const jitterVal = Math.random() * base;
	return Math.min(max, base + jitterVal);
}
