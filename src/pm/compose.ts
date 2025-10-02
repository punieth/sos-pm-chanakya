import { ScoredItem } from '../types';
import { callLLM, LLMEnv } from '../providers/llm';
import { sanitize } from '../utils/text';
import { ShortlistedCandidate } from './select';
import { indiaRelevanceScore } from '../utils/india';

interface PmCompositionFields {
	hook: string;
	india_take: string;
	watch: string[];
	do_this_week: string;
}

export interface PmPost {
	title: string;
	url: string;
	domain: string;
	publishedAt?: string;
	eventClass: ScoredItem['eventClass'];
	impactScore: number;
	pmScore: number;
	urgency: '⚡' | '🛑' | '🧩';
	hook: string;
	indiaTake: string;
	watch: string[];
	doThisWeek: string;
	signals: string[];
	llmStatus: 'ok' | 'fallback' | 'error';
	llmReason?: string;
	llmProvider?: string;
}

// ---------- Class normalization (prevents TS union overlap issues) ----------
type CanonicalClass = 'POLICY' | 'COMMERCE' | 'LAUNCH' | 'PARTNERSHIP' | 'TREND' | 'OTHER';

const CLASS_MAP: Record<string, CanonicalClass> = {
  PRICING_POLICY: 'POLICY',
  PLATFORM_RULE: 'POLICY',
  CONTENT_POLICY: 'POLICY',
  DATA_PRIVACY: 'POLICY',
  POLICY: 'POLICY',
  PLATFORM_POLICY: 'POLICY',
  PRICING: 'POLICY',

  PAYMENT_COMMERCE: 'COMMERCE',
  COMMERCE: 'COMMERCE',
  PAYMENTS: 'COMMERCE',

  PARTNERSHIP_INTEGRATION: 'PARTNERSHIP',
  PARTNERSHIP: 'PARTNERSHIP',
  INTEGRATION: 'PARTNERSHIP',

  MODEL_LAUNCH: 'LAUNCH',
  PRODUCT_UPDATE: 'LAUNCH',
  LAUNCH: 'LAUNCH',
  ADDITION: 'LAUNCH',

  TREND_ANALYSIS: 'TREND',
  TREND: 'TREND',

  RISK_INCIDENT: 'OTHER',
  OTHER: 'OTHER',
};

function canonClass(raw: unknown): CanonicalClass {
	const key = String(raw || 'OTHER').toUpperCase();
	return CLASS_MAP[key] ?? 'OTHER';
}

type ToneHint = 'threat' | 'upside' | 'neutral';

const THREAT_CLASSES = new Set<CanonicalClass>(['POLICY', 'TREND']);
const UPSIDE_CLASSES = new Set<CanonicalClass>(['LAUNCH', 'PARTNERSHIP']);
function deriveTone(candidate: ShortlistedCandidate): ToneHint {
  const cls = canonClass(candidate.item.eventClass);
  const impact = candidate.item.impact.impact;
  if (candidate.urgency === '🛑') return 'threat';
  if (cls === 'POLICY') return 'threat';
  if (candidate.item.eventClass === 'RISK_INCIDENT') return 'threat';
  if (cls === 'LAUNCH' || cls === 'PARTNERSHIP') return 'upside';
  if (candidate.item.eventClass === 'PAYMENT_COMMERCE' && impact >= 0.65) return 'upside';
  return impact >= 0.75 ? 'upside' : 'neutral';
}

// --- Style helpers: make lines punchy + ban fluff ---
const REWRITE: Array<[RegExp, string]> = [
	[/\bconsider\b/gi, 'run'],
	[/\bleverage\b/gi, 'use'],
	[/\butilize\b/gi, 'use'],
	[/\bensure\b/gi, 'make'],
	[/\bmonitor\b/gi, 'watch'],
	[/\bassess\b/gi, 'gap-check'],
	[/\bsynergy|synergies\b/gi, 'fit'],
	[/\blearnings?\b/gi, 'findings'],
	[/\boptimize\b/gi, 'tighten'],
	[/\bstreamline\b/gi, 'simplify'],
	[/\bbrief\b/gi, 'huddle'],
	[/\bprepare\b/gi, 'ship'],
	[/\brecommend\b/gi, 'call'],
];

function punchy(line: string, max = 140): string {
	let out = (line || '').trim();
	REWRITE.forEach(([pattern, rep]) => (out = out.replace(pattern, rep)));
	out = out
		.replace(/\s+/g, ' ')
		.replace(/[–—-]\s*$/, '')
		.replace(/\s*[.·]*\s*$/, '');
	return out.slice(0, max);
}

function limit(s: string, n: number) {
	return (s || '').trim().slice(0, n);
}

export async function composePmPost(env: LLMEnv, candidate: ShortlistedCandidate): Promise<PmPost> {
	const tone = deriveTone(candidate);
	const prompt = buildPrompt(candidate, tone);
	const result = await callLLM(env, prompt, (phase, details) => {
		console.log('LLM_EVENT', phase, details);
	});
	const parsed = result.ok ? parseFields(result.text) : null;
	const provider = result.telemetry?.provider || 'unknown';
	console.log('LLM_CALL_RESULT', {
		title: candidate.item.title,
		provider,
		model: result.telemetry?.model_used || 'unknown',
		status: result.ok ? 'ok' : 'error',
		reason: result.reason,
		textLength: result.text ? result.text.length : 0,
		textPreview: result.text ? String(result.text).slice(0, 200) : undefined,
	});
	const status: PmPost['llmStatus'] = result.ok ? (parsed ? 'ok' : 'fallback') : 'error';
	const reason = !result.ok
		? result.reason || 'llm_call_failed'
		: parsed
		? undefined
		: provider === 'local'
		? 'llm_local_template'
		: 'parse_failed';
	const fieldsRaw = parsed || fallbackFields(candidate, tone);
	const fields = polishFields(fieldsRaw, candidate);

	const watchSignals = fields.watch.length >= 2 ? fields.watch.slice(0, 2) : ensureWatch(candidate, fields.watch);

	return {
		title: candidate.item.title,
		url: candidate.item.url,
		domain: candidate.item.domain,
		publishedAt: candidate.item.publishedAt,
		eventClass: candidate.item.eventClass,
		impactScore: candidate.item.impact.impact,
		pmScore: candidate.pmScore,
		urgency: candidate.urgency,
		hook: `${candidate.urgency} ${fields.hook}`.trim(),
		indiaTake: fields.india_take,
		watch: watchSignals,
		doThisWeek: fields.do_this_week,
		signals: candidate.signals,
		llmStatus: status,
		llmReason: reason,
		llmProvider: provider,
	};
}

// NEW: polish step
function polishFields(f: PmCompositionFields, _candidate: ShortlistedCandidate): PmCompositionFields {
	return {
		hook: limit(punchy(f.hook), 110),
		india_take: limit(punchy(f.india_take), 140),
		watch: (f.watch || []).map((w) => limit(punchy(w), 70)).slice(0, 2),
		do_this_week: limit(punchy(f.do_this_week), 110),
	};
}

function buildPrompt(candidate: ShortlistedCandidate, tone: ToneHint): string {
	const { item, pmScore, indiaScore, signals } = candidate;
	const title = sanitize(item.title) || 'Untitled story';
	const source = sanitize(item.source || item.domain || 'unknown source');
	const summary = sanitize(item.description) || 'No summary available.';
	const cls = canonClass(item.eventClass);
	const indiaHint =
		indiaScore >= 0.45
			? 'India impact is clear—speak directly to Indian users, markets, or regulators.'
			: 'India impact is weak. Explicitly say it is a watch item for India (not action now).';
	const breakdown = item.impact.components;
	const signalsText = signals.length ? signals.join(', ') : 'no additional signals';
	const authority = typeof breakdown.authority === 'number' ? breakdown.authority : 0;

	const EXAMPLE_GOOD = [
		{
			hook: 'Visa drops an AI payments hub—manual payables is legacy now.',
			india_take: "India B2B: expect higher auth + fewer fraud flags; fee revenue won't save weak rails.",
			watch: ['Auth success % vs baseline', 'False-positive fraud rate'],
			do_this_week: 'Run a 30-min spike: Visa API fit; propose PoC partner shortlist by Fri.',
		},
		{
			hook: 'RBI keeps UPI at ₹0—no fees, no excuses.',
			india_take: 'Monetization via UPI fees is dead; retention/credit rails matter more this quarter.',
			watch: ['UPI 7-day volume delta', 'Merchant opt-in %'],
			do_this_week: 'Ship a 1-pager: ‘UPI-no-fee moat’ with 2 bets we can launch in 2 weeks.',
		},
	];

	return `You are an Indian product war-room lead. Write a four-line PM brief in sharp, direct language.
Tone knob: ${tone}. If threat, make it urgent; if upside, make it opportunistic; neutral = steady.

Return STRICT JSON (no prose, no markdown) with EXACT keys:
{
  "hook": string,            // <= 110 chars, no emoji. Call the move bluntly; name the actor.
  "india_take": string,      // <= 140 chars. Concrete outcome for Indian users/regulators/revenue.
  "watch": [string, string], // two metric/decision signals, <= 70 chars each.
  "do_this_week": string     // imperative, <= 110 chars, one concrete action/KPI owner.
}

BANS: no hashtags, no bullets, no emojis, no “consider/leverage/synergy/ensure”.
Tone: decisive, visceral, operator. Prefer verbs: run, ship, spike, gap-check, harden, de-risk.

CONTEXT
- Title: ${title}
- Source: ${source}
- URL: ${item.url}
- Event archetype: ${cls}
- Impact score: ${(item.impact.impact * 100).toFixed(0)} / 100
- PM score: ${(pmScore * 100).toFixed(0)} / 100
- Impact breakdown: recency ${breakdown.recency.toFixed(2)}, novelty ${breakdown.graphNovelty.toFixed(
		2
	)}, reach ${breakdown.surfaceReach.toFixed(2)}, commerce ${breakdown.commerceTie.toFixed(2)}, india ${breakdown.indiaTie.toFixed(
		2
	)}, authority ${authority.toFixed(2)}, momentum ${breakdown.momentum.toFixed(2)}
- Summary: ${summary}
- Potential signals: ${signalsText}
- India guidance: ${indiaHint}
- Tone hint: ${tone.toUpperCase()} (write copy that matches this stance)

STYLE EXAMPLES (follow tone/shape, not content):
${JSON.stringify(EXAMPLE_GOOD, null, 2)}

Rules:
- Hook: state the move + consequence in one punchy line.
- India_take: spell the India impact (users, regulators, revenue). If weak India impact: say it’s a watch item.
- Watch: two crisp signals (metrics/decision gates), not generic “monitor”.
- Do_this_week: one command to an owner or KPI. No process fluff.
Return JSON ONLY.`;
}

function parseFields(text?: string | null): PmCompositionFields | null {
	if (!text) return null;
	try {
		let body = text.trim();
		const fenced = body.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
		if (fenced) body = fenced[1];
		const parsed = JSON.parse(body) as PmCompositionFields;
		if (!parsed || typeof parsed !== 'object') return null;
		if (typeof parsed.hook !== 'string') return null;
		if (typeof parsed.india_take !== 'string') return null;
		if (!Array.isArray(parsed.watch)) return null;
		if (typeof parsed.do_this_week !== 'string') return null;
		return {
			hook: parsed.hook.trim(),
			india_take: parsed.india_take.trim(),
			watch: parsed.watch
				.map((entry) => sanitize(String(entry)))
				.filter(Boolean)
				.slice(0, 2),
			do_this_week: parsed.do_this_week.trim(),
		};
	} catch (err) {
		console.log('Failed to parse PM LLM output', err instanceof Error ? err.message : err);
		return null;
	}
}

function fallbackFields(candidate: ShortlistedCandidate, tone: ToneHint): PmCompositionFields {
	const { item } = candidate;
	const hookBase = sanitize(item.title) || 'Big move just dropped.';
	const indiaHit = isIndiaRelevant(item)
		? 'India impact is real—users or regulators move next. Revenue shifts if we stall.'
		: 'India impact is weak—treat as watch item until a partner or regulator moves.';

	const cls = canonClass(item.eventClass);
	let action: string;

	switch (cls) {
		case 'POLICY':
			action = 'Spin up compliance+GTM huddle; ship regulator counter-plan by EOD.';
			break;
		case 'LAUNCH':
			action = 'Run a 30-min spike: API fit + moat check; propose PoC partners by Fri.';
			break;
		case 'PARTNERSHIP':
			action = 'Map integration scope; send partner shortlist + API gaps by Fri.';
			break;
		case 'COMMERCE':
			action = 'Gap-check payments KPIs; draft PoC to lift auth% / cut fraud by Fri.';
			break;
		case 'TREND':
			action = 'Write a 1-pager: why this trend matters; nominate a fast experiment.';
			break;
		default:
			action = 'Gap-check KPIs hit; write a 1-page call with next bet by Fri.';
			break;
	}

	if (tone === 'threat') {
		action = 'Assign response pod; lock owner + regulator call notes by EOD.';
	}
	if (tone === 'upside') {
		action = 'Draft go-live burst; name squad + KPI lift target by Fri.';
	}

	const tonePrefix = tone === 'threat' ? 'Red flag—' : tone === 'upside' ? 'Upside—' : 'Heads-up—';
	const hook = `${tonePrefix}${hookBase}`;

	const watch = ensureWatch(candidate, []);

	return {
		hook: limit(punchy(hook), 110),
		india_take: limit(punchy(indiaHit), 140),
		watch,
		do_this_week: limit(punchy(action), 110),
	};
}

function ensureWatch(candidate: ShortlistedCandidate, base: string[]): string[] {
	const { item } = candidate;
	const combined = (base || []).concat(candidate.signals || []).filter(Boolean);

	if (combined.length >= 2) return combined.slice(0, 2);

	const cls = canonClass(item.eventClass);
	const byClass: Record<CanonicalClass, string[]> = {
		POLICY: ['Effective date / clause', 'User impact % (fees/limits)'],
		COMMERCE: ['Conversion delta vs baseline', 'Auth success % / chargeback %'],
		LAUNCH: ['Waitlist size / API coverage', 'Partner count (signed/live)'],
		PARTNERSHIP: ['Integration depth (read/write)', 'Time-to-first-live'],
		TREND: ['7-day volume delta', 'Top merchant/segment adoption %'],
		OTHER: ['7-day volume delta', 'Top merchant/segment adoption %'],
	};

	const fallbacks = byClass[cls] || byClass.OTHER;
	return combined
		.concat(fallbacks)
		.slice(0, 2)
		.map((s) => limit(punchy(s), 70));
}

function isIndiaRelevant(item: ScoredItem): boolean {
	return (
		indiaRelevanceScore({
			title: item.title,
			description: item.description,
			source: item.source,
			domain: item.domain,
		}) >= 0.45
	);
}
