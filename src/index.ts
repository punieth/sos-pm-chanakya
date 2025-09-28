export default {
	async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
	  console.log("HIT_SCHEDULED");
	  ctx.waitUntil(run(env));
	},
  
	async fetch(request: Request, env: Env): Promise<Response> {
	  const url = new URL(request.url);
	  console.log("HIT_FETCH", request.method, url.pathname);
  
	  // --- NEW: score endpoint ---
	  if (url.pathname === "/score-new") {
		try {
		  const limit = parseInt((env as any).SCORE_BATCH_SIZE || "15", 10);
		  const pages = await notionFetchUnscored(env, limit);
		  if (!pages.length) return json({ ok: true, message: "No unscored rows." });
  
		  const results: Array<{ id: string; ok: boolean; error?: string }> = [];
		  for (const p of pages) {
			try {
			  const shaped = shapeContentForLLM(p);
			  const llmJson = await callLLM(env, shaped.prompt);
			  const parsed = safeParseLLM(llmJson);
			  const mapped = mapLLMToNotion(parsed);
			  await notionUpdateScoring(env, p.id, mapped);
			  results.push({ id: p.id, ok: true });
			} catch (e: any) {
			  results.push({ id: p.id, ok: false, error: String(e?.message || e) });
			}
		  }
		  return json({ ok: true, updated: results.filter(r => r.ok).length, results });
		} catch (e: any) {
		  return json({ ok: false, error: String(e?.message || e) }, 500);
		}
	  }
  
	  if (url.pathname === "/run") {
		const force = url.searchParams.get("force") === "1";
		const details = await run(env, { force });
		return json(details);
	  }
  
	  // Debug probe (optional)
	  if (url.pathname === "/debug/fetch") {
		const target = url.searchParams.get("url");
		if (!target) return new Response("missing ?url=", { status: 400 });
		const txt = await safeGet(target);
		if (!txt) return new Response("fetch failed", { status: 502 });
		return new Response(txt.slice(0, 1200), { headers: { "content-type": "text/plain" } });
	  }
  
	  // Admin: clear KV (unchanged)
	  if (url.pathname === "/admin/kv/clear-all") {
		const token = url.searchParams.get("token") || "";
		if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
		  return json({ ok: false, error: "unauthorized" }, 401);
		}
		const deleted = await clearAllKV(env.SEEN);
		return json({ ok: true, deleted });
	  }
  
	  if (url.pathname === "/health") return new Response("OK", { status: 200 });
  
	  return new Response("OK\nVisit /run to execute.", { status: 200 });
	}
  };
  
  // ---------- Config ----------
  const MAX_AGE_DAYS = 90;          // skip items older than this
  const MAX_CREATE_PER_FEED = 50;   // cap creates per feed on a single run
  
  // ---------- Types ----------
  interface Env {
	SEEN: KVNamespace;
	NOTION_TOKEN: string;
	NOTION_DATABASE_ID: string;
	FEED_URLS: string; // comma-separated
	ADMIN_TOKEN?: string;
  }
  
  type TypeName = "Platform" | "Rails" | "Marketplace" | "Coverage" | "Long-form";
  type FormatName = "Short-form" | "Long-form";
  
  type FeedItem = { title: string; link: string; pubDate?: string };
  
  type FeedStats = {
	scanned: number;
	created: number;
	skippedSeen: number;
	skippedNoise: number;
	skippedOld: number;
	samples: string[];
  };
  
  type RunResult = {
	totals: { scanned: number; created: number; skippedSeen: number; skippedNoise: number; skippedOld: number };
	feeds: Record<string, FeedStats>;
  };
  
  // ---------- Runner ----------
  async function run(env: Env, opts: { force?: boolean } = {}): Promise<RunResult> {
	console.log("HIT_RUN");
	const raw = (env.FEED_URLS || "").toString();
	console.log("FEEDS_ACTIVE_RAW", raw);
  
	const feeds = raw
	  .split(",")
	  .map(s => s.trim())
	  .filter(Boolean)
	  .map(normalizeFeedUrl);
  
	console.log("FEEDS_PARSED", feeds);
  
	const feedsOut: Record<string, FeedStats> = {};
	let T_scanned = 0, T_created = 0, T_seen = 0, T_noise = 0, T_old = 0;
  
	for (const feed of feeds) {
	  console.log("FEED_START", feed);
	  let scanned = 0, created = 0, skippedSeen = 0, skippedNoise = 0, skippedOld = 0;
	  const samples: string[] = [];
  
	  const xml = await safeGet(feed);
	  if (!xml) {
		console.log("FEED_NO_XML", feed);
		feedsOut[feed] = { scanned, created, skippedSeen, skippedNoise, skippedOld, samples };
		continue;
	  }
  
	  // Parse + sort newest first
	  const items = parseFeed(xml).sort((a, b) => {
		const da = Date.parse(a.pubDate || "") || 0;
		const db = Date.parse(b.pubDate || "") || 0;
		return db - da;
	  });
  
	  console.log("FEED_ITEMS", feed, items.length);
  
	  for (const item of items) {
		scanned++;
		if (samples.length < 3) samples.push(item.title || "(no title)");
		if (!item.link) continue;
  
		const cls = classifyItem(item.title);
		if (cls === "Noise") { skippedNoise++; continue; }
  
		const publishedAt = normalizeDate(item.pubDate);
		if (isTooOld(publishedAt)) { skippedOld++; continue; }
  
		const key = `v2:${hash(feed)}:${hash(item.link)}`;
		if (!opts.force) {
		  const seen = await env.SEEN.get(key);
		  if (seen) { skippedSeen++; continue; }
		}
  
		const type: TypeName = (cls === "Long-form") ? "Long-form" : guessType(feed);
		const format: FormatName = (cls === "Long-form") ? "Long-form" : "Short-form";
  
		try {
		  await createNotionPage(env, {
			sourceName: guessSourceName(feed),
			url: item.link,
			type,
			format,
			publishedAt
		  });
		  if (!opts.force) await env.SEEN.put(key, "1");
		  created++;
		  console.log("NEW →", item.title, "(", type, "/", format, ")");
  
		  if (created >= MAX_CREATE_PER_FEED) {
			console.log("FEED_CAP_REACHED", feed, created);
			break;
		  }
		} catch (e) {
		  console.error("NOTION_ERR", feed, item.title, e);
		}
	  }
  
	  feedsOut[feed] = { scanned, created, skippedSeen, skippedNoise, skippedOld, samples };
	  console.log("FEED_END", feed, { scanned, created, skippedSeen, skippedNoise, skippedOld });
	  T_scanned += scanned; T_created += created; T_seen += skippedSeen; T_noise += skippedNoise; T_old += skippedOld;
	}
  
	const total = { scanned: T_scanned, created: T_created, skippedSeen: T_seen, skippedNoise: T_noise, skippedOld: T_old };
	console.log("RUN_SUMMARY", total);
	return { totals: total, feeds: feedsOut };
  }
  
  // Normalize known-tricky feeds (safety)
  function normalizeFeedUrl(u: string): string {
	try {
	  const url = new URL(u);
	  if (url.hostname.includes("android-developers.googleblog.com")) {
		// Prefer stable RSS variant; ignore whatever was passed
		return "https://android-developers.googleblog.com/feeds/posts/default?alt=rss";
	  }
	  return url.toString();
	} catch {
	  return u.trim();
	}
  }
  
  // ---------- KV utilities ----------
  async function clearAllKV(ns: KVNamespace): Promise<number> {
	let count = 0;
	let cursor: string | undefined = undefined;
	do {
	  const list: Awaited<ReturnType<typeof ns.list>> = await ns.list({ cursor, limit: 1000 });
	  if (list.keys.length === 0) break;
	  await Promise.all(list.keys.map(k => ns.delete(k.name)));
	  count += list.keys.length;
	  cursor = list.list_complete ? undefined : list.cursor;
	} while (cursor);
	return count;
  }
  
  // ---------- HTTP helpers ----------
  function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data, null, 2), {
	  status,
	  headers: { "content-type": "application/json" }
	});
  }
  
  // ---------- Networking ----------
  async function safeGet(url: string): Promise<string | null> {
	console.log("ENTER_safeGet", url);
	try {
	  const res = await fetch(url, {
		headers: {
		  "Accept": "application/rss+xml, application/atom+xml, text/xml",
		  "User-Agent": "Mozilla/5.0 (compatible; PMDigestWorker/1.0)"
		}
	  });
	  if (!res.ok) { console.log("FETCH_FAIL", url, "status:", res.status); return null; }
	  const txt = await res.text();
	  console.log("FETCH_OK", url, "bytes:", txt.length);
	  return txt;
	} catch (e) {
	  console.log("FETCH_ERR", url, String(e));
	  return null;
	}
  }
  
  // ---------- Parsing (RSS + Atom) ----------
  function parseFeed(xml: string): FeedItem[] {
	const items: FeedItem[] = [];
  
	// RSS
	const itemBlocks = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
	for (const b of itemBlocks) {
	  const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
	  const link =
		pick(b, /<link[^>]*>([\s\S]*?)<\/link>/i) ||
		pick(b, /<link[^>]*href="([^"]+)"/i);
	  const pub =
		pick(b, /<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i) ||
		pick(b, /<dc:date[^>]*>([\s\S]*?)<\/dc:date>/i);
	  items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}
  
	// Atom
	const entryBlocks = xml.match(/<entry[\s\S]*?<\/entry>/gi) || [];
	for (const b of entryBlocks) {
	  const title = pick(b, /<title[^>]*>([\s\S]*?)<\/title>/i);
	  const link =
		pick(b, /<link[^>]*rel="alternate"[^>]*href="([^"]+)"/i) ||
		pick(b, /<link[^>]*href="([^"]+)"[^>]*rel="alternate"[^>]*>/i) ||
		pick(b, /<link[^>]*href="([^"]+)"/i);
	  const pub =
		pick(b, /<updated[^>]*>([\s\S]*?)<\/updated>/i) ||
		pick(b, /<published[^>]*>([\s\S]*?)<\/published>/i);
	  items.push({ title: clean(title), link: clean(link), pubDate: clean(pub) });
	}
  
	// Dedup by link
	const seen = new Set<string>();
	return items.filter(x => {
	  if (!x.link) return false;
	  if (seen.has(x.link)) return false;
	  seen.add(x.link);
	  return true;
	});
  }
  
  function pick(text: string, re: RegExp): string {
	const m = text.match(re);
	return m ? m[1] : "";
  }
  function clean(s: string): string {
	return (s || "").replace(/<!\[CDATA\[|\]\]>/g, "").trim();
  }
  
  // ---------- Classifier & mapping ----------
  function classifyItem(title = ""): "Noise" | "Long-form" | "OK" {
	const t = title.toLowerCase();
	const junk = ["hiring", "careers", "job opening", "we're hiring", "award", "funding round"];
	if (junk.some(w => t.includes(w))) return "Noise";
	if (t.includes("podcast") || t.includes("webinar") || t.includes("livestream")) return "Long-form";
	return "OK";
  }
  
  function guessSourceName(feedUrl: string): string {
	try {
	  const h = new URL(feedUrl).hostname;
	  if (h.includes("googleblog")) return "Android Dev Blog";
	  if (h.includes("apple.com") && feedUrl.includes("/news/")) return "Apple Developer News";
	  if (h.includes("google.com") && feedUrl.includes("/search/")) return "Google Search Central";
	  if (h.includes("medianama")) return "Medianama";
	  if (h.includes("inc42")) return "Inc42";
	  if (h.includes("ondc")) return "ONDC";
	  if (h.includes("npci")) return "NPCI";
	  if (h.includes("flipkart")) return "Flipkart Newsroom";
	  if (h.includes("amazon")) return "Amazon Seller Announcements";
	  return h;
	} catch { return feedUrl; }
  }
  
  function guessType(feedUrl: string): TypeName {
	const h = new URL(feedUrl).hostname;
	if (h.includes("google") || h.includes("googleblog") || h.includes("apple") || h.includes("whatsapp") || h.includes("facebook") || h.includes("youtube"))
	  return "Platform";
	if (h.includes("npci") || h.includes("ondc") || h.includes("trai"))
	  return "Rails";
	if (h.includes("amazon") || h.includes("flipkart") || h.includes("delhivery") || h.includes("ecomexpress"))
	  return "Marketplace";
	if (h.includes("medianama") || h.includes("inc42") || h.includes("entrackr") || h.includes("techcrunch") || h.includes("indiatimes"))
	  return "Coverage";
	return "Coverage";
  }
  
  function normalizeDate(s?: string): string | undefined {
	if (!s) return undefined;
	const d = new Date(s);
	return isNaN(d.getTime()) ? undefined : d.toISOString();
  }
  
  function isTooOld(publishedAt?: string): boolean {
	if (!publishedAt) return false; // keep if unknown
	const d = new Date(publishedAt);
	if (isNaN(d.getTime())) return false;
	const ageMs = Date.now() - d.getTime();
	return ageMs > MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  }
  
  // ---------- Notion write ----------
  async function createNotionPage(
	env: Env,
	{ sourceName, url, type, format, publishedAt }:
	{ sourceName: string; url: string; type: TypeName; format: FormatName; publishedAt?: string }
  ) {
	const props: Record<string, any> = {
	  "Source Name": { title: [{ text: { content: sourceName } }] },
	  "URL": { url },
	  "Type": { select: { name: type } },
	  "Format": { select: { name: format } },
	  "Imported By": { rich_text: [{ text: { content: "Cloudflare Worker" } }] },
	  "Last Checked": { date: { start: new Date().toISOString() } }
	};
	if (publishedAt) props["Published At"] = { date: { start: publishedAt } };
  
	const payload = { parent: { database_id: env.NOTION_DATABASE_ID }, properties: props };
  
	const res = await fetch("https://api.notion.com/v1/pages", {
	  method: "POST",
	  headers: {
		Authorization: `Bearer ${env.NOTION_TOKEN}`,
		"Notion-Version": "2022-06-28",
		"Content-Type": "application/json"
	  },
	  body: JSON.stringify(payload)
	});
  
	if (!res.ok) {
	  const txt = await res.text();
	  throw new Error(`Notion error (${res.status}): ${txt}`);
	}
  }
  
  // ---------- tiny non-crypto hash ----------
  function hash(s: string): string {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = ((h << 5) + h) + s.charCodeAt(i);
	return (h >>> 0).toString(36);
  }

  /********************
 * /score-new endpoint
 * - Reads latest unscored rows from Notion
 * - Calls LLM
 * - Updates scoring fields back to Notion
 ********************/
interface ScoreResult {
	id: string;
	ok: boolean;
	error?: string;
  }

  type NotionPageLite = {
	id: string;
	url?: string;
	title?: string;      // Source Name
	sourceName?: string; // redundant, but handy
	type?: string;       // Select
	format?: string;     // Select
  };
  
  async function notionFetchUnscored(env: Env, limit: number): Promise<NotionPageLite[]> {
	// Filter: Signal Score is empty (null)
	const body = {
	  filter: {
		property: "Signal Score",
		number: { is_empty: true }
	  },
	  sorts: [
		{ property: "Published At", direction: "descending" },
		{ property: "Last Checked", direction: "descending" }
	  ],
	  page_size: Math.min(limit, 25)
	};
  
	const res = await fetch(`https://api.notion.com/v1/databases/${env.NOTION_DATABASE_ID}/query`, {
	  method: "POST",
	  headers: notionHeaders(env),
	  body: JSON.stringify(body)
	});
	if (!res.ok) throw new Error(`Notion query failed: ${res.status} ${await res.text()}`);
	const data = await res.json();
  
	const pages: NotionPageLite[] = [];
	for (const r of (data.results || [])) {
	  const props = r.properties || {};
	  pages.push({
		id: r.id,
		url: props["URL"]?.url,
		title: extractTitle(props["Source Name"]),
		sourceName: extractTitle(props["Source Name"]),
		type: props["Type"]?.select?.name,
		format: props["Format"]?.select?.name
	  });
	}
	return pages;
  }
  
  function extractTitle(titleProp: any): string | undefined {
	const arr = titleProp?.title || [];
	if (!Array.isArray(arr) || arr.length === 0) return undefined;
	return arr.map((t: any) => t.plain_text || t.text?.content).filter(Boolean).join(" ").trim();
  }
  
  function notionHeaders(env: Env): Record<string, string> {
	return {
	  Authorization: `Bearer ${env.NOTION_TOKEN}`,
	  "Notion-Version": "2022-06-28",
	  "Content-Type": "application/json"
	};
  }
  
  type NotionScorePayload = {
	signalScore: number;
	roleTag: "IC" | "Lead" | "VP";
	quickAction: string;
	why: string;
	decisionWindow: "<7d" | "7–30d" | ">30d";
	affectedSteps: string[]; // Multi-select names
	kpiImpact: string[];     // Multi-select names
	status: "Keep" | "Archive" | "Researching" | "Draft";
  };
  
  async function notionUpdateScoring(env: Env, pageId: string, s: NotionScorePayload): Promise<void> {
	const props: any = {
	  "Signal Score": { number: clamp(+s.signalScore, 0, 10) },
	  "Role Tag": { select: { name: s.roleTag } },
	  "Quick Action": { rich_text: [{ text: { content: s.quickAction.slice(0, 1000) } }] },
	  "Why": { rich_text: [{ text: { content: s.why.slice(0, 4000) } }] },
	  "Decision Window": { select: { name: s.decisionWindow } },
	  "Affected Steps": { multi_select: s.affectedSteps.map(n => ({ name: n })) },
	  "KPI Impact": { multi_select: s.kpiImpact.map(n => ({ name: n })) },
	  "Status": { select: { name: s.status } }
	};
  
	const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
	  method: "PATCH",
	  headers: notionHeaders(env),
	  body: JSON.stringify({ properties: props })
	});
	if (!res.ok) throw new Error(`Notion update failed: ${res.status} ${await res.text()}`);
  }
  
  function clamp(n: number, lo: number, hi: number) {
	return Math.max(lo, Math.min(hi, n));
  }

// put near your other types
type GeminiModel = { name: string };

// --- helper: list models available to THIS key
async function listGeminiModels(key: string): Promise<GeminiModel[]> {
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${key}`);
  if (!res.ok) throw new Error(`Gemini listModels error ${res.status}: ${await res.text()}`);
  const data: any = await res.json();
  const models: GeminiModel[] = (data.models || []).map((m: any) => ({ name: m.name || "" }));
  return models;
}

// --- helper: choose best text model from the list
function pickBestGeminiModel(models: GeminiModel[]): string | null {
  const names = models.map(m => m.name); // e.g., "models/gemini-1.5-flash-002"
  // Priorities (highest first)
  const prefs = [
    /gemini-1\.5-flash-002/i,
    /gemini-1\.5-flash-latest/i,
    /gemini-1\.5-flash/i,
    /gemini-1\.5-pro-002/i,
    /gemini-1\.5-pro-latest/i,
    /gemini-1\.5-pro/i,
    /gemini-1\.0-pro/i,
    /flash/i,
    /pro/i
  ];
  for (const re of prefs) {
    const hit = names.find(n => re.test(n));
    if (hit) return hit.replace(/^models\//, ""); // strip "models/"
  }
  return names[0]?.replace(/^models\//, "") || null;
}

// --- callLLM: lists models once, caches working model, then generateContent
async function callLLM(env: Env, prompt: string): Promise<string> {
  const provider = (env as any).LLM_PROVIDER || "openai";

  if (provider === "gemini") {
    const key = (env as any).GEMINI_API_KEY;
    if (!key) throw new Error("Missing GEMINI_API_KEY");

    // Try to use cached working model first (KV key: GEMINI_MODEL)
    let model = await env.SEEN.get("GEMINI_MODEL"); // reuse KV
    if (!model) {
      const models = await listGeminiModels(key);
      if (!models.length) throw new Error("Gemini: no models available to this key");
      const chosen = pickBestGeminiModel(models);
      if (!chosen) throw new Error("Gemini: could not select a model from list");
      model = chosen; // e.g., "gemini-1.5-flash-002"
      await env.SEEN.put("GEMINI_MODEL", model);
      console.log("GEMINI_MODEL_SELECTED", model);
    }

    // Always use v1beta for AI Studio keys
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
    const body = {
      contents: [{ role: "user", parts: [{ text: prompt }] }],
      generationConfig: { temperature: 0.2 }
    };

    let res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });

    // If Google rotated the model name, clear cache and retry once
    if (res.status === 404) {
      await env.SEEN.delete("GEMINI_MODEL");
      const models = await listGeminiModels(key);
      const chosen = pickBestGeminiModel(models);
      if (!chosen) throw new Error(`Gemini: 404 on ${model} and no fallback found`);
      model = chosen;
      await env.SEEN.put("GEMINI_MODEL", model);
      const retryUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
      res = await fetch(retryUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
    }

    if (!res.ok) throw new Error(`Gemini error ${res.status}: ${await res.text()}`);

    const data: any = await res.json();
    const text =
      data?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text).filter(Boolean).join("\n") ||
      data?.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!text) throw new Error("Gemini returned no text");
    return text;
  }

  // --- OpenAI fallback (requires OPENAI_API_KEY) ---
  const key = (env as any).OPENAI_API_KEY;
  if (!key) throw new Error("Missing OPENAI_API_KEY");
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${key}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      temperature: 0.2,
      messages: [
        { role: "system", content: "You are a pragmatic product manager for India-focused consumer apps. Return STRICT JSON only." },
        { role: "user", content: prompt }
      ]
    })
  });
  if (!res.ok) throw new Error(`OpenAI error ${res.status}: ${await res.text()}`);
  const data: any = await res.json();
  const text = data?.choices?.[0]?.message?.content;
  if (!text) throw new Error("OpenAI returned no text");
  return text;
}
  
  function shapeContentForLLM(p: NotionPageLite): { prompt: string } {
	// We pass minimal info; the LLM will fetch context from the URL if you later add a web-fetch step.
	const title = p.title || "(no title)";
	const url = p.url || "";
	const type = p.type || "";
	const format = p.format || "";
  
	const prompt = `
  You score news for PMs in India. Given a single item:
  
  TITLE: ${title}
  URL: ${url}
  TYPE: ${type}
  FORMAT: ${format}
  
  Return STRICT JSON with keys:
  {
	"signal_score": 0-10 number,
	"role_tag": "IC"|"Lead"|"VP",
	"quick_action": "one actionable sentence",
	"why": "3-5 lines of reasoning tailored to Indian PMs",
	"decision_window": "<7d"|"7–30d"|">30d",
	"affected_steps": ["Search","Browse","Signup","Checkout","Payment","Refund","Returns","Notifications","Pricing","Policy"],
	"kpi_impact": ["Conversion","Churn","CAC","NPS","Approval","CTR","ASO","SEO"],
	"status": "Keep"|"Archive"|"Researching"|"Draft"
  }
  
  Scoring guidance:
  - Prefer PLATFORM/RAILS changes with deadlines, enforcement, policy or distribution impact.
  - If it's generic coverage, likely "Archive" unless it changes pricing, ranking, or rules.
  - Decision window: if there's a deadline/enforcement within 7 days → "<7d"; within 30 days → "7–30d"; otherwise ">30d".
  - Role tag: IC for tactical quick fixes; Lead for cross-team work; VP for strategic org/product shifts.
  
  Reply with JSON ONLY. No prose.
  `.trim();
  
	return { prompt };
  }
  
  function safeParseLLM(raw: string): any {
	// Strip code fences if present and parse
	const cleaned = raw.replace(/```json|```/g, "").trim();
	try {
	  return JSON.parse(cleaned);
	} catch {
	  // Try to salvage the first {...} block
	  const m = cleaned.match(/\{[\s\S]*\}/);
	  if (!m) throw new Error("LLM did not return JSON");
	  return JSON.parse(m[0]);
	}
  }
  
  function mapLLMToNotion(j: any): NotionScorePayload {
	const role = pickEnum(j.role_tag, ["IC","Lead","VP"], "IC") as "IC"|"Lead"|"VP";
	const dw = pickEnum(j.decision_window, ["<7d","7–30d",">30d"], ">30d") as "<7d"|"7–30d"|">30d";
	const status = pickEnum(j.status, ["Keep","Archive","Researching","Draft"], "Keep") as "Keep"|"Archive"|"Researching"|"Draft";
  
	const stepsAll = ["Search","Browse","Signup","Checkout","Payment","Refund","Returns","Notifications","Pricing","Policy"];
	const kpiAll = ["Conversion","Churn","CAC","NPS","Approval","CTR","ASO","SEO"];
  
	const steps = Array.isArray(j.affected_steps) ? j.affected_steps.filter((x: string) => stepsAll.includes(x)) : [];
	const kpis = Array.isArray(j.kpi_impact) ? j.kpi_impact.filter((x: string) => kpiAll.includes(x)) : [];
  
	const score = typeof j.signal_score === "number" ? j.signal_score : 0;
	const qa = (j.quick_action || "").toString().slice(0, 1000);
	const why = (j.why || "").toString().slice(0, 4000);
  
	return {
	  signalScore: clamp(score, 0, 10),
	  roleTag: role,
	  decisionWindow: dw,
	  status,
	  affectedSteps: steps,
	  kpiImpact: kpis,
	  quickAction: qa,
	  why
	};
  }
  
  function pickEnum<T extends string>(val: any, allowed: T[], fallback: T): T {
	if (typeof val === "string" && allowed.includes(val as T)) return val as T;
	return fallback;
  }

  