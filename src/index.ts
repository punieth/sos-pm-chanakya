export default {
	async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
	  console.log("HIT_SCHEDULED");
	  ctx.waitUntil(run(env));
	},
  
	async fetch(request: Request, env: Env): Promise<Response> {
	  const url = new URL(request.url);
	  console.log("HIT_FETCH", request.method, url.pathname);
  
	  if (url.pathname === "/run") {
		const force = url.searchParams.get("force") === "1";
		const details = await run(env, { force });
		return json(details);
	  }
  
	  // Quick probe: pull a URL and return first 1200 chars (for debugging feeds)
	  if (url.pathname === "/debug/fetch") {
		const target = url.searchParams.get("url");
		console.log("DEBUG_FETCH target =", target);
		if (!target) return new Response("missing ?url=", { status: 400 });
		const txt = await safeGet(target);
		if (!txt) return new Response("fetch failed", { status: 502 });
		return new Response(txt.slice(0, 1200), { headers: { "content-type": "text/plain" } });
	  }
  
	  // Admin: clear ALL KV keys (protect with token)
	  if (url.pathname === "/admin/kv/clear-all") {
		const token = url.searchParams.get("token") || "";
		if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
		  return json({ ok: false, error: "unauthorized" }, 401);
		}
		console.log("ADMIN_CLEAR_ALL");
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