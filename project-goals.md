

🧭 North Star Doc: Adaptive PM News Agent

Why This Exists

You (the builder) are not after another keyword RSS bot.
You’re building a PM intelligence agent that finds, filters, and frames events that matter for Indian product managers.
The bar: if Visa, PayPal, Jio, or OpenAI drop something, no Indian PM should be caught off guard.

⸻

Core Vision
	•	Outcome, not keywords: Detect shifts in digital/tech/commerce landscape that require PM action.
	•	India-first lens: Filter noise, promote only global events that touch India’s markets, regulators, or users.
	•	Adaptive, not static: The agent should refine archetypes, lexicons, and scores based on misses (e.g., Stripe × OpenAI news that slipped).

⸻

Principles
	1.	Unified Scoring System
	•	Every feed (RSS, NewsAPI, GDELT, Bing, etc.) → one pipeline.
	•	Shared impact formula: recency + novelty + reach + India tie + commerce tie.
	•	Big shifts must score >0.7, background noise <0.5.
	2.	Event Archetypes
	•	No “OTHER.”
	•	Archetypes Indian PMs care about:
	•	Launch/Unveil → new product, feature, model.
	•	Partnership/Integration → orgs joining forces.
	•	Policy/Rule Change → compliance, pricing, regulation.
	•	Payments/Commerce Shift → checkout, APIs, wallets.
	•	Archetypes expand dynamically via clustering, not hardcoding.
	3.	Global → India Filter
	•	Downweight US-only policies unless tied to India/global commerce.
	•	Upweight RBI, NPCI, TRAI, Jio, Paytm, Razorpay, global APIs impacting India.
	4.	Masala Output
	•	Kill jargon. Every item in 4 punchy lines:
	•	Hook: crisp headline.
	•	India take: why it matters here.
	•	Watch: 2 signals to track.
	•	Do this week: specific action.
	•	Urgency emojis tied to score (⚡ Hot, 🛑 Risk, 🧩 Trend).
	5.	Fail-Safe & Adaptive
	•	LLM failover → degrade gracefully to templates.
	•	Pipeline logs misses (e.g., misclassifications, skipped PayPal × Ooredoo) → refine lexicons and embeddings.
	•	System learns what surprised PMs and tunes itself.

⸻

What “Good” Looks Like
	•	Precision: fewer irrelevant policy/library items.
	•	Recall: no missed Visa × AI hub or Jio AI launch.
	•	Masala: every surfaced item feels Slack-ready without edits.
	•	Adaptiveness: the system improves week over week, not through hardcoding but by refining archetypes and weights.

⸻

Success Criteria
	•	80% of surfaced items are genuinely PM-relevant in India.
	•	Each “Hot Launch” output is <4 lines, actionable, and shareable.
	•	Impact scores consistently differentiate transformative (>0.7) vs trivial (<0.5).
	•	RoleTag in Notion = “Hot Launch” appears only on high-impact events.

⸻

Call to Codex/Gemini

Don’t patch rules. Don’t hardcode.
Build an adaptive agent that:
	•	Scores events consistently across all sources.
	•	Detects and evolves archetypes dynamically.
	•	Frames output in crisp, India-first PM language.
	•	Never crashes, always degrades gracefully.
	•	Self-audits misses and refines itself.

⸻

This is the compass. The code is just the road — the system must evolve toward this North Star.

⸻
