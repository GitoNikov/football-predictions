"""
generate_analysis.py — Generate AI match analysis using Groq API (free tier)
and update data/matchday.json with fresh ai/aiEn fields.

Usage:
    export GROQ_API_KEY=your_key_here
    python generate_analysis.py

Free tier: 14,400 requests/day · 30 req/min (no credit card required)
Get a free key at: https://console.groq.com
"""

import hashlib
import json
import os
import sys
import time
from pathlib import Path

try:
    from groq import Groq
except ImportError:
    sys.exit("❌  groq not installed. Run: pip install groq")

try:
    from duckduckgo_search import DDGS
    _DDGS_AVAILABLE = True
except ImportError:
    _DDGS_AVAILABLE = False


def search_team_news(home_en: str, away_en: str) -> str:
    """Search DuckDuckGo for injuries/suspensions. Returns a short snippet or empty string."""
    if not _DDGS_AVAILABLE:
        return ""
    try:
        query   = f"{home_en} {away_en} team news injuries suspensions"
        results = DDGS().text(query, max_results=3)
        snippets = [r["body"] for r in results if r.get("body")]
        return " | ".join(snippets[:3]) if snippets else ""
    except Exception:
        return ""

DATA_FILE  = Path("data/matchday.json")
MODEL_NAME = "llama-3.3-70b-versatile"
DELAY      = 1.5   # seconds between calls

FORM_BG = str.maketrans("WDL", "ПРЗ")

def to_bg_form(ctx: str) -> str:
    """Convert W/D/L form letters to Bulgarian П/Р/З inside a context string."""
    import re
    return re.sub(
        r'(?<=form: )[\w-]+',
        lambda m: m.group().translate(FORM_BG),
        ctx,
    )


SYSTEM_MSG = (
    "You are a professional football analyst fluent in English and Bulgarian. "
    "Your Bulgarian writing sounds like a native Bulgarian sports journalist — "
    "natural, direct, and idiomatic. You never translate mechanically from English; "
    "you write each language independently."
)


def build_prompt(match: dict, label: str) -> str:
    pick    = match["pick"]
    prob    = match.get("prob", {"h": 50, "d": 25, "a": 25})
    ctx     = match.get("aiCtx") or f"{match.get('homeEn')} vs {match.get('awayEn')}"
    home_en = match.get("homeEn", "")
    away_en = match.get("awayEn", "")
    home_bg = match.get("home", home_en)
    away_bg = match.get("away", away_en)
    news    = match.get("newsCtx", "")
    news_line = f"\nLatest news (injuries/suspensions): {news}" if news else ""
    bg_ctx  = to_bg_form(ctx)   # form letters already converted to П/Р/З

    return f"""Match: {home_en} vs {away_en} ({label})
Context (English): {ctx}{news_line}
Context (Bulgarian — form letters already converted): {bg_ctx}
Probabilities: Home {prob['h']}% | Draw {prob['d']}% | Away {prob['a']}%
Pick: {pick['betEn']} @ {pick['odd']} William Hill (confidence {pick['conf']}%)

STRICT RULES — violating any of these invalidates the response:
1. Odds MUST be exactly {pick['odd']} in both languages — never round or alter.
2. Only cite facts from Context (positions, points, form). Never invent statistics.
3. ENGLISH form letters: use W, D, L exactly as in Context (English).
4. BULGARIAN form letters: copy exactly from the Bulgarian Context above (П, Р, З).
   NEVER use W, D, L, В, Л, Д or any other variant in the Bulgarian text.
5. Bulgarian team names MUST be exactly "{home_bg}" and "{away_bg}" — no other spelling.
6. For goals use "гола" — never "голови".

Write a 3-sentence match analysis. Return ONLY valid JSON, no markdown:
{{"bg": "...", "en": "..."}}

ENGLISH — factual, journalistic tone:
- Sentence 1: current league position, points, and form of both teams (from Context only)
- Sentence 2: why the pick makes sense based on form/position (no invented stats)
- Sentence 3: state the pick, exact odds {pick['odd']}, and confidence {pick['conf']}%

BULGARIAN — write as a native Bulgarian football journalist, NOT a translation:
- Use "{home_bg}" and "{away_bg}" as team names throughout — no exceptions
- Use form letters П (win), Р (draw), З (loss) — copy from Bulgarian Context above
- Natural vocabulary: двубой, форма, домакините, гостите, котировка, залог, прогноза
- Active voice, present tense, journalistic register
- Mirror the 3-sentence structure but phrased naturally — never translate word-for-word
- Odds must also be exactly {pick['odd']}"""


def generate_analysis(client, match: dict, label: str) -> tuple[str, str]:
    prompt = build_prompt(match, label)
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_MSG},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.7,
            max_tokens=500,
        )
        text = response.choices[0].message.content.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        result = json.loads(text)
        return result.get("bg", ""), result.get("en", "")
    except json.JSONDecodeError as e:
        print(f"\n    ⚠  JSON parse error: {e}")
        return "", ""
    except Exception as e:
        print(f"\n    ⚠  API error: {e}")
        return "", ""


def ctx_hash(match: dict) -> str:
    content = match.get("aiCtx", "") + match.get("newsCtx", "")
    return hashlib.md5(content.encode()).hexdigest()[:8]


def main():
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        sys.exit("❌  Set GROQ_API_KEY env var.\n    Free key: https://console.groq.com")

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found.")

    client = Groq(api_key=api_key)

    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    matches = data.get("upcoming", [])
    label   = data.get("labelEn", "Football")
    total   = len(matches)
    print(f"\n🤖  Generating AI analysis for {total} matches via {MODEL_NAME}…\n")

    updated = skipped = 0
    for i, match in enumerate(matches):
        name = f"{match.get('homeEn')} vs {match.get('awayEn')}"
        current_hash = ctx_hash(match)
        if match.get("ai") and match.get("aiEn") and match.get("aiCtxHash") == current_hash:
            print(f"  ↩  {name} — context unchanged, skipping")
            skipped += 1
            continue

        news = search_team_news(match.get("homeEn", ""), match.get("awayEn", ""))
        if news:
            match["newsCtx"] = news
        print(f"  ⚙  {name}… ", end="", flush=True)
        bg, en = generate_analysis(client, match, label)

        if bg and en:
            match["ai"]        = bg
            match["aiEn"]      = en
            match["aiCtxHash"] = current_hash
            print("✓")
            updated += 1
        else:
            print("⚠  kept existing")
            skipped += 1

        if i < total - 1:
            time.sleep(DELAY)

    # ── betBuilder reasoning ──────────────────────────────────────────────────
    bb = data.get("betBuilder")
    if bb and bb.get("matchId") and bb.get("markets") and not bb.get("reasoning"):
        mid = bb["matchId"]
        src = next((m for m in matches if m["id"] == mid), None)
        if src:
            home_en = bb.get("homeEn", src.get("homeEn", ""))
            away_en = bb.get("awayEn", src.get("awayEn", ""))
            home_bg = bb.get("home", src.get("home", home_en))
            away_bg = bb.get("away", src.get("away", away_en))
            market_list_en = ", ".join(
                mkt.get("marketEn", mkt.get("market", "")) for mkt in bb["markets"]
            )
            market_list_bg = ", ".join(
                mkt.get("market", mkt.get("marketEn", "")) for mkt in bb["markets"]
            )
            ai_ctx = src.get("aiCtx", "")
            prompt = (
                f'Bet builder for {home_en} vs {away_en}: [{market_list_en}]\n'
                f'Context: {ai_ctx}\n\n'
                f'Write 1-2 sentences explaining why these markets combine well. '
                f'Return ONLY valid JSON, no markdown:\n{{"bg": "...", "en": "..."}}\n\n'
                f'ENGLISH — concise, journalistic: explain why the combination is statistically sound.\n\n'
                f'BULGARIAN — write as a native Bulgarian football journalist, NOT a translation:\n'
                f'- Use "{home_bg}" and "{away_bg}" as team names\n'
                f'- Bulgarian markets: {market_list_bg}\n'
                f'- Natural vocabulary: комбинация, пазари, залог, котировка, вероятност, форма\n'
                f'- Never translate word-for-word; write each sentence originally in Bulgarian'
            )
            print(f"\n  ⚙  betBuilder reasoning for {home_en} vs {away_en}… ", end="", flush=True)
            try:
                resp = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": SYSTEM_MSG},
                        {"role": "user",   "content": prompt},
                    ],
                    temperature=0.7,
                    max_tokens=250,
                )
                text = resp.choices[0].message.content.strip()
                if "```" in text:
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.strip()
                result = json.loads(text)
                bb["reasoning"]   = result.get("bg", "")
                bb["reasoningEn"] = result.get("en", "")
                print("✓")
            except Exception as e:
                print(f"⚠  {e}")

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Done! Updated {updated}, skipped {skipped}.")
    print(f"    Saved → {DATA_FILE}\n")


if __name__ == "__main__":
    main()
