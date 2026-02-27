"""
generate_analysis.py — Generate AI match analysis using Google Gemini API
and update data/matchday.json with fresh ai/aiEn fields.

Usage:
    export GEMINI_API_KEY=your_key_here
    python generate_analysis.py

Free tier limits: 1,500 requests/day · 1M tokens/day · 15 req/min
Typical cost per run: ~18 matches × 2 fields = ~36 requests, ~7,000 tokens
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    import google.generativeai as genai
except ImportError:
    sys.exit("❌  google-generativeai not installed. Run: pip install google-generativeai")

# ── Config ──────────────────────────────────────────────────────────────────
DATA_FILE  = Path("data/matchday.json")
MODEL_NAME = "gemini-1.5-flash"
DELAY      = 4.5   # seconds between calls — stays safely under 15 req/min


# ── Prompt builder ──────────────────────────────────────────────────────────
def build_prompt(match: dict, league_label: str) -> str:
    pick = match["pick"]
    prob = match["prob"]
    ctx  = match.get("ctxEn") or match.get("ctx", "")

    return f"""You are a concise sports analyst for a football predictions website.

Match: {match['homeEn']} vs {match['awayEn']} ({league_label})
Context / first leg: {ctx}
Probabilities: Home {prob['h']}% | Draw {prob['d']}% | Away {prob['a']}%
Our pick: {pick['betEn']} (confidence {pick['conf']}%)

Write a short analysis (2-3 sentences, max 55 words each) in TWO languages.
Return ONLY valid JSON — no markdown, no extra text:
{{
  "bg": "Анализ на български тук.",
  "en": "Analysis in English here."
}}

Rules:
- Explain WHY the pick makes sense using the context and probabilities
- Mention odds value if confidence is high (≥65%)
- Be direct and confident, avoid vague filler phrases
- Bulgarian must use Cyrillic script"""


# ── API call ────────────────────────────────────────────────────────────────
def generate_analysis(model, match: dict, league_label: str) -> tuple[str, str]:
    prompt = build_prompt(match, league_label)
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()

        # Strip markdown code fences if Gemini wraps in ```json ... ```
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        return result.get("bg", ""), result.get("en", "")

    except json.JSONDecodeError as e:
        print(f"\n    ⚠  JSON parse error: {e}\n    Raw: {text[:120]}")
        return "", ""
    except Exception as e:
        print(f"\n    ⚠  API error: {e}")
        return "", ""


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        sys.exit(
            "❌  No API key found.\n"
            "    Set GEMINI_API_KEY env var.\n"
            "    Get a free key at https://aistudio.google.com/app/apikey"
        )

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found. Run from the repo root.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(MODEL_NAME)

    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    leagues = data.get("leagues", [])
    total   = sum(len(lg["matches"]) for lg in leagues)
    print(f"\n🤖  Generating AI analysis for {total} matches via {MODEL_NAME}…\n")

    updated = 0
    skipped = 0

    for league in leagues:
        label = league.get("labelEn") or league.get("label", "")
        print(f"📋  {label}  ({len(league['matches'])} matches)")

        for i, match in enumerate(league["matches"]):
            name = f"{match.get('homeEn', match.get('home'))} vs {match.get('awayEn', match.get('away'))}"
            print(f"  ⚙  {name}… ", end="", flush=True)

            bg, en = generate_analysis(model, match, label)

            if bg and en:
                match["ai"]   = bg
                match["aiEn"] = en
                print("✓")
                updated += 1
            else:
                print("⚠  kept existing")
                skipped += 1

            # Rate-limit: pause between calls (skip after last match)
            if i < len(league["matches"]) - 1:
                time.sleep(DELAY)

        print()

    # Save updated JSON
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅  Done! Updated {updated} matches, skipped {skipped}.")
    print(f"    Saved → {DATA_FILE}\n")


if __name__ == "__main__":
    main()
