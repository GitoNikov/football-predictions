"""
generate_analysis.py — Generate AI match analysis using Google Gemini API
and update data/matchday.json with fresh ai/aiEn fields.

Usage:
    export GEMINI_API_KEY=your_key_here
    python generate_analysis.py

Free tier: 1,500 requests/day · 15 req/min
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    from google import genai
except ImportError:
    sys.exit("❌  google-genai not installed. Run: pip install google-genai")

DATA_FILE  = Path("data/matchday.json")
MODEL_NAME = "gemini-1.5-flash"
DELAY      = 4.5   # seconds between calls — stays under 15 req/min


def build_prompt(match: dict, label: str) -> str:
    pick = match["pick"]
    prob = match["prob"]
    ctx  = match.get("aiCtx") or f"{match.get('homeEn')} vs {match.get('awayEn')}"
    return f"""You are a concise sports analyst for a football predictions website.

Match: {match['homeEn']} vs {match['awayEn']} ({label})
Context: {ctx}
Probabilities: Home {prob['h']}% | Draw {prob['d']}% | Away {prob['a']}%
Our pick: {pick['betEn']} @ {pick['odd']} (confidence {pick['conf']}%)

Write a short analysis (2-3 sentences, max 55 words each) in TWO languages.
Return ONLY valid JSON — no markdown, no extra text:
{{
  "bg": "Анализ на български тук.",
  "en": "Analysis in English here."
}}

Rules:
- Explain WHY the pick makes sense using the probabilities
- Mention the William Hill odds and confidence level
- Be direct and confident, avoid vague filler phrases
- Bulgarian must use Cyrillic script"""


def generate_analysis(client, match: dict, label: str) -> tuple[str, str]:
    prompt = build_prompt(match, label)
    try:
        response = client.models.generate_content(model=MODEL_NAME, contents=prompt)
        text = response.text.strip()
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


def main():
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        sys.exit("❌  Set GEMINI_API_KEY env var.\n    Free key: https://aistudio.google.com/app/apikey")

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found.")

    client = genai.Client(api_key=api_key)

    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    matches = data.get("upcoming", [])
    label   = data.get("labelEn", "Football")
    total   = len(matches)
    print(f"\n🤖  Generating AI analysis for {total} matches via {MODEL_NAME}…\n")

    updated = skipped = 0
    for i, match in enumerate(matches):
        name = f"{match.get('homeEn')} vs {match.get('awayEn')}"
        # Skip if AI already exists and match status is not pending
        if match.get("ai") and match.get("aiEn"):
            print(f"  ↩  {name} — already has AI, skipping")
            skipped += 1
            continue

        print(f"  ⚙  {name}… ", end="", flush=True)
        bg, en = generate_analysis(client, match, label)

        if bg and en:
            match["ai"]   = bg
            match["aiEn"] = en
            print("✓")
            updated += 1
        else:
            print("⚠  kept existing")
            skipped += 1

        if i < total - 1:
            time.sleep(DELAY)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Done! Updated {updated}, skipped {skipped}.")
    print(f"    Saved → {DATA_FILE}\n")


if __name__ == "__main__":
    main()
