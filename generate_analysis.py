"""
generate_analysis.py — Generate AI match analysis using Groq API (free tier)
and update data/matchday.json with fresh ai/aiEn fields.

Usage:
    export GROQ_API_KEY=your_key_here
    python generate_analysis.py

Free tier: 14,400 requests/day · 30 req/min (no credit card required)
Get a free key at: https://console.groq.com
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    from groq import Groq
except ImportError:
    sys.exit("❌  groq not installed. Run: pip install groq")

DATA_FILE  = Path("data/matchday.json")
MODEL_NAME = "llama-3.3-70b-versatile"
DELAY      = 2.5   # seconds between calls


def build_prompt(match: dict, label: str) -> str:
    pick = match["pick"]
    prob = match["prob"]
    ctx  = match.get("aiCtx") or f"{match.get('homeEn')} vs {match.get('awayEn')}"
    return f"""You are an expert football analyst writing for a predictions website.

Match: {match['homeEn']} vs {match['awayEn']} ({label})
Key context: {ctx}
Probabilities: Home {prob['h']}% | Draw {prob['d']}% | Away {prob['a']}%
Our pick: {pick['betEn']} @ {pick['odd']} William Hill (confidence {pick['conf']}%)

Write an informative analysis of 3 sentences in TWO languages.
Return ONLY valid JSON — no markdown, no extra text:
{{
  "bg": "Анализ на български тук.",
  "en": "Analysis in English here."
}}

Rules:
- Sentence 1: describe the current form/situation of both teams using the context facts
- Sentence 2: explain why the pick makes sense tactically and statistically
- Sentence 3: mention the William Hill odds, confidence level, and whether it represents value
- Be specific — use team names, numbers, and facts from the context
- Do NOT use vague phrases like "this is a good pick" or "the match will be interesting"
- Bulgarian must use Cyrillic script"""


def generate_analysis(client, match: dict, label: str) -> tuple[str, str]:
    prompt = build_prompt(match, label)
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
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
