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
import re
import sys
import time
from pathlib import Path

try:
    from groq import Groq
except ImportError:
    sys.exit("❌  groq not installed. Run: pip install groq")

DATA_FILE  = Path("data/matchday.json")
MODEL_NAME = "qwen/qwen3-32b"   # reasoning model — better arguments, slower
DELAY      = 4.0   # seconds between calls (deepseek-r1 TPM limit is tighter)

def extract_json(text: str) -> str:
    """
    Extract JSON from model output that may contain <think>...</think> blocks.
    Strategy: strip think block first; if nothing remains, try to find JSON
    inside the think block itself (qwen3 sometimes puts JSON inside thinking).
    As last resort, find the first {...} or [{...}] block in the full text.
    """
    stripped = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    if stripped:
        return stripped

    # JSON may be buried inside the think block — extract last {...} occurrence
    matches = re.findall(r'\{[^{}]*\}', text, re.DOTALL)
    return matches[-1] if matches else text


def to_bg_form(ctx: str) -> str:
    """Convert W/D/L form letters to Bulgarian П/Р/З inside form sections of the context."""
    def translate_segment(m):
        s = m.group()
        s = re.sub(r'\bW\b', 'П', s)
        s = re.sub(r'\bD\b', 'Р', s)
        s = re.sub(r'\bL\b', 'З', s)
        return s
    return re.sub(r'form[^.]+\.', translate_segment, ctx)


SYSTEM_MSG = (
    "You are a professional football analyst fluent in English and Bulgarian. "
    "Your Bulgarian writing sounds like a native Bulgarian sports journalist — "
    "natural, direct, and idiomatic. You never translate mechanically from English; "
    "you write each language independently."
)

# Market-specific guidance for sentence 2 (EN / BG)
_S2 = {
    ("btts", None): (
        "Sentence 2: make the BTTS case — reference both teams' goals scored and conceded per game to show both are likely to score",
        "Изречение 2: обоснови двата гола — посочи средния брой вкарани и допуснати голове, за да покажеш, че двата отбора ще вкарат",
    ),
    ("over_under", None): (
        "Sentence 2: make the Over 2.5 case — reference both teams' goals per game averages and high-scoring recent form results",
        "Изречение 2: обоснови над 2.5 гола — средни голове на мач и скорошни резултати с много голове",
    ),
    ("h2h", "draw"): (
        "Sentence 2: make the draw case — closeness in the table, evenly matched form, and H2H history if available",
        "Изречение 2: обоснови равен — близки позиции в класирането, изравнена форма, история на двубоите",
    ),
    ("h2h", "away"): (
        "Sentence 2: make the away win case — away team's quality, superior position, and recent form vs home team's weaknesses",
        "Изречение 2: обоснови гостуваща победа — класа на госта, предимство в класирането, форма срещу слабостите на домакина",
    ),
    ("h2h", "home"): (
        "Sentence 2: make the home win case — home team's position, scoring strength, and form advantage",
        "Изречение 2: обоснови домакинска победа — позиция, голова сила и предимство на формата на домакина",
    ),
}


def build_prompt(match: dict, label: str) -> str:
    pick    = match["pick"]
    prob    = match.get("prob", {"h": 50, "d": 25, "a": 25})
    ctx     = match.get("aiCtx") or f"{match.get('homeEn')} vs {match.get('awayEn')}"
    home_en = match.get("homeEn", "")
    away_en = match.get("awayEn", "")
    home_bg = match.get("home", home_en)
    away_bg = match.get("away", away_en)
    news        = match.get("newsCtx", "")
    conf_reason = pick.get("confReason", "")
    news_line        = f"\nLatest news (injuries/suspensions): {news}" if news else ""
    conf_reason_line = f"\nPick reasoning (use as the core argument for S2): {conf_reason}" if conf_reason else ""
    h2h_line         = "\nH2H note: the Context includes head-to-head history — weave the record and avg goals into sentence 2 where it supports the pick." if "H2H last" in ctx else ""
    bg_ctx  = to_bg_form(ctx)   # form letters already converted to П/Р/З

    market = pick.get("market", "h2h")
    sel    = pick.get("selection", "home")

    # Detect value/underdog angle: backing the less-favoured team in a h2h pick
    is_value_pick = (
        market == "h2h"
        and (
            (sel == "home" and prob.get("h", 50) < prob.get("a", 50))
            or (sel == "away" and prob.get("a", 50) < prob.get("h", 50))
        )
    )

    if is_value_pick:
        s2_en = (
            "Sentence 2: this is a VALUE bet on the statistical underdog — identify what specifically "
            "justifies the pick at these odds: home advantage, motivation (relegation/title/European "
            "stakes visible in the standings), the favourite's soft or inconsistent recent form, or "
            "a head-to-head anomaly. Be honest that the underdog is the weaker side on paper; do NOT "
            "claim they have overall statistical superiority if the Context shows otherwise."
        )
        s2_bg = (
            "Изречение 2: залогът е с добра стойност на аутсайдера — посочи конкретна причина: "
            "домакинско предимство, мотивация (борба за оцеляване/титла/Европа видима от класирането), "
            "непостоянната форма на фаворита или история на двубоите. Бъди честен, че аутсайдерът е "
            "по-слабият отбор на теория; не твърди, че има обща статистическа надмощия, ако "
            "контекстът показва обратното."
        )
    else:
        s2_key = (market, sel if market == "h2h" else None)
        s2_en, s2_bg = _S2.get(s2_key, _S2[("h2h", "home")])

    return f"""Match: {home_en} vs {away_en} ({label})
Context (English): {ctx}{news_line}{conf_reason_line}{h2h_line}
Context (Bulgarian — form letters already converted): {bg_ctx}
Probabilities: Home {prob['h']}% | Draw {prob['d']}% | Away {prob['a']}%
Pick: {pick['betEn']} @ {pick['odd']} William Hill

STRICT RULES — violating any of these invalidates the response:
1. Odds MUST be exactly {pick['odd']} in both languages — never round or alter.
2. Only cite facts from Context (positions, points, goals per game, form scores). Never invent statistics.
3. ENGLISH form entries: each entry is "W/D/L score" e.g. "W 2-0" — use exactly as in Context (English).
4. BULGARIAN form entries: copy exactly from the Bulgarian Context above (П, Р, З + score).
   NEVER use W, D, L, В, Л, Д or any other variant in the Bulgarian text.
5. Bulgarian team names MUST be exactly "{home_bg}" and "{away_bg}" — no other spelling.
6. For goals use "гола" — never "голови".
7. NEVER use first-person pronouns (ми, аз, ни, my, our) — write in third-person journalistic style.
8. Do NOT mention any confidence percentage in the text.

Write a 3-sentence match analysis. Return ONLY valid JSON, no markdown:
{{"bg": "...", "en": "..."}}

ENGLISH — factual, journalistic tone:
- Sentence 1: highlight the KEY DIFFERENTIAL between the two teams — pick the 2-3 most telling stats (position gap, points, goals per game, most relevant form results with scores). Do NOT list all form results verbatim; synthesise.
- {s2_en} — ground it in the Pick reasoning above if provided; include H2H if noted above
- Sentence 3: state the pick and exact odds {pick['odd']} — do NOT mention any confidence percentage

BULGARIAN — write as a native Bulgarian football journalist, NOT a translation:
- Use "{home_bg}" and "{away_bg}" as team names throughout — no exceptions
- Use form letters П (win), Р (draw), З (loss) — copy from Bulgarian Context above
- Reference goals per game naturally: "вкарват X гола на мач", "допускат Y гола"
- Natural vocabulary: двубой, форма, домакините, гостите, котировка, прогноза
- Active voice, present tense, third-person journalistic register — NEVER "ми", "аз", "ни"
- {s2_bg} — ground it in the Pick reasoning above if provided; include H2H if noted above
- Mirror the 3-sentence structure but phrased naturally — never translate word-for-word
- Sentence 1 (BG): same principle — synthesise the key differential, do not copy the full stat list
- Sentence 3: state the pick and exact odds {pick['odd']} — do NOT mention any confidence percentage"""


def generate_analysis(client, match: dict, label: str) -> tuple[str, str]:
    prompt = build_prompt(match, label)
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_MSG},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.5,
            max_tokens=3000,   # qwen3 thinking can be long; leave room for JSON output
        )
        text = response.choices[0].message.content.strip()
        text = extract_json(text)   # strip <think> blocks; fall back to inner JSON
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
        if "429" in str(e) or "rate_limit" in str(e).lower():
            print(f"\n    ⚠  Rate limited — waiting 60s…")
            time.sleep(60)
        else:
            print(f"\n    ⚠  API error: {e}")
        return "", ""


def ctx_hash(match: dict) -> str:
    content = match.get("aiCtx", "") + match.get("newsCtx", "")
    return hashlib.md5(content.encode()).hexdigest()[:8]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Regenerate analysis for all matches even if context is unchanged")
    args = parser.parse_args()

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

        # Skip stubs with no real pick (UEFA fixtures waiting for odds)
        if match.get("pick", {}).get("conf", 0) == 0:
            print(f"  ⚠  {name} — no pick yet, skipping")
            skipped += 1
            continue

        current_hash = ctx_hash(match)
        analysis_ok  = match.get("ai") and match.get("aiEn") and match.get("aiCtxHash") == current_hash

        if analysis_ok and not args.force:
            print(f"  ↩  {name} — context unchanged, skipping")
            skipped += 1
            continue

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
                    max_tokens=800,
                )
                text = resp.choices[0].message.content.strip()
                text = extract_json(text)
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
