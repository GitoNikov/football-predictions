"""
fetch_results.py — Check Odds API scores to mark finished matches W/L
and move them from upcoming → results in data/matchday.json.

Usage:
    export ODDS_API_KEY=your_key_here
    python fetch_results.py

Free tier: uses ~1 quota unit per sport.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed.")

BASE_URL  = "https://api.the-odds-api.com/v4"
DATA_FILE = Path("data/matchday.json")


def fetch_scores(api_key: str, sport: str, days_from: int = 3) -> list:
    url = f"{BASE_URL}/sports/{sport}/scores/"
    resp = requests.get(url, params={"apiKey": api_key, "daysFrom": days_from}, timeout=15)
    if resp.status_code in (401,):
        print("⚠  Invalid ODDS_API_KEY — skipping results check.")
        return []
    if resp.status_code in (404, 422):
        return []
    resp.raise_for_status()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"  ✓  Scores fetched · quota remaining: {remaining}")
    return resp.json()


def team_matches(api_home: str, api_away: str, our_home: str, our_away: str) -> bool:
    """Fuzzy match team names."""
    def norm(s): return s.lower().replace("fc ", "").replace(" fc", "").strip()
    ah, aa = norm(api_home), norm(api_away)
    oh, oa = norm(our_home), norm(our_away)
    return (oh in ah or ah in oh) and (oa in aa or aa in oa)


def determine_result(match: dict, scores: dict) -> str | None:
    """Return 'W', 'L', or None if undetermined."""
    market    = match["pick"].get("market", "h2h")
    selection = match["pick"].get("selection", "home")
    home_score = scores.get("home")
    away_score = scores.get("away")

    if home_score is None or away_score is None:
        return None

    h, a = int(home_score), int(away_score)

    if market == "h2h":
        if selection == "home":   return "W" if h > a else "L"
        if selection == "away":   return "W" if a > h else "L"
        if selection == "draw":   return "W" if h == a else "L"
    elif market == "btts":
        return "W" if (h > 0 and a > 0) else "L"
    elif market == "over_under":
        line  = float(match["pick"].get("line", 2.5))
        total = h + a
        if selection.startswith("over"):  return "W" if total > line else "L"
        if selection.startswith("under"): return "W" if total < line else "L"
    return None


def main():
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        sys.exit("❌  Set ODDS_API_KEY env var.")

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found.")

    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    sport    = data.get("sport", "soccer_epl")
    upcoming = data.get("upcoming", [])
    results  = data.get("results", [])
    record   = data.get("record", {"correct": 0, "total": 0, "period": ""})

    now = datetime.now(timezone.utc)

    # Only fetch if any upcoming match might have finished
    pending = [m for m in upcoming if m.get("status") == "pending"]
    if not pending:
        print("No pending matches — nothing to check.")
        return

    print(f"\n🔍  Checking scores for {len(pending)} pending matches…\n")
    api_scores = fetch_scores(api_key, sport)

    moved = 0
    remaining_upcoming = []

    for match in upcoming:
        if match.get("status") != "pending":
            remaining_upcoming.append(match)
            continue

        # Find matching event in API scores
        matched_event = None
        for event in api_scores:
            if event.get("completed") and team_matches(
                event["home_team"], event["away_team"],
                match["homeEn"], match["awayEn"]
            ):
                matched_event = event
                break

        if not matched_event:
            remaining_upcoming.append(match)
            continue

        # Parse score
        score_info = matched_event.get("scores") or []
        score_map  = {s["name"]: s["score"] for s in score_info}
        home_score = score_map.get(matched_event["home_team"])
        away_score = score_map.get(matched_event["away_team"])

        result = determine_result(match, {"home": home_score, "away": away_score})
        if result is None:
            remaining_upcoming.append(match)
            continue

        score_str = f"{home_score}-{away_score}" if home_score is not None else None

        result_entry = {
            "id":            match["id"],
            "home":          match["home"],
            "homeEn":        match["homeEn"],
            "away":          match["away"],
            "awayEn":        match["awayEn"],
            "date":          match["date"],
            "competition":   data.get("label", ""),
            "competitionEn": data.get("labelEn", ""),
            "pick":          match["pick"],
            "result":        result,
        }
        if score_str:
            result_entry["score"] = score_str

        results.insert(0, result_entry)  # newest first
        record["correct"] = record.get("correct", 0) + (1 if result == "W" else 0)
        record["total"]   = record.get("total", 0) + 1

        print(f"  {'✓' if result == 'W' else '✗'}  {match['homeEn']} vs {match['awayEn']} → {result}" +
              (f" ({score_str})" if score_str else ""))
        moved += 1

    data["upcoming"] = remaining_upcoming
    data["results"]  = results[:30]   # keep last 30
    data["record"]   = record

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Moved {moved} matches to results. Record: {record['correct']}/{record['total']}\n")


if __name__ == "__main__":
    main()
