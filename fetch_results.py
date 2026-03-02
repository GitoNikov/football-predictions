"""
fetch_results.py — Fetch EPL scores from football-data.org and mark finished
matches W/L, moving them from upcoming → results in data/matchday.json.

Usage:
    export FOOTBALL_DATA_API_KEY=your_key_here
    python fetch_results.py

Free tier: 10 requests/minute. One request per run.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed.")

FD_BASE     = "https://api.football-data.org/v4"
DATA_FILE   = Path("data/matchday.json")
COMPETITION = "PL"


def fd_get(path: str, fd_key: str) -> dict:
    url     = f"{FD_BASE}{path}"
    headers = {"X-Auth-Token": fd_key}
    resp    = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 429:
        print("  ⚠  Rate limit hit, sleeping 65s…")
        time.sleep(65)
        resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 401:
        sys.exit("❌  Invalid FOOTBALL_DATA_API_KEY")
    resp.raise_for_status()
    return resp.json()


def team_matches(fd_name: str, our_name: str) -> bool:
    """Fuzzy match football-data.org team names to our stored English names."""
    def norm(s):
        return (s.lower()
                  .replace("& hove albion", "")
                  .replace("fc ", "")
                  .replace(" fc", "")
                  .strip())
    a, b = norm(fd_name), norm(our_name)
    if b in a or a in b:
        return True
    a_words = {w for w in a.split() if len(w) > 3}
    b_words = {w for w in b.split() if len(w) > 3}
    return bool(a_words & b_words)


def determine_result(match: dict, home_score: int, away_score: int) -> str | None:
    """Return 'W', 'L', or None if the market/selection cannot be resolved."""
    market    = match["pick"].get("market", "h2h")
    selection = match["pick"].get("selection", "home")
    h, a      = home_score, away_score

    if market == "h2h":
        if selection == "home": return "W" if h > a else "L"
        if selection == "away": return "W" if a > h else "L"
        if selection == "draw": return "W" if h == a else "L"
    elif market == "btts":
        return "W" if (h > 0 and a > 0) else "L"
    elif market == "over_under":
        # Extract line from selection string, e.g. "over_2.5" → 2.5
        parts = selection.split("_")
        try:
            line = float(parts[-1])
        except (ValueError, IndexError):
            line = float(match["pick"].get("line", 2.5))
        total = h + a
        if selection.startswith("over"):  return "W" if total > line else "L"
        if selection.startswith("under"): return "W" if total < line else "L"
    return None


def main():
    fd_key = os.environ.get("FOOTBALL_DATA_API_KEY", "")
    if not fd_key:
        sys.exit("❌  Set FOOTBALL_DATA_API_KEY env var (free at football-data.org)")

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found.")

    with open(DATA_FILE, encoding="utf-8") as f:
        md = json.load(f)

    upcoming = md.get("upcoming", [])
    results  = md.get("results", [])
    record   = md.get("record", {"correct": 0, "total": 0, "period": ""})

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Only EPL pending matches whose date is in the past
    # UEFA stubs carry a top-level "competition" field; EPL matches do not
    pending_past = [
        m for m in upcoming
        if m.get("status") == "pending"
        and m.get("date", "9999-99-99") < today_str
        and not m.get("competition")
    ]

    if not pending_past:
        print("No pending past EPL matches — nothing to check.")
        return

    date_from = min(m["date"] for m in pending_past)
    date_to   = max(m["date"] for m in pending_past)

    print(f"\n🔍  Checking {len(pending_past)} pending past EPL matches "
          f"({date_from} – {date_to})…\n")

    path    = (f"/competitions/{COMPETITION}/matches"
               f"?status=FINISHED&dateFrom={date_from}&dateTo={date_to}")
    payload = fd_get(path, fd_key)
    fd_matches = payload.get("matches", [])
    print(f"  ✓  football-data.org returned {len(fd_matches)} finished PL matches\n")

    moved              = 0
    remaining_upcoming = []

    for match in upcoming:
        # Keep anything that isn't a past pending EPL match
        if (match.get("status") != "pending"
                or match.get("date", "9999-99-99") >= today_str
                or match.get("competition")):
            remaining_upcoming.append(match)
            continue

        # Try to find the corresponding finished match
        matched_fd = None
        for fd_m in fd_matches:
            fd_home = fd_m.get("homeTeam", {}).get("name", "")
            fd_away = fd_m.get("awayTeam", {}).get("name", "")
            if team_matches(fd_home, match["homeEn"]) and team_matches(fd_away, match["awayEn"]):
                matched_fd = fd_m
                break

        if not matched_fd:
            remaining_upcoming.append(match)
            continue

        ft = matched_fd.get("score", {}).get("fullTime", {})
        home_score = ft.get("home")
        away_score = ft.get("away")

        if home_score is None or away_score is None:
            remaining_upcoming.append(match)
            continue

        result = determine_result(match, int(home_score), int(away_score))
        if result is None:
            remaining_upcoming.append(match)
            continue

        score_str    = f"{home_score}-{away_score}"
        result_entry = {
            "id":            match["id"],
            "home":          match["home"],
            "homeEn":        match["homeEn"],
            "away":          match["away"],
            "awayEn":        match["awayEn"],
            "date":          match["date"],
            "competition":   md.get("label", ""),
            "competitionEn": md.get("labelEn", ""),
            "pick":          match["pick"],
            "result":        result,
            "score":         score_str,
        }

        results.insert(0, result_entry)
        record["correct"] = record.get("correct", 0) + (1 if result == "W" else 0)
        record["total"]   = record.get("total", 0) + 1

        icon = "✓" if result == "W" else "✗"
        print(f"  {icon}  {match['homeEn']} vs {match['awayEn']} → {result} ({score_str})")
        moved += 1

    md["upcoming"] = remaining_upcoming
    md["results"]  = results[:30]
    md["record"]   = record

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(md, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Moved {moved} matches to results. "
          f"Record: {record['correct']}/{record['total']}\n")


if __name__ == "__main__":
    main()
