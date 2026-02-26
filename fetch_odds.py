"""
fetch_odds.py — pull real Bet365 odds for today's UEFA EL/CL matches
and write data/live_odds.json for the football predictions site.

Usage:
    export ODDS_API_KEY=your_key_here
    python fetch_odds.py

Or pass the key directly:
    python fetch_odds.py --key YOUR_KEY
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed. Run: pip install requests")

# ── Config ─────────────────────────────────────────────────────────────────
BASE_URL    = "https://api.the-odds-api.com/v4"
SPORTS      = ["soccer_uefa_champs_league", "soccer_europa_league"]
REGIONS     = "eu"
BOOKMAKER   = "bet365"
MARKETS     = "h2h,totals,btts"
ODDS_FORMAT = "decimal"
OUTPUT_FILE = Path("data/live_odds.json")

# ── Team-name → match-ID mapping ───────────────────────────────────────────
# Keys are lowercase substrings that appear in the API team names.
# The value is our short match prefix (e.g. "stu" → stu_1, stu_x, stu_2 …)
# Add / edit entries here whenever the fixture list changes.
TEAM_TO_PREFIX = {
    # EL 26.02.2026
    "stuttgart":       "stu",
    "celtic":          "stu",
    "bologna":         "bol",
    "brann":           "bol",
    "nottingham":      "for",
    "fenerbahce":      "for",
    "red star":        "red",
    "crvena zvezda":   "red",
    "lille":           "red",
    "ferencvaros":     "fer",
    "ferencvárosi":    "fer",
    "ludogorets":      "fer",
    "genk":            "gen",
    "dinamo zagreb":   "gen",
    "plzen":           "plz",
    "viktoria plzen":  "plz",
    "panathinaikos":   "plz",
    "celta":           "cel",
    "paok":            "cel",
    # CL 25.02.2026
    "atalanta":        "atl",
    "dortmund":        "atl",
    "borussia dortmund": "atl",
    "juventus":        "juv",
    "galatasaray":     "juv",
    "paris saint":     "psg",
    "psg":             "psg",
    "monaco":          "psg",
    "real madrid":     "rma",
    "benfica":         "rma",
}


def get_prefix(team_name: str) -> str | None:
    """Match a team name (from API) to our short prefix."""
    name = team_name.lower()
    for key, prefix in TEAM_TO_PREFIX.items():
        if key in name:
            return prefix
    return None


def extract_h2h(market: dict, home_team: str) -> dict:
    """Return {_1, _x, _2} odds from a h2h market dict."""
    out = {}
    for outcome in market.get("outcomes", []):
        n, p = outcome["name"], str(round(outcome["price"], 2))
        if n == "Draw":
            out["_x"] = p
        elif n == home_team:
            out["_1"] = p
        else:
            out["_2"] = p
    return out


def extract_totals(market: dict, point: float = 2.5) -> dict:
    """Return {_o25} from a totals market at the requested line."""
    for outcome in market.get("outcomes", []):
        if outcome.get("point") == point and outcome["name"] == "Over":
            return {"_o25": str(round(outcome["price"], 2))}
    return {}


def extract_btts(market: dict) -> dict:
    """Return {_btts} Yes odds from a btts market."""
    for outcome in market.get("outcomes", []):
        if outcome["name"].lower() in ("yes", "btts yes"):
            return {"_btts": str(round(outcome["price"], 2))}
    return {}


def fetch_sport_odds(api_key: str, sport: str) -> list:
    """Fetch all upcoming events for a sport from Bet365."""
    url = f"{BASE_URL}/sports/{sport}/odds/"
    params = {
        "apiKey":      api_key,
        "regions":     REGIONS,
        "markets":     MARKETS,
        "oddsFormat":  ODDS_FORMAT,
        "bookmakers":  BOOKMAKER,
    }
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code == 401:
        sys.exit("❌  Invalid API key — check your ODDS_API_KEY")
    if resp.status_code == 422:
        print(f"  ⚠  Sport not found or not available: {sport}")
        return []
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    used      = resp.headers.get("x-requests-used", "?")
    print(f"  ✓  {sport}: {len(resp.json())} events  |  quota used {used}, remaining {remaining}")
    return resp.json()


def is_today(commence_time_str: str) -> bool:
    """Check whether an event starts today (UTC)."""
    try:
        dt = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end   = today_start + timedelta(days=1)
        return today_start <= dt < today_end
    except Exception:
        return True  # include if we can't parse


def build_odds_dict(events: list) -> dict:
    """Convert a list of API events into our flat key→value odds dict."""
    odds: dict[str, str] = {}

    for event in events:
        if not is_today(event.get("commence_time", "")):
            continue

        home = event["home_team"]
        away = event["away_team"]
        prefix = get_prefix(home) or get_prefix(away)

        if not prefix:
            print(f"  ⚠  No mapping for: {home} vs {away}")
            continue

        # Find Bet365 bookmaker entry
        bet365 = next(
            (bk for bk in event.get("bookmakers", []) if bk["key"] == BOOKMAKER),
            None
        )
        if not bet365:
            print(f"  ⚠  Bet365 not available for: {home} vs {away}")
            continue

        markets = {m["key"]: m for m in bet365.get("markets", [])}

        if "h2h" in markets:
            for k, v in extract_h2h(markets["h2h"], home).items():
                odds[f"{prefix}{k}"] = v

        if "totals" in markets:
            for k, v in extract_totals(markets["totals"]).items():
                odds[f"{prefix}{k}"] = v

        if "btts" in markets:
            for k, v in extract_btts(markets["btts"]).items():
                odds[f"{prefix}{k}"] = v

        print(f"  ✓  {home} vs {away}  [{prefix}]  "
              f"1={odds.get(prefix+'_1','?')}  "
              f"X={odds.get(prefix+'_x','?')}  "
              f"2={odds.get(prefix+'_2','?')}  "
              f"O2.5={odds.get(prefix+'_o25','?')}  "
              f"BTTS={odds.get(prefix+'_btts','?')}")

    return odds


def main():
    parser = argparse.ArgumentParser(description="Fetch Bet365 odds for today's UEFA matches")
    parser.add_argument("--key", help="API key (overrides ODDS_API_KEY env var)")
    args = parser.parse_args()

    api_key = args.key or os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        sys.exit("❌  No API key found.\n"
                 "    Set ODDS_API_KEY env var or pass --key YOUR_KEY\n"
                 "    Get a free key at https://the-odds-api.com")

    print(f"\n📡 Fetching Bet365 odds for today's UEFA matches …\n")

    all_odds: dict[str, str] = {}
    for sport in SPORTS:
        events = fetch_sport_odds(api_key, sport)
        all_odds.update(build_odds_dict(events))

    if not all_odds:
        print("\n⚠  No odds found for today. Check the date or API key.")
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bookmaker":  "Bet365",
        "odds":       all_odds,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\n✅  Saved {len(all_odds)} odds entries → {OUTPUT_FILE}")
    print(f"    Keys: {', '.join(sorted(all_odds.keys()))}\n")


if __name__ == "__main__":
    main()
