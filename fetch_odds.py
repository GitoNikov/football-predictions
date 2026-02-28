"""
fetch_odds.py — pull real William Hill odds for today's UEFA EL/CL + EPL matches
and write data/live_odds.json for the football predictions site.

Usage:
    export ODDS_API_KEY=your_key_here
    python fetch_odds.py

Or pass the key directly:
    python fetch_odds.py --key YOUR_KEY

Quota estimate per run:
  Bulk (h2h+totals): 4 sports × ~2 units = ~8
  Event BTTS:        len(BTTS_PREFIXES) × 3 units = ~24
  Total: ~32 units   → 500/month allows 15 full runs
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
SPORTS      = [
    "soccer_epl",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
]
REGIONS     = "eu"
BOOKMAKER   = "williamhill"
MARKETS     = "h2h,totals"
ODDS_FORMAT = "decimal"
OUTPUT_FILE = Path("data/live_odds.json")

# Prefixes for which to fetch BTTS via the per-event endpoint.
# Costs 3 quota units per event — only list key picks/combos.
BTTS_PREFIXES = {"stu", "bol", "for", "cel", "liv", "ars", "bri", "gen"}

# ── Team-name → match-ID mapping ───────────────────────────────────────────
# Keys are lowercase substrings that appear in the API team names.
# The value is our short match prefix (e.g. "stu" → stu_1, stu_x, stu_2 …)
# Both home AND away for a match map to the same prefix so that either
# team name resolves correctly. Home is tried first via get_prefix(home).
TEAM_TO_PREFIX = {
    # ── EPL GW28 (27 Feb – 1 Mar 2026) ──────────────────────────────────
    "wolverhampton":      "wol",
    "wolves":             "wol",
    "aston villa":        "wol",

    "bournemouth":        "bou",
    "sunderland":         "bou",

    "burnley":            "bur",
    "brentford":          "bur",

    "newcastle":          "new",
    "everton":            "new",

    "liverpool":          "liv",
    "west ham":           "liv",

    "leeds":              "lee",
    "manchester city":    "lee",

    "brighton":           "bri",
    # Nottm Forest away at Brighton — "brighton" is home, found first.
    # "nottm forest" / "nottingham forest" → bri only needed as fallback:
    "nottm forest":       "bri",

    "manchester united":  "mun",
    "crystal palace":     "mun",

    "fulham":             "ful",
    "tottenham":          "ful",

    "arsenal":            "ars",
    "chelsea":            "ars",

    # ── EL 26.02.2026 ────────────────────────────────────────────────────
    "stuttgart":          "stu",
    "celtic":             "stu",

    "bologna":            "bol",
    "brann":              "bol",

    # NOTE: "nottingham" key kept for Nottm Forest as EL *home* team (for).
    # Brighton vs Nottm Forest (EPL) uses "brighton" → "bri" first.
    "nottingham":         "for",
    "fenerbahce":         "for",

    "red star":           "red",
    "crvena zvezda":      "red",
    "lille":              "red",

    "ferencvaros":        "fer",
    "ferencvárosi":       "fer",
    "ludogorets":         "fer",

    "genk":               "gen",
    "dinamo zagreb":      "gen",

    "plzen":              "plz",
    "viktoria plzen":     "plz",
    "panathinaikos":      "plz",

    "celta":              "cel",
    "paok":               "cel",

    # ── CL 25.02.2026 ────────────────────────────────────────────────────
    "atalanta":           "atl",
    "dortmund":           "atl",
    "borussia dortmund":  "atl",

    "juventus":           "juv",
    "galatasaray":        "juv",

    "paris saint":        "psg",
    "psg":                "psg",
    "monaco":             "psg",

    "real madrid":        "rma",
    "benfica":            "rma",
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
    """Fetch all upcoming events for a sport from William Hill (bulk)."""
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
    if resp.status_code in (404, 422):
        print(f"  ⚠  Sport not found or not available: {sport}")
        return []
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    used      = resp.headers.get("x-requests-used", "?")
    print(f"  ✓  {sport}: {len(resp.json())} events  |  quota used {used}, remaining {remaining}")
    return resp.json()


def fetch_event_odds(api_key: str, sport: str, event_id: str, markets: str = "btts") -> dict:
    """Fetch odds for a single event (used for BTTS market)."""
    url = f"{BASE_URL}/sports/{sport}/events/{event_id}/odds"
    params = {
        "apiKey":      api_key,
        "regions":     REGIONS,
        "markets":     markets,
        "oddsFormat":  ODDS_FORMAT,
        "bookmakers":  BOOKMAKER,
    }
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code in (404, 422):
        return {}
    resp.raise_for_status()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"      event {event_id[:8]}… quota remaining {remaining}")
    return resp.json()


def is_today_or_weekend(commence_time_str: str) -> bool:
    """Include events starting within the next 5 days (covers full EPL weekend)."""
    try:
        dt  = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        window_end = now + timedelta(days=5)
        return now - timedelta(hours=3) <= dt <= window_end
    except Exception:
        return True  # include if we can't parse


def build_odds_dict(events: list, sport: str) -> tuple[dict, dict]:
    """
    Convert a list of API events into our flat key→value odds dict.
    Also returns event_map: {prefix → (sport, event_id)} for BTTS fetching.
    """
    odds: dict[str, str] = {}
    event_map: dict[str, tuple[str, str]] = {}

    for event in events:
        if not is_today_or_weekend(event.get("commence_time", "")):
            continue

        home   = event["home_team"]
        away   = event["away_team"]
        prefix = get_prefix(home) or get_prefix(away)

        if not prefix:
            print(f"  ⚠  No mapping for: {home} vs {away}")
            continue

        # Find William Hill bookmaker entry
        wh = next(
            (bk for bk in event.get("bookmakers", []) if bk["key"] == BOOKMAKER),
            None
        )
        if not wh:
            print(f"  ⚠  William Hill not available for: {home} vs {away}")
            continue

        markets = {m["key"]: m for m in wh.get("markets", [])}

        if "h2h" in markets:
            for k, v in extract_h2h(markets["h2h"], home).items():
                odds[f"{prefix}{k}"] = v

        if "totals" in markets:
            for k, v in extract_totals(markets["totals"]).items():
                odds[f"{prefix}{k}"] = v

        if "btts" in markets:
            for k, v in extract_btts(markets["btts"]).items():
                odds[f"{prefix}{k}"] = v

        event_map[prefix] = (sport, event["id"])

        print(f"  ✓  {home} vs {away}  [{prefix}]  "
              f"1={odds.get(prefix+'_1','?')}  "
              f"X={odds.get(prefix+'_x','?')}  "
              f"2={odds.get(prefix+'_2','?')}  "
              f"O2.5={odds.get(prefix+'_o25','?')}  "
              f"BTTS={odds.get(prefix+'_btts','?')}")

    return odds, event_map


def fetch_btts_for_events(api_key: str, event_map: dict, target_prefixes: set) -> dict:
    """
    For each prefix in target_prefixes that was matched in the bulk fetch,
    call the per-event endpoint to get the BTTS market.
    Returns partial odds dict with only _btts keys.
    """
    btts_odds: dict[str, str] = {}
    to_fetch = [(prefix, sport, eid)
                for prefix, (sport, eid) in event_map.items()
                if prefix in target_prefixes]

    if not to_fetch:
        return btts_odds

    print(f"\n  🎯 Fetching BTTS for {len(to_fetch)} events via per-event endpoint…")
    for prefix, sport, event_id in to_fetch:
        data = fetch_event_odds(api_key, sport, event_id, markets="btts")
        if not data:
            continue
        wh = next(
            (bk for bk in data.get("bookmakers", []) if bk["key"] == BOOKMAKER),
            None
        )
        if not wh:
            continue
        for market in wh.get("markets", []):
            if market["key"] == "btts":
                result = extract_btts(market)
                if result:
                    btts_odds[f"{prefix}_btts"] = result["_btts"]
                    print(f"  ✓  BTTS [{prefix}] = {result['_btts']}")
    return btts_odds


def main():
    parser = argparse.ArgumentParser(
        description="Fetch William Hill odds for today's EPL + UEFA matches"
    )
    parser.add_argument("--key", help="API key (overrides ODDS_API_KEY env var)")
    args = parser.parse_args()

    api_key = args.key or os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        sys.exit("❌  No API key found.\n"
                 "    Set ODDS_API_KEY env var or pass --key YOUR_KEY\n"
                 "    Get a free key at https://the-odds-api.com")

    print(f"\n📡 Fetching William Hill odds for EPL + UEFA matches …\n")

    all_odds:   dict[str, str] = {}
    all_events: dict[str, tuple[str, str]] = {}  # prefix → (sport, event_id)

    for sport in SPORTS:
        events = fetch_sport_odds(api_key, sport)
        odds, event_map = build_odds_dict(events, sport)
        all_odds.update(odds)
        all_events.update(event_map)  # later sport overwrites — fine, prefixes are unique

    if not all_odds:
        print("\n⚠  No odds found. Check the date/window or API key.")
        sys.exit(1)

    # Step 2: fetch BTTS for key events via per-event endpoint
    btts = fetch_btts_for_events(api_key, all_events, BTTS_PREFIXES)
    all_odds.update(btts)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bookmaker":  "William Hill",
        "odds":       all_odds,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\n✅  Saved {len(all_odds)} odds entries → {OUTPUT_FILE}")

    # Also patch odds_wh into matchday.json upcoming matches
    matchday_file = Path("data/matchday.json")
    if matchday_file.exists():
        with open(matchday_file, encoding="utf-8") as f:
            md = json.load(f)
        updated_md = 0
        for match in md.get("upcoming", []):
            mid = match["id"]
            wh = {}
            if f"{mid}_1"    in all_odds: wh["h"]    = all_odds[f"{mid}_1"]
            if f"{mid}_x"    in all_odds: wh["x"]    = all_odds[f"{mid}_x"]
            if f"{mid}_2"    in all_odds: wh["a"]    = all_odds[f"{mid}_2"]
            if f"{mid}_o25"  in all_odds: wh["o25"]  = all_odds[f"{mid}_o25"]
            if f"{mid}_btts" in all_odds: wh["btts"] = all_odds[f"{mid}_btts"]
            if wh:
                match["odds_wh"] = {**match.get("odds_wh", {}), **wh}
                # Update pick odd
                pick = match.get("pick", {})
                market = pick.get("market", "h2h")
                sel    = pick.get("selection", "home")
                if market == "h2h":
                    key = {"home": "h", "away": "a", "draw": "x"}.get(sel)
                    if key and key in wh:
                        pick["odd"] = wh[key]
                elif market == "btts" and "btts" in wh:
                    pick["odd"] = wh["btts"]
                elif market == "over_under" and "o25" in wh:
                    pick["odd"] = wh["o25"]
                updated_md += 1
        with open(matchday_file, "w", encoding="utf-8") as f:
            json.dump(md, f, ensure_ascii=False, indent=2)
        print(f"    Patched odds into matchday.json for {updated_md} matches\n")


if __name__ == "__main__":
    main()
