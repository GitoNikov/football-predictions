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

MATCHDAY_FILE = Path("data/matchday.json")


def load_upcoming() -> list:
    """Load pending upcoming matches from matchday.json."""
    if not MATCHDAY_FILE.exists():
        return []
    with open(MATCHDAY_FILE, encoding="utf-8") as f:
        md = json.load(f)
    return [m for m in md.get("upcoming", []) if m.get("status") == "pending"]


def teams_match(api_name: str, json_name: str) -> bool:
    """
    True if the API team name and the matchday.json team name refer to the same club.
    Uses substring check first, then significant-word overlap as fallback
    (handles abbreviations like 'Man City' ↔ 'Manchester City').
    """
    a, j = api_name.lower(), json_name.lower()
    if j in a or a in j:
        return True
    a_words = {w for w in a.split() if len(w) > 3}
    j_words = {w for w in j.split() if len(w) > 3}
    return bool(a_words & j_words)


def find_match_id(home_api: str, away_api: str, upcoming: list) -> str | None:
    """
    Find the matchday.json match ID for an API event by fuzzy-matching
    both the home and away team names simultaneously.
    Returns the full match ID (e.g. 'liv_whu') or None.
    """
    for match in upcoming:
        if teams_match(home_api, match["homeEn"]) and teams_match(away_api, match["awayEn"]):
            return match["id"]
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
    """Return {_oNN} from a totals market at the requested line."""
    key = "_o" + str(point).replace(".", "")   # 2.5→"_o25", 1.5→"_o15"
    for outcome in market.get("outcomes", []):
        if outcome.get("point") == point and outcome["name"] == "Over":
            return {key: str(round(outcome["price"], 2))}
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


def build_odds_dict(events: list, sport: str, upcoming: list) -> tuple[dict, dict]:
    """
    Convert a list of API events into our flat key→value odds dict.
    prefix = full matchday.json match ID (e.g. 'liv_whu') so patching works directly.
    Also returns event_map: {match_id → (sport, event_id)} for BTTS fetching.
    """
    odds: dict[str, str] = {}
    event_map: dict[str, tuple[str, str]] = {}

    for event in events:
        if not is_today_or_weekend(event.get("commence_time", "")):
            continue

        home   = event["home_team"]
        away   = event["away_team"]
        prefix = find_match_id(home, away, upcoming)

        if not prefix:
            print(f"  ⚠  No matchday.json entry for: {home} vs {away}")
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
                odds[f"{prefix}{k}"] = v   # e.g. liv_whu_1

        if "totals" in markets:
            for k, v in extract_totals(markets["totals"], 2.5).items():
                odds[f"{prefix}{k}"] = v
            for k, v in extract_totals(markets["totals"], 1.5).items():
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
              f"O1.5={odds.get(prefix+'_o15','?')}  "
              f"BTTS={odds.get(prefix+'_btts','?')}")

    return odds, event_map


def fetch_btts_for_events(api_key: str, event_map: dict) -> dict:
    """
    Fetch BTTS market for all matched events via the per-event endpoint.
    Costs 3 quota units per event.
    Returns partial odds dict with only _btts keys.
    """
    btts_odds: dict[str, str] = {}
    to_fetch = [(prefix, sport, eid)
                for prefix, (sport, eid) in event_map.items()]

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

    # Load matchday.json once — used to map API team names → match IDs
    upcoming = load_upcoming()
    print(f"  📋 Loaded {len(upcoming)} pending matches from matchday.json\n")

    all_odds:   dict[str, str] = {}
    all_events: dict[str, tuple[str, str]] = {}  # match_id → (sport, event_id)

    for sport in SPORTS:
        events = fetch_sport_odds(api_key, sport)
        odds, event_map = build_odds_dict(events, sport, upcoming)
        all_odds.update(odds)
        all_events.update(event_map)

    if not all_odds:
        print("\n⚠  No odds found. Check the date/window or API key.")
        sys.exit(1)

    # Fetch BTTS for all matched events
    btts = fetch_btts_for_events(api_key, all_events)
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

    # Patch odds_wh into matchday.json upcoming matches
    if MATCHDAY_FILE.exists():
        with open(MATCHDAY_FILE, encoding="utf-8") as f:
            md = json.load(f)
        updated_md = 0
        for match in md.get("upcoming", []):
            mid = match["id"]
            wh = {}
            if f"{mid}_1"    in all_odds: wh["h"]    = all_odds[f"{mid}_1"]
            if f"{mid}_x"    in all_odds: wh["x"]    = all_odds[f"{mid}_x"]
            if f"{mid}_2"    in all_odds: wh["a"]    = all_odds[f"{mid}_2"]
            if f"{mid}_o25"  in all_odds: wh["o25"]  = all_odds[f"{mid}_o25"]
            if f"{mid}_o15"  in all_odds: wh["o15"]  = all_odds[f"{mid}_o15"]
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
        # Refresh betBuilder market odds + recompute totalOdd
        bb = md.get("betBuilder", {})
        bb_mid = bb.get("matchId", "")
        if bb_mid and bb.get("markets"):
            bb_match = next((m for m in md.get("upcoming", []) if m["id"] == bb_mid), None)
            if bb_match:
                wh = bb_match.get("odds_wh", {})
                mkt_key = {
                    "home win": "h",          "победа домакин": "h",
                    "away win": "a",          "победа гост": "a",
                    "draw": "x",              "равенство": "x",
                    "btts yes": "btts",       "и двата вкарват": "btts",
                    "over 1.5 goals": "o15",  "над 1.5 гола": "o15",
                    "over 2.5 goals": "o25",  "над 2.5 гола": "o25",
                }
                changed = False
                for mkt in bb["markets"]:
                    key = mkt_key.get(mkt.get("marketEn", "").lower()) \
                       or mkt_key.get(mkt.get("market", "").lower())
                    if key and key in wh:
                        mkt["odd"] = wh[key]
                        changed = True
                if changed:
                    product = 1.0
                    for mkt in bb["markets"]:
                        product *= float(mkt["odd"])
                    bb["totalOdd"] = str(round(product, 2))
                    print(f"    ↻  betBuilder totalOdd refreshed → {bb['totalOdd']}")

        with open(MATCHDAY_FILE, "w", encoding="utf-8") as f:
            json.dump(md, f, ensure_ascii=False, indent=2)
        print(f"    Patched odds into matchday.json for {updated_md} matches\n")


if __name__ == "__main__":
    main()
