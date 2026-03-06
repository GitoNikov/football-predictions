"""
fetch_odds.py — hybrid odds fetcher:
  • EPL        → the-odds-api.com  (free 500/month; EPL paywalled on odds-api.io)
  • All others → odds-api.io       (free 100 req/hour; covers UEFA + La Liga etc.)

Usage:
    export ODDS_API_KEY=your_key_here    # the-odds-api.com (EPL)   https://the-odds-api.com
    export OAIO_API_KEY=your_key_here    # odds-api.io (UEFA+)      https://odds-api.io
    python fetch_odds.py

    python fetch_odds.py --list-leagues  # list odds-api.io league slugs

Quota per run:
  odds-api.io    : ~2 req/league × 3 UEFA leagues  = ~6 req  (free: 100/hour)
  the-odds-api.com: 1 bulk call for EPL             = ~2 units (free: 500/month)

odds-api.io response format differs from The Odds API v4:
  - bookmakers is a dict  {"WilliamHill": [...markets]}  not an array
  - team names: event["home"] / event["away"]
  - odds is a LIST per market: [{"home": "2.1", "draw": "3.4", "away": "1.8"}]
  - values can be "N/A" strings → treated as missing
  - event IDs are integers
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed. Run: pip install requests")

# ── Config ──────────────────────────────────────────────────────────────────

# odds-api.io (primary — UEFA + expansion leagues)
OAIO_BASE     = "https://api.odds-api.io/v3"
OAIO_BOOKMAKER = "WilliamHill"   # exact key as returned by /v3/bookmakers

# the-odds-api.com (EPL fallback — paywalled on odds-api.io)
TODA_BASE      = "https://api.the-odds-api.com/v4"
TODA_BOOKMAKER = "williamhill"
TODA_REGIONS   = "eu"
TODA_MARKETS   = "h2h,totals"   # no per-event BTTS — saves quota

# EPL sport key handled via the-odds-api.com
EPL_SPORT = "soccer_epl"

# odds-api.io leagues (verified slugs, 2026-03-06)
LEAGUES: dict[str, str] = {
    "soccer_uefa_champs_league":                "international-clubs-uefa-champions-league",
    "soccer_uefa_europa_league":                "international-clubs-uefa-europa-league",
    "soccer_uefa_europa_conference_league":     "international-clubs-uefa-conference-league",
    # Uncomment to expand (all verified free on odds-api.io):
    # "soccer_spain_la_liga":                   "spain-laliga",
    # "soccer_germany_bundesliga":              "germany-bundesliga",
    # "soccer_france_ligue_1":                  "france-ligue-1",
    # "soccer_netherlands_eredivisie":          "netherlands-eredivisie",
    # "soccer_portugal_primeira_liga":          "portugal-liga-portugal",
    # "soccer_scotland_premier_league":         "scotland-premiership",
    # "soccer_turkey_super_league":             "turkiye-super-lig",
}

OUTPUT_FILE   = Path("data/live_odds.json")
MATCHDAY_FILE = Path("data/matchday.json")


# ── Response parsing helpers ─────────────────────────────────────────────────

def _safe_price(val) -> str | None:
    """Return numeric price string, or None for missing/N/A values."""
    if val is None or val == "N/A":
        return None
    try:
        f = float(val)
        return str(round(f, 2)) if f > 1.0 else None
    except (ValueError, TypeError):
        return None


def extract_from_event(event: dict) -> dict:
    """
    Extract h2h / o25 / o15 / btts from an odds-api.io event (with odds embedded).

    Actual odds-api.io bookmakers structure (verified 2026-03-06):
        event["bookmakers"] = {
            "WilliamHill": [
                {"name": "ML",      "odds": [{"home": "4.20", "draw": "N/A", "away": "1.80"}]},
                {"name": "Totals",  "odds": [{"hdp": 2.5, "over": "1.90", "under": "1.90"}, ...]},
                {"name": "Both Teams To Score", "odds": [{"hdp": 0, "home": "1.80", "away": "1.95"}]},
            ]
        }
    Notes:
      - odds is a LIST, not a dict
      - values may be "N/A" strings → treat as missing
      - BTTS yes=home key, no=away key
      - Totals: find entry where hdp == target line (2.5 or 1.5)

    Returns flat dict: {"_1": "2.1", "_x": "3.4", "_2": "3.5", "_o25": "1.9", ...}
    """
    bookmakers = event.get("bookmakers", {})
    if not isinstance(bookmakers, dict):
        return {}

    wh_markets = bookmakers.get(OAIO_BOOKMAKER)
    if not wh_markets:
        return {}

    out: dict[str, str] = {}
    for market in wh_markets:
        name      = market.get("name", "")
        odds_list = market.get("odds", [])
        if not odds_list:
            continue
        o = odds_list[0]   # first entry is the main line

        if name == "ML":
            h = _safe_price(o.get("home"))
            x = _safe_price(o.get("draw"))
            a = _safe_price(o.get("away"))
            if h: out["_1"] = h
            if x: out["_x"] = x
            if a: out["_2"] = a

        elif name == "Totals":
            for entry in odds_list:
                hdp = entry.get("hdp")
                if hdp == 2.5:
                    v = _safe_price(entry.get("over"))
                    if v: out["_o25"] = v
                elif hdp == 1.5:
                    v = _safe_price(entry.get("over"))
                    if v: out["_o15"] = v

        elif name == "Both Teams To Score":
            # WilliamHill uses home=Yes, away=No keys for BTTS
            v = _safe_price(o.get("yes") or o.get("home"))
            if v: out["_btts"] = v

    return out


# ── API helpers ──────────────────────────────────────────────────────────────

def api_get(base: str, api_key: str, path: str, params: dict | None = None) -> list | dict | None:
    """GET with auth, retry on 429, and structured error handling."""
    url  = f"{base}/{path.lstrip('/')}"
    p    = {"apiKey": api_key, **(params or {})}
    resp = requests.get(url, params=p, timeout=15)
    if resp.status_code == 401:
        print(f"  ❌  Invalid API key for {base}")
        return None
    if resp.status_code == 429:
        print("  ⚠  429 rate limit — sleeping 10s and retrying…")
        time.sleep(10)
        resp = requests.get(url, params=p, timeout=15)
    if resp.status_code in (404, 422):
        return None
    resp.raise_for_status()
    return resp.json()


# ── EPL via the-odds-api.com ──────────────────────────────────────────────────

def _toda_extract_h2h(market: dict, home_team: str) -> dict:
    """Parse h2h outcomes from the-odds-api.com format."""
    out = {}
    for outcome in market.get("outcomes", []):
        n, p = outcome["name"], str(round(outcome["price"], 2))
        if n == "Draw":        out["_x"] = p
        elif n == home_team:   out["_1"] = p
        else:                  out["_2"] = p
    return out


def _toda_extract_totals(market: dict, point: float) -> dict:
    """Parse totals at a specific line from the-odds-api.com format."""
    key = "_o" + str(point).replace(".", "")
    for outcome in market.get("outcomes", []):
        if outcome.get("point") == point and outcome["name"] == "Over":
            return {key: str(round(outcome["price"], 2))}
    return {}


def fetch_epl_odds(epl_key: str, upcoming_md: list) -> dict:
    """
    Fetch EPL odds from the-odds-api.com (bulk h2h + totals, no per-event BTTS).
    ~2 quota units per run. Free tier: 500/month.
    """
    data = api_get(TODA_BASE, epl_key, f"/sports/{EPL_SPORT}/odds/", {
        "regions":    TODA_REGIONS,
        "markets":    TODA_MARKETS,
        "oddsFormat": "decimal",
        "bookmakers": TODA_BOOKMAKER,
    })
    if not data:
        print(f"  ⚠  No EPL data from the-odds-api.com (check THE_ODDS_API_KEY)")
        return {}

    remaining = "?"
    all_odds: dict[str, str] = {}

    for event in data:
        if not is_upcoming(event.get("commence_time", "")):
            continue

        home   = event["home_team"]
        away   = event["away_team"]
        prefix = find_match_id(home, away, upcoming_md)
        if not prefix:
            print(f"  ⚠  No matchday.json entry for: {home} vs {away}")
            continue

        wh = next((bk for bk in event.get("bookmakers", []) if bk["key"] == TODA_BOOKMAKER), None)
        if not wh:
            continue

        markets = {m["key"]: m for m in wh.get("markets", [])}
        if "h2h" in markets:
            all_odds.update({f"{prefix}{k}": v for k, v in _toda_extract_h2h(markets["h2h"], home).items()})
        if "totals" in markets:
            all_odds.update({f"{prefix}{k}": v for k, v in _toda_extract_totals(markets["totals"], 2.5).items()})
            all_odds.update({f"{prefix}{k}": v for k, v in _toda_extract_totals(markets["totals"], 1.5).items()})

        print(f"  ✓  {home} vs {away}  [{prefix}]  "
              f"1={all_odds.get(prefix+'_1','?')}  "
              f"X={all_odds.get(prefix+'_x','?')}  "
              f"2={all_odds.get(prefix+'_2','?')}  "
              f"O2.5={all_odds.get(prefix+'_o25','?')}  "
              f"O1.5={all_odds.get(prefix+'_o15','?')}")

    return all_odds


def _unwrap(data) -> list:
    """Handle both bare-list and {data: [...]} response wrappers."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", data.get("results", data.get("events", [])))
    return []


def is_upcoming(dt_str: str) -> bool:
    """True if event falls within a 12-day window from now (covers full weekly setup cycle)."""
    try:
        dt  = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return now - timedelta(hours=3) <= dt <= now + timedelta(days=12)
    except Exception:
        return True   # include if unparseable


# ── Core fetch logic ─────────────────────────────────────────────────────────

def fetch_league_events(api_key: str, league_slug: str) -> list:
    """
    Step 1: Get upcoming event list (team names + IDs) for one league.
    1 API call. Returns only events within the 12-day window.
    """
    data = api_get(OAIO_BASE, api_key, "/events", {"sport": "football", "league": league_slug})
    if data is None:
        print(f"  ⚠  League not found: '{league_slug}' — run --list-leagues to check slugs")
        return []
    events  = _unwrap(data)
    window  = [e for e in events if is_upcoming(e.get("date", e.get("commence_time", "")))]
    print(f"  📋 {league_slug}: {len(window)} upcoming events")
    return window


def fetch_odds_for_events(api_key: str, event_ids: list) -> list:
    """
    Step 2: Fetch h2h + totals + BTTS for up to 10 events per call via /odds/multi.
    Returns list of event dicts with bookmakers embedded.
    """
    if not event_ids:
        return []
    ids_str = ",".join(str(i) for i in event_ids)
    data = api_get(OAIO_BASE, api_key, "/odds/multi", {
        "eventIds":   ids_str,
        "bookmakers": OAIO_BOOKMAKER,
    })
    if data is None:
        return []
    return _unwrap(data)


def process_league(api_key: str, league_slug: str, upcoming_md: list) -> dict:
    """
    Fetch events + odds for one league.
    Returns flat odds dict: {"arsliv_1": "2.1", "arsliv_x": "3.4", ...}
    API calls: 1 events + ceil(N/10) odds/multi  ≈ 2 total for typical league.
    """
    events = fetch_league_events(api_key, league_slug)
    if not events:
        return {}

    # Batch fetch odds (max 10 per call)
    event_ids    = [e["id"] for e in events if isinstance(e.get("id"), (int, str))]
    enriched_map = {}
    for i in range(0, len(event_ids), 10):
        batch    = event_ids[i:i+10]
        enriched = fetch_odds_for_events(api_key, batch)
        for e in enriched:
            enriched_map[e["id"]] = e
        if i + 10 < len(event_ids):
            time.sleep(0.5)

    all_odds: dict[str, str] = {}
    for event in events:
        home = event.get("home", event.get("home_team", ""))
        away = event.get("away", event.get("away_team", ""))
        prefix = find_match_id(home, away, upcoming_md)
        if not prefix:
            print(f"  ⚠  No matchday.json entry for: {home} vs {away}")
            continue

        # Prefer enriched (has odds), fall back to raw event
        merged    = enriched_map.get(event["id"], event)
        extracted = extract_from_event(merged)

        if not extracted:
            print(f"  ⚠  No William Hill odds for: {home} vs {away}")
            continue

        for k, v in extracted.items():
            all_odds[f"{prefix}{k}"] = v

        print(f"  ✓  {home} vs {away}  [{prefix}]  "
              f"1={all_odds.get(prefix+'_1','?')}  "
              f"X={all_odds.get(prefix+'_x','?')}  "
              f"2={all_odds.get(prefix+'_2','?')}  "
              f"O2.5={all_odds.get(prefix+'_o25','?')}  "
              f"O1.5={all_odds.get(prefix+'_o15','?')}  "
              f"BTTS={all_odds.get(prefix+'_btts','?')}")

    return all_odds


# ── Team matching (unchanged) ─────────────────────────────────────────────────

def teams_match(api_name: str, json_name: str) -> bool:
    a, j = api_name.lower(), json_name.lower()
    if j in a or a in j:
        return True
    a_words = {w for w in a.split() if len(w) > 3}
    j_words = {w for w in j.split() if len(w) > 3}
    return bool(a_words & j_words)


def find_match_id(home_api: str, away_api: str, upcoming: list) -> str | None:
    for match in upcoming:
        if teams_match(home_api, match["homeEn"]) and teams_match(away_api, match["awayEn"]):
            return match["id"]
    return None


def load_upcoming() -> list:
    if not MATCHDAY_FILE.exists():
        return []
    with open(MATCHDAY_FILE, encoding="utf-8") as f:
        md = json.load(f)
    return [m for m in md.get("upcoming", []) if m.get("status") == "pending"]


# ── Discovery helper ─────────────────────────────────────────────────────────

def list_leagues(api_key: str):
    """Print all available football league slugs to help configure LEAGUES dict."""
    data = api_get(OAIO_BASE, api_key, "/leagues", {"sport": "football"})
    if not data:
        print("⚠  No data returned — check API key.")
        return
    leagues = _unwrap(data)
    if not leagues:
        leagues = list(data.values())[0] if isinstance(data, dict) else []
    print(f"\n{'Slug':<45} Name")
    print("─" * 75)
    for lg in sorted(leagues, key=lambda x: x.get("slug", "")):
        print(f"{lg.get('slug', ''):<45} {lg.get('name', '')}")
    print(f"\nTotal: {len(leagues)} leagues\n")
    print("Update the LEAGUES dict in fetch_odds.py with the correct slugs.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch William Hill odds (hybrid: odds-api.io + the-odds-api.com for EPL)"
    )
    parser.add_argument("--key",          help="odds-api.io key (overrides ODDS_API_KEY)")
    parser.add_argument("--epl-key",      help="the-odds-api.com key (overrides THE_ODDS_API_KEY)")
    parser.add_argument("--list-leagues", action="store_true",
                        help="Print available odds-api.io league slugs and exit")
    args = parser.parse_args()

    toda_key = args.key     or os.environ.get("ODDS_API_KEY", "")    # the-odds-api.com (EPL)
    oaio_key = args.epl_key or os.environ.get("OAIO_API_KEY", "")   # odds-api.io (UEFA+)

    if not toda_key and not oaio_key:
        sys.exit("❌  No API keys found.\n"
                 "    ODDS_API_KEY  → the-odds-api.com (EPL)   https://the-odds-api.com\n"
                 "    OAIO_API_KEY  → odds-api.io (UEFA+)       https://odds-api.io")

    if args.list_leagues:
        if not oaio_key:
            sys.exit("❌  OAIO_API_KEY required for --list-leagues")
        list_leagues(oaio_key)
        return

    print(f"\n📡 Fetching William Hill odds (hybrid) …\n")

    upcoming_md = load_upcoming()
    print(f"  📋 {len(upcoming_md)} pending matches loaded from matchday.json\n")

    all_odds: dict[str, str] = {}

    # ── EPL via the-odds-api.com ──────────────────────────────────────────────
    if toda_key:
        print("── EPL (the-odds-api.com) ──────────────────────────────────────────")
        epl_odds = fetch_epl_odds(toda_key, upcoming_md)
        all_odds.update(epl_odds)
    else:
        print("  ℹ  THE_ODDS_API_KEY not set — skipping EPL odds refresh")

    # ── All other leagues via odds-api.io ─────────────────────────────────────
    if oaio_key:
        for sport_key, league_slug in LEAGUES.items():
            print(f"\n── {league_slug} ──────────────────────────────────────────")
            odds = process_league(oaio_key, league_slug, upcoming_md)
            all_odds.update(odds)
    else:
        print("  ℹ  OAIO_API_KEY not set — skipping UEFA/expansion league odds refresh")

    if not all_odds:
        print("\n⚠  No odds found — check API keys.")
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bookmaker":  "William Hill",
        "odds":       all_odds,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\n✅  Saved {len(all_odds)} odds entries → {OUTPUT_FILE}")

    # ── Patch odds into matchday.json (logic unchanged) ───────────────────────
    if not MATCHDAY_FILE.exists():
        return

    with open(MATCHDAY_FILE, encoding="utf-8") as f:
        md = json.load(f)

    updated_md = 0
    for match in md.get("upcoming", []):
        mid = match["id"]
        wh  = {}
        if f"{mid}_1"    in all_odds: wh["h"]    = all_odds[f"{mid}_1"]
        if f"{mid}_x"    in all_odds: wh["x"]    = all_odds[f"{mid}_x"]
        if f"{mid}_2"    in all_odds: wh["a"]    = all_odds[f"{mid}_2"]
        if f"{mid}_o25"  in all_odds: wh["o25"]  = all_odds[f"{mid}_o25"]
        if f"{mid}_o15"  in all_odds: wh["o15"]  = all_odds[f"{mid}_o15"]
        if f"{mid}_btts" in all_odds: wh["btts"] = all_odds[f"{mid}_btts"]
        if wh:
            match["odds_wh"] = {**match.get("odds_wh", {}), **wh}
            pick   = match.get("pick", {})
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
    bb     = md.get("betBuilder", {})
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
