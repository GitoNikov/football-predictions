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
from datetime import datetime, timezone, date
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed.")

FD_BASE     = "https://api.football-data.org/v4"
ODDS_BASE   = "https://api.the-odds-api.com/v4"
AF_BASE     = "https://v3.football.api-sports.io"
DATA_FILE   = Path("data/matchday.json")
COMPETITION = "PL"

# The Odds API sport keys for UEFA competitions
COMP_ODDS_SPORT = {
    "Champions League": "soccer_uefa_champs_league",
    "Europa League":    "soccer_uefa_europa_league",
    "Conference League": "soccer_uefa_europa_conference_league",
    # La Liga uses api-football.com directly (Odds API /scores free tier capped at 3 days)
}

# api-football.com league IDs for UEFA competitions (season = start year, e.g. 2025 for 2025-26)
COMP_AF_LEAGUE = {
    "Champions League": 2,
    "Europa League":    3,
    "Conference League": 848,
}

# football-data.org competition codes for domestic leagues (same API as EPL)
COMP_FD_CODE = {
    "La Liga": "PD",
}


def odds_scores(sport: str, odds_key: str, days_from: int) -> list:
    """Fetch completed scores from The Odds API scores endpoint."""
    url  = f"{ODDS_BASE}/sports/{sport}/scores/"
    resp = requests.get(url, params={"apiKey": odds_key, "daysFrom": days_from}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def af_scores(league_id: int, af_key: str, date_from: str, date_to: str) -> list:
    """Fetch finished fixtures from api-football.com for a UEFA league."""
    # api-football uses season start year: 2025-26 season → season=2025
    year = int(date_from[:4])
    month = int(date_from[5:7])
    season = year - 1 if month < 7 else year
    url  = f"{AF_BASE}/fixtures"
    headers = {"x-apisports-key": af_key}
    params  = {"league": league_id, "season": season,
               "from": date_from, "to": date_to, "status": "FT"}
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("response", [])


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


def determine_bb_result(markets: list, h: int, a: int) -> str:
    """Return 'W' only if every bet-builder market wins, 'L' if any loses."""
    total = h + a
    for mkt in markets:
        en = mkt.get("marketEn", "")
        if   en == "Home Win":          won = h > a
        elif en == "Away Win":          won = a > h
        elif en == "Draw":              won = h == a
        elif en == "BTTS Yes":          won = h > 0 and a > 0
        elif en == "Over 2.5 Goals":    won = total >= 3
        elif en == "Over 1.5 Goals":    won = total >= 2
        else:                           continue
        if not won:
            return "L"
    return "W"


def infer_market_selection(pick: dict) -> tuple[str, str]:
    """Infer market/selection from betEn when fields are missing."""
    bet_en = pick.get("betEn", "")
    if "Over 2.5" in bet_en:  return "over_under", "over_2.5"
    if "Over 1.5" in bet_en:  return "over_under", "over_1.5"
    if "Under 2.5" in bet_en: return "over_under", "under_2.5"
    if "BTTS" in bet_en:      return "btts", "yes"
    if "Draw" in bet_en:      return "h2h", "draw"
    return "h2h", "home"  # default


def determine_result(match: dict, home_score: int, away_score: int) -> str | None:
    """Return 'W', 'L', or None if the market/selection cannot be resolved."""
    pick      = match["pick"]
    market    = pick.get("market") or infer_market_selection(pick)[0]
    selection = pick.get("selection") or infer_market_selection(pick)[1]
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
    af_key = os.environ.get("API_FOOTBALL_KEY", "")

    if not DATA_FILE.exists():
        sys.exit(f"❌  {DATA_FILE} not found.")

    with open(DATA_FILE, encoding="utf-8") as f:
        md = json.load(f)

    upcoming  = md.get("upcoming", [])
    results   = md.get("results", [])
    record    = md.get("record",   {"correct": 0, "total": 0, "period": ""})
    bb_record = md.get("bbRecord", {"correct": 0, "total": 0})

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Only EPL pending matches whose date is in the past
    # UEFA stubs carry a top-level "competition" field; EPL matches do not
    pending_past = [
        m for m in upcoming
        if m.get("status") == "pending"
        and m.get("date", "9999-99-99") < today_str
        and not m.get("competition")
    ]

    # ── EPL match results ──────────────────────────────────────────────────────
    fd_matches = []
    moved = 0
    remaining_upcoming = list(upcoming)  # default: nothing moved

    # Past pending UEFA matches (have a competition field)
    pending_past_uefa = [
        m for m in upcoming
        if m.get("status") == "pending"
        and m.get("date", "9999-99-99") < today_str
        and m.get("competition")
    ]

    if not pending_past:
        print("No pending past EPL matches to check.")
    else:
        date_from = min(m["date"] for m in pending_past)
        date_to   = max(m["date"] for m in pending_past)

        print(f"\n🔍  Checking {len(pending_past)} pending past EPL matches "
              f"({date_from} – {date_to})…\n")

        path    = (f"/competitions/{COMPETITION}/matches"
                   f"?status=FINISHED&dateFrom={date_from}&dateTo={date_to}")
        payload = fd_get(path, fd_key)
        fd_matches = payload.get("matches", [])
        print(f"  ✓  football-data.org returned {len(fd_matches)} finished PL matches\n")

        remaining_upcoming = []
        for match in upcoming:
            if (match.get("status") != "pending"
                    or match.get("date", "9999-99-99") >= today_str
                    or match.get("competition")):
                remaining_upcoming.append(match)
                continue

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

    # ── UEFA match results (Odds API → api-football.com fallback) ────────────
    odds_key = os.environ.get("ODDS_API_KEY", "")
    if pending_past_uefa:
        if not odds_key and not af_key:
            print("⚠  Neither ODDS_API_KEY nor API_FOOTBALL_KEY set — skipping UEFA results.")
        else:
            by_comp: dict[str, list] = {}
            for m in pending_past_uefa:
                by_comp.setdefault(m["competition"], []).append(m)

            remaining_after_uefa = list(md.get("upcoming", remaining_upcoming))

            for comp_en, comp_matches in by_comp.items():
                date_from = min(m["date"] for m in comp_matches)
                date_to   = max(m["date"] for m in comp_matches)

                print(f"\n🏆  Checking {len(comp_matches)} pending past {comp_en} matches "
                      f"({date_from} – {date_to})…")

                # Domestic leagues (e.g. La Liga) use football-data.org directly
                completed: list = []
                source = ""
                fd_comp_code = COMP_FD_CODE.get(comp_en)
                if fd_comp_code and fd_key:
                    try:
                        path = (f"/competitions/{fd_comp_code}/matches"
                                f"?status=FINISHED&dateFrom={date_from}&dateTo={date_to}")
                        payload   = fd_get(path, fd_key)
                        fd_raw    = payload.get("matches", [])
                        completed = [
                            {
                                "_fd":        True,
                                "home_team":  m["homeTeam"]["name"],
                                "away_team":  m["awayTeam"]["name"],
                                "home_score": m["score"]["fullTime"]["home"],
                                "away_score": m["score"]["fullTime"]["away"],
                            }
                            for m in fd_raw
                            if m.get("score", {}).get("fullTime", {}).get("home") is not None
                        ]
                        source = "football-data.org"
                        print(f"  ✓  football-data.org returned {len(completed)} finished {comp_en} matches")
                    except Exception as e:
                        print(f"  ⚠  football-data.org failed for {comp_en}: {e}")

                # UEFA: Try The Odds API first (if sport key defined), fall back to api-football.com
                sport = COMP_ODDS_SPORT.get(comp_en)
                if not completed and odds_key and sport:
                    days_ago = (date.today() - date.fromisoformat(date_from)).days + 1
                    days_from_n = min(max(days_ago, 1), 3)
                    try:
                        fixtures  = odds_scores(sport, odds_key, days_from_n)
                        completed = [f for f in fixtures if f.get("completed")]
                        source    = "Odds API"
                        print(f"  ✓  Odds API returned {len(completed)} finished {comp_en} matches")
                    except Exception as e:
                        print(f"  ⚠  Odds API failed: {e} — trying api-football.com…")

                if not completed and af_key:
                    league_id = COMP_AF_LEAGUE.get(comp_en)
                    if not league_id:
                        print(f"  ⚠  No api-football league ID for {comp_en}, skipping")
                        continue
                    try:
                        af_fixtures = af_scores(league_id, af_key, date_from, date_to)
                        # Normalise to same shape used below
                        completed = [
                            {
                                "_af":       True,
                                "home_team": f["teams"]["home"]["name"],
                                "away_team": f["teams"]["away"]["name"],
                                "home_score": f["goals"]["home"],
                                "away_score": f["goals"]["away"],
                            }
                            for f in af_fixtures
                            if f.get("goals", {}).get("home") is not None
                        ]
                        source = "api-football.com"
                        print(f"  ✓  api-football.com returned {len(completed)} finished {comp_en} matches")
                    except Exception as e:
                        print(f"  ⚠  api-football.com failed: {e}")
                        continue

                if not completed:
                    print(f"  ⚠  No results found for {comp_en}")
                    continue

                still_pending = []
                for match in remaining_after_uefa:
                    if match.get("competition") != comp_en or match.get("status") != "pending":
                        still_pending.append(match)
                        continue

                    matched = None
                    for fix in completed:
                        h_name = fix.get("home_team", "")
                        a_name = fix.get("away_team", "")
                        if team_matches(h_name, match["homeEn"]) and \
                           team_matches(a_name, match["awayEn"]):
                            matched = fix
                            break

                    if not matched:
                        still_pending.append(match)
                        continue

                    if matched.get("_af") or matched.get("_fd"):
                        home_score = matched["home_score"]
                        away_score = matched["away_score"]
                    else:
                        scores     = matched.get("scores") or []
                        home_score = next((int(s["score"]) for s in scores
                                          if team_matches(s["name"], match["homeEn"])), None)
                        away_score = next((int(s["score"]) for s in scores
                                          if team_matches(s["name"], match["awayEn"])), None)

                    if home_score is None or away_score is None:
                        still_pending.append(match)
                        continue

                    result = determine_result(match, int(home_score), int(away_score))
                    if result is None:
                        still_pending.append(match)
                        continue

                    score_str = f"{home_score}-{away_score}"
                    results.insert(0, {
                        "id":            match["id"],
                        "home":          match["home"],
                        "homeEn":        match["homeEn"],
                        "away":          match["away"],
                        "awayEn":        match["awayEn"],
                        "date":          match["date"],
                        "competition":   match.get("competitionBG", comp_en),
                        "competitionEn": comp_en,
                        "pick":          match["pick"],
                        "result":        result,
                        "score":         score_str,
                    })
                    record["correct"] = record.get("correct", 0) + (1 if result == "W" else 0)
                    record["total"]   = record.get("total", 0) + 1
                    icon = "✓" if result == "W" else "✗"
                    print(f"  {icon}  {match['homeEn']} vs {match['awayEn']} → {result} ({score_str})  [{source}]")
                    moved += 1

                remaining_after_uefa = still_pending

            md["upcoming"] = remaining_after_uefa
            md["results"]  = results[:30]
            md["record"]   = record

    # ── Bet builder result ─────────────────────────────────────────────────────
    bb = md.get("betBuilder")
    bb_changed = False
    if bb and bb.get("homeEn") and not bb.get("result"):
        # Find the source match to determine which competition it's from
        bb_src  = next((m for m in upcoming if m.get("id") == bb.get("matchId")), None)
        bb_comp = bb_src.get("competition") if bb_src else None
        bb_date = bb_src.get("date") if bb_src else None

        if bb_comp and bb_date:
            # UEFA match — try Odds API first, fall back to api-football.com
            bb_completed: list = []
            sport = COMP_ODDS_SPORT.get(bb_comp)
            if sport and odds_key:
                days_ago  = (date.today() - date.fromisoformat(bb_date)).days + 1
                days_from_n = min(max(days_ago, 1), 3)
                try:
                    fixtures     = odds_scores(sport, odds_key, days_from_n)
                    bb_completed = [f for f in fixtures if f.get("completed")]
                    print(f"\n🏆  betBuilder {bb_comp}: {len(bb_completed)} finished matches on {bb_date}")
                except Exception as e:
                    print(f"  ⚠  Odds API betBuilder failed: {e}")

            if not bb_completed and af_key:
                league_id = COMP_AF_LEAGUE.get(bb_comp)
                if league_id:
                    try:
                        af_fix = af_scores(league_id, af_key, bb_date, bb_date)
                        bb_completed = [
                            {
                                "_af": True,
                                "home_team":  f["teams"]["home"]["name"],
                                "away_team":  f["teams"]["away"]["name"],
                                "home_score": f["goals"]["home"],
                                "away_score": f["goals"]["away"],
                            }
                            for f in af_fix if f.get("goals", {}).get("home") is not None
                        ]
                        print(f"\n🏆  betBuilder {bb_comp} (api-football): {len(bb_completed)} matches")
                    except Exception as e:
                        print(f"  ⚠  api-football.com betBuilder failed: {e}")

            for fix in bb_completed:
                if team_matches(fix.get("home_team", ""), bb["homeEn"]) and \
                   team_matches(fix.get("away_team", ""), bb["awayEn"]):
                    if fix.get("_af"):
                        h_s, a_s = fix["home_score"], fix["away_score"]
                    else:
                        scores = fix.get("scores") or []
                        h_s = next((int(s["score"]) for s in scores
                                    if team_matches(s["name"], bb["homeEn"])), None)
                        a_s = next((int(s["score"]) for s in scores
                                    if team_matches(s["name"], bb["awayEn"])), None)
                    if h_s is not None and a_s is not None:
                        h_s, a_s = int(h_s), int(a_s)
                        bb_res = determine_bb_result(bb.get("markets", []), h_s, a_s)
                        bb["result"] = bb_res
                        bb_record["correct"] = bb_record.get("correct", 0) + (1 if bb_res == "W" else 0)
                        bb_record["total"]   = bb_record.get("total", 0) + 1
                        bb_changed = True
                        icon = "✓" if bb_res == "W" else "✗"
                        print(f"  {icon}  betBuilder {bb['homeEn']} vs {bb['awayEn']} "
                              f"→ {bb_res} ({h_s}-{a_s})")
                    break
        else:
            # EPL / domestic — use already-fetched fd_matches
            for fd_m in fd_matches:
                fd_home = fd_m.get("homeTeam", {}).get("name", "")
                fd_away = fd_m.get("awayTeam", {}).get("name", "")
                if team_matches(fd_home, bb["homeEn"]) and team_matches(fd_away, bb["awayEn"]):
                    ft  = fd_m.get("score", {}).get("fullTime", {})
                    h_s = ft.get("home")
                    a_s = ft.get("away")
                    if h_s is not None and a_s is not None:
                        h_s, a_s = int(h_s), int(a_s)
                        bb_res = determine_bb_result(bb.get("markets", []), h_s, a_s)
                        bb["result"] = bb_res
                        bb_record["correct"] = bb_record.get("correct", 0) + (1 if bb_res == "W" else 0)
                        bb_record["total"]   = bb_record.get("total", 0) + 1
                        bb_changed = True
                        icon = "✓" if bb_res == "W" else "✗"
                        print(f"  {icon}  betBuilder {bb['homeEn']} vs {bb['awayEn']} "
                              f"→ {bb_res} ({h_s}-{a_s})")
                    break

    md["bbRecord"] = bb_record

    if not pending_past and not pending_past_uefa and not bb_changed:
        print("Nothing to update.")
        return

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(md, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Done. EPL: {moved} moved, record {record['correct']}/{record['total']} | "
          f"BB: {bb_record['correct']}/{bb_record['total']}\n")


if __name__ == "__main__":
    main()
