"""
fetch_matches.py — Weekly auto-setup script for Football Predictions Site.

Pulls real EPL standings and form from football-data.org, combines with
The Odds API for match list and odds, then uses Groq to generate picks
and select a betBuilder combo. Updates data/matchday.json.

Run once per week (Monday morning) via GitHub Actions weekly_setup.yml.

Required env vars:
    ODDS_API_KEY           — from the-odds-api.com
    FOOTBALL_DATA_API_KEY  — from football-data.org (free, no card needed)
    GROQ_API_KEY           — from console.groq.com (free)

Usage:
    export ODDS_API_KEY=...
    export FOOTBALL_DATA_API_KEY=...
    export GROQ_API_KEY=...
    python fetch_matches.py
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  requests not installed. Run: pip install requests")

try:
    from groq import Groq
except ImportError:
    sys.exit("❌  groq not installed. Run: pip install groq")

# ── Config ─────────────────────────────────────────────────────────────────────
MATCHDAY_FILE   = Path("data/matchday.json")
ODDS_BASE       = "https://api.the-odds-api.com/v4"
FD_BASE         = "https://api.football-data.org/v4"
MODEL_NAME      = "llama-3.3-70b-versatile"
FORM_SLEEP      = 6.5   # seconds between football-data.org team requests
COMPETITION     = "PL"  # Premier League code

# UEFA competitions to auto-populate (fixtures only, no AI analysis)
UEFA_SPORTS = {
    "soccer_uefa_champs_league":            {"bg": "Шампионска лига", "en": "Champions League"},
    "soccer_uefa_europa_league":            {"bg": "Лига Европа",     "en": "Europa League"},
    "soccer_uefa_europa_conference_league": {"bg": "Конференц лига",  "en": "Conference League"},
}

# ── Bulgarian team name mapping ────────────────────────────────────────────────
TEAM_BG = {
    "Arsenal":           "Арсенал",
    "Aston Villa":       "Астън Вила",
    "Bournemouth":       "Борнемут",
    "Brentford":         "Брентфорд",
    "Burnley":           "Бърнли",
    "Brighton":          "Брайтън",
    "Brighton & Hove Albion": "Брайтън",
    "Chelsea":           "Челси",
    "Crystal Palace":    "Кристъл Палас",
    "Everton":           "Евъртън",
    "Fulham":            "Фулъм",
    "Ipswich":           "Ипсуич",
    "Ipswich Town":      "Ипсуич",
    "Leeds":             "Лийдс",
    "Leeds United":      "Лийдс",
    "Leicester":         "Лестър",
    "Leicester City":    "Лестър",
    "Liverpool":         "Ливърпул",
    "Man City":          "Ман Сити",
    "Manchester City":   "Ман Сити",
    "Man United":        "Ман Юнайтед",
    "Manchester United": "Ман Юнайтед",
    "Newcastle":         "Нюкасъл",
    "Newcastle United":  "Нюкасъл",
    "Nottm Forest":      "Нотингам Форест",
    "Nottingham Forest": "Нотингам Форест",
    "Southampton":       "Саутхамптън",
    "Sunderland":        "Съндърланд",
    "Tottenham":         "Тотнъм",
    "Tottenham Hotspur": "Тотнъм",
    "West Ham":          "Уест Хем",
    "West Ham United":   "Уест Хем",
    "Wolves":            "Уулвърхямптън",
    "Wolverhampton Wanderers": "Уулвърхямптън",
}

# Short abbreviations for match IDs (3 chars lowercase)
TEAM_ABBR = {
    "Arsenal":           "ars",
    "Aston Villa":       "avl",
    "Bournemouth":       "bou",
    "Brentford":         "bre",
    "Burnley":           "bur",
    "Brighton":          "bri",
    "Brighton & Hove Albion": "bri",
    "Chelsea":           "che",
    "Crystal Palace":    "pal",
    "Everton":           "eve",
    "Fulham":            "ful",
    "Ipswich":           "ips",
    "Ipswich Town":      "ips",
    "Leeds":             "lee",
    "Leeds United":      "lee",
    "Leicester":         "lei",
    "Leicester City":    "lei",
    "Liverpool":         "liv",
    "Man City":          "mci",
    "Manchester City":   "mci",
    "Man United":        "mun",
    "Manchester United": "mun",
    "Newcastle":         "new",
    "Newcastle United":  "new",
    "Nottm Forest":      "for",
    "Nottingham Forest": "for",
    "Southampton":       "sou",
    "Sunderland":        "sun",
    "Tottenham":         "tot",
    "Tottenham Hotspur": "tot",
    "West Ham":          "whu",
    "West Ham United":   "whu",
    "Wolves":            "wol",
    "Wolverhampton Wanderers": "wol",
}

# Market labels for betBuilder — Python controls BG/EN, Groq only picks the key
MARKET_LABELS = {
    "h":    {"market": "Победа домакин",  "marketEn": "Home Win"},
    "x":    {"market": "Равенство",       "marketEn": "Draw"},
    "a":    {"market": "Победа гост",     "marketEn": "Away Win"},
    "btts": {"market": "И двата вкарват", "marketEn": "BTTS Yes"},
    "o25":  {"market": "Над 2.5 гола",    "marketEn": "Over 2.5 Goals"},
    "o15":  {"market": "Над 1.5 гола",    "marketEn": "Over 1.5 Goals"},
}

# Python-generated Bulgarian pick labels — never rely on Groq for these
def bet_bg(market: str, selection: str, home_bg: str, away_bg: str) -> str:
    if market == "h2h":
        if selection == "home": return f"{home_bg} победа"
        if selection == "away": return f"{away_bg} победа"
        if selection == "draw": return "Равенство"
    if market == "btts":
        return "И двата вкарват" if selection == "yes" else "И двата не вкарват"
    if market == "over_under":
        if "2.5" in selection: return "Над 2.5 гола" if "over" in selection else "Под 2.5 гола"
        if "1.5" in selection: return "Над 1.5 гола" if "over" in selection else "Под 1.5 гола"
    return f"{home_bg} победа"

# BG names for common UEFA club teams (fallback to English if not found)
UEFA_TEAM_BG = {
    "Real Madrid":           "Реал Мадрид",
    "Barcelona":             "Барселона",
    "Atletico Madrid":       "Атлетико Мадрид",
    "Real Sociedad":         "Реал Сосиедад",
    "Real Betis":            "Реал Бетис",
    "Athletic Club":         "Атлетик Билбао",
    "Sevilla":               "Севиля",
    "Villarreal":            "Вияреал",
    "Valencia":              "Валенсия",
    "Osasuna":               "Осасуна",
    "Bayern Munich":         "Байерн",
    "Borussia Dortmund":     "Дортмунд",
    "Bayer Leverkusen":      "Леверкузен",
    "RB Leipzig":            "Лайпциг",
    "Eintracht Frankfurt":   "Айнтрахт Франкфурт",
    "Stuttgart":             "Щутгарт",
    "Union Berlin":          "Унион Берлин",
    "Paris Saint Germain":   "ПСЖ",
    "Lyon":                  "Лион",
    "Marseille":             "Марсей",
    "Monaco":                "Монако",
    "Lille":                 "Лил",
    "Rennes":                "Рен",
    "AC Milan":              "Милан",
    "Inter Milan":           "Интер",
    "Juventus":              "Ювентус",
    "Napoli":                "Наполи",
    "Roma":                  "Рома",
    "Lazio":                 "Лацио",
    "Atalanta":              "Аталанта",
    "Fiorentina":            "Фиорентина",
    "Bologna":               "Болоня",
    "Benfica":               "Бенфика",
    "Porto":                 "Порто",
    "Sporting CP":           "Спортинг",
    "Braga":                 "Брага",
    "Ajax":                  "Аякс",
    "PSV Eindhoven":         "ПСВ",
    "Feyenoord":             "Фейенорд",
    "Club Brugge":           "Брюж",
    "Anderlecht":            "Андерлехт",
    "Celtic":                "Селтик",
    "Rangers":               "Рейнджърс",
    "Red Bull Salzburg":     "Залцбург",
    "Rapid Vienna":          "Рапид Виена",
    "Galatasaray":           "Галатасарай",
    "Fenerbahce":            "Фенербахче",
    "Besiktas":              "Бешикташ",
    "Slavia Prague":         "Славия Прага",
    "Shakhtar Donetsk":      "Шахтьор",
    "Dynamo Kyiv":           "Динамо Киев",
    "Olympiakos":            "Олимпиакос",
    "PAOK":                  "ПАОК",
    "Maccabi Tel Aviv":      "Маккаби Тел Авив",
}

# 3-char display abbreviations for homeA/awayA
TEAM_ABBR_DISPLAY = {
    "ars": "ARS", "avl": "AVL", "bou": "BOU", "bre": "BRE", "bri": "BRI",
    "che": "CHE", "pal": "PAL", "eve": "EVE", "ful": "FUL", "ips": "IPS",
    "lee": "LEE", "lei": "LEI", "liv": "LIV", "mci": "MCI", "mun": "MUN",
    "new": "NEW", "for": "FOR", "sou": "SOU", "sun": "SUN", "tot": "TOT",
    "whu": "WHU", "wol": "WOL",
}


def normalize_team(name: str) -> str:
    """Return the canonical English short name for a team."""
    name_lower = name.lower()
    # 1. Exact substring check
    for key in TEAM_ABBR:
        kl = key.lower()
        if kl in name_lower or name_lower in kl:
            return key
    # 2. Best word-overlap fallback (significant words only, len > 3)
    name_words = {w for w in name_lower.split() if len(w) > 3}
    best_key, best_score = None, 0
    for key in TEAM_ABBR:
        key_words = {w for w in key.lower().split() if len(w) > 3}
        score = len(name_words & key_words)
        if score > best_score:
            best_score, best_key = score, key
    return best_key if best_key else name


def team_bg(en_name: str) -> str:
    for key, bg in TEAM_BG.items():
        if key.lower() in en_name.lower() or en_name.lower() in key.lower():
            return bg
    return en_name


def team_bg_uefa(en_name: str) -> str:
    """BG name lookup: UEFA dict first, then EPL dict, then English fallback."""
    name_lower = en_name.lower()
    for key, bg in UEFA_TEAM_BG.items():
        if key.lower() in name_lower or name_lower in key.lower():
            return bg
    return team_bg(en_name)


def team_abbr(en_name: str) -> str:
    for key, abbr in TEAM_ABBR.items():
        if key.lower() in en_name.lower() or en_name.lower() in key.lower():
            return abbr
    return en_name[:3].lower()


def make_match_id(home_en: str, away_en: str) -> str:
    h = team_abbr(home_en)
    a = team_abbr(away_en)
    return f"{h}_{a}"


# ── Odds API ──────────────────────────────────────────────────────────────────
def fetch_epl_events(api_key: str) -> list:
    """Fetch upcoming EPL events from The Odds API (next 14 days)."""
    url = f"{ODDS_BASE}/sports/soccer_epl/odds/"
    params = {
        "apiKey":     api_key,
        "regions":    "eu",
        "markets":    "h2h,totals",
        "oddsFormat": "decimal",
        "bookmakers": "williamhill",
    }
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code == 401:
        sys.exit("❌  Invalid ODDS_API_KEY")
    resp.raise_for_status()
    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"  ✓  Odds API: {len(resp.json())} EPL events | quota remaining {remaining}")
    return resp.json()


def fetch_uefa_fixtures(api_key: str, existing_ids: set, groq_client) -> list:
    """
    Fetch upcoming CL/EL/ECL fixtures from Odds API /odds (includes WH odds).
    Generates Groq picks when odds are available, so cards are fully populated.
    """
    new_fixtures = []
    now        = datetime.now(timezone.utc)
    window_end = now + timedelta(days=7)

    sel_to_key = {
        "home": "h", "away": "a", "draw": "x",
        "yes": "btts", "no": "btts",
        "over_2.5": "o25", "under_2.5": "o25",
        "over_1.5": "o15", "under_1.5": "o15",
    }

    for sport_key, comp in UEFA_SPORTS.items():
        url    = f"{ODDS_BASE}/sports/{sport_key}/odds/"
        params = {
            "apiKey":     api_key,
            "regions":    "eu",
            "markets":    "h2h,totals",
            "oddsFormat": "decimal",
            "bookmakers": "williamhill",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code in (404, 422):
                print(f"  ⚠  {comp['en']}: no events (off-season or unavailable)")
                continue
            resp.raise_for_status()
        except Exception as e:
            print(f"  ⚠  {comp['en']} error: {e}")
            continue

        remaining = resp.headers.get("x-requests-remaining", "?")
        events    = resp.json()
        added     = 0

        for ev in events:
            ct = ev.get("commence_time", "")
            try:
                dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            except Exception:
                continue
            if not (now <= dt <= window_end):
                continue

            match_id = ev["id"][:8]
            if match_id in existing_ids:
                continue

            home_en = ev["home_team"]
            away_en = ev["away_team"]
            home_bg = team_bg_uefa(home_en)
            away_bg = team_bg_uefa(away_en)

            dt_sofia = dt + timedelta(hours=2)
            date_str = dt_sofia.strftime("%Y-%m-%d")
            time_str = dt_sofia.strftime("%H:%M")

            odds_wh = extract_wh_odds(ev)
            ai_ctx  = f"{home_en} vs {away_en} in the {comp['en']}."

            if odds_wh:
                print(f"\n  🔎  Searching news for {home_en} vs {away_en}…", end=" ", flush=True)
                news = search_team_news(home_en, away_en)
                print("✓")
                print(f"  🤖  Groq pick for {home_en} vs {away_en}…", end=" ", flush=True)
                pick_raw = groq_pick(groq_client, home_en, away_en, ai_ctx, odds_wh, news)
                print("✓")

                market = pick_raw.get("market", "h2h")
                sel    = pick_raw.get("selection", "home")
                resolved_odd = odds_wh.get(sel_to_key.get(sel, "h"), pick_raw.get("odd", "1.80"))
                pick = {
                    "bet":       bet_bg(market, sel, home_bg, away_bg),
                    "betEn":     pick_raw.get("betEN", f"{home_en} Win"),
                    "conf":      int(pick_raw.get("conf", 55)),
                    "market":    market,
                    "selection": sel,
                    "odd":       str(resolved_odd),
                }
                prob = {
                    "h": round(100 / float(odds_wh["h"])) if "h" in odds_wh else 50,
                    "d": round(100 / float(odds_wh["x"])) if "x" in odds_wh else 25,
                    "a": round(100 / float(odds_wh["a"])) if "a" in odds_wh else 25,
                }
            else:
                pick = {"bet": "—", "betEn": "—", "conf": 0, "market": "h2h", "selection": "home", "odd": "?"}
                prob = {"h": 50, "d": 25, "a": 25}

            new_fixtures.append({
                "id":            match_id,
                "home":          home_bg,
                "homeEn":        home_en,
                "homeA":         home_en[:3].upper(),
                "away":          away_bg,
                "awayEn":        away_en,
                "awayA":         away_en[:3].upper(),
                "date":          date_str,
                "time":          time_str,
                "competition":   comp["en"],
                "competitionBG": comp["bg"],
                "status":        "pending",
                "pick":          pick,
                "odds_wh":       odds_wh,
                "prob":          prob,
                "aiCtx":         ai_ctx,
            })
            added += 1

        print(f"  ✓  {comp['en']}: {added} new fixtures | quota remaining {remaining}")

    return new_fixtures


def extract_wh_odds(event: dict) -> dict:
    """Extract William Hill h2h + totals from a single event."""
    wh = next((bk for bk in event.get("bookmakers", []) if bk["key"] == "williamhill"), None)
    if not wh:
        return {}
    result = {}
    for market in wh.get("markets", []):
        if market["key"] == "h2h":
            for o in market["outcomes"]:
                if o["name"] == "Draw":
                    result["x"] = str(round(o["price"], 2))
                elif o["name"] == event["home_team"]:
                    result["h"] = str(round(o["price"], 2))
                else:
                    result["a"] = str(round(o["price"], 2))
        elif market["key"] == "totals":
            for point in (2.5, 1.5):
                key = f"o{str(point).replace('.', '')}"
                for o in market["outcomes"]:
                    if o.get("point") == point and o["name"] == "Over":
                        result[key] = str(round(o["price"], 2))
    return result


def filter_next_gameweek(events: list) -> list:
    """
    Return events in the next gameweek window.
    Groups by date cluster: first non-empty cluster within 14 days.
    """
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(days=14)

    upcoming = []
    for ev in events:
        ct = ev.get("commence_time", "")
        try:
            dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        except Exception:
            continue
        if now <= dt <= window_end:
            upcoming.append((dt, ev))

    if not upcoming:
        return []

    upcoming.sort(key=lambda x: x[0])
    # Find the date of the first fixture
    first_date = upcoming[0][0].date()
    # Include all fixtures within 4 days of the first one (covers Fri-Mon window)
    cutoff = first_date + timedelta(days=4)
    return [ev for dt, ev in upcoming if dt.date() <= cutoff]


# ── football-data.org ─────────────────────────────────────────────────────────
def fd_get(path: str, fd_key: str) -> dict:
    url = f"{FD_BASE}{path}"
    headers = {"X-Auth-Token": fd_key}
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 429:
        print("  ⚠  Rate limit hit, sleeping 30s…")
        time.sleep(30)
        resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 401:
        sys.exit("❌  Invalid FOOTBALL_DATA_API_KEY")
    resp.raise_for_status()
    return resp.json()


def fetch_standings(fd_key: str) -> tuple[dict, int]:
    """
    Returns:
        standings_map: {team_name_en: {pos, pts, team_id}}
        current_matchday: int
    """
    data = fd_get(f"/competitions/{COMPETITION}/standings", fd_key)
    matchday = data.get("season", {}).get("currentMatchday", 0)
    standings_map = {}
    for table in data.get("standings", []):
        if table.get("type") == "TOTAL":
            for entry in table.get("table", []):
                name = entry["team"]["name"]
                standings_map[name] = {
                    "pos":     entry["position"],
                    "pts":     entry["points"],
                    "team_id": entry["team"]["id"],
                }
    print(f"  ✓  Standings: {len(standings_map)} teams | matchday {matchday}")
    return standings_map, matchday


def fetch_team_form(team_id: int, fd_key: str, limit: int = 6) -> str:
    """Return form string like 'W-W-D-L-W' for last N finished matches."""
    data = fd_get(f"/teams/{team_id}/matches?status=FINISHED&limit={limit}", fd_key)
    matches = data.get("matches", [])
    form_parts = []
    for match in matches[-limit:]:
        home_id = match.get("homeTeam", {}).get("id")
        score   = match.get("score", {}).get("winner")
        if score == "HOME_TEAM":
            form_parts.append("W" if team_id == home_id else "L")
        elif score == "AWAY_TEAM":
            form_parts.append("L" if team_id == home_id else "W")
        elif score == "DRAW":
            form_parts.append("D")
    return "-".join(form_parts) if form_parts else "N/A"


def find_standing(name_en: str, standings_map: dict) -> dict | None:
    """Fuzzy-match a team name to standings, returning the best match."""
    name_lower = name_en.lower()
    # 1. Exact substring check
    for key, val in standings_map.items():
        if key.lower() in name_lower or name_lower in key.lower():
            return val
    # 2. Best word-overlap (significant words only, len > 3)
    name_words = {w for w in name_lower.split() if len(w) > 3}
    best_val, best_score = None, 0
    for key, val in standings_map.items():
        key_words = {w for w in key.lower().split() if len(w) > 3}
        score = len(name_words & key_words)
        if score > best_score:
            best_score, best_val = score, val
    return best_val


def ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n%10,4)]}"


def build_ai_ctx(home_en: str, away_en: str, home_st: dict, away_st: dict,
                 home_form: str, away_form: str) -> str:
    h_pos  = ordinal(home_st["pos"])
    a_pos  = ordinal(away_st["pos"])
    h_pts  = home_st["pts"]
    a_pts  = away_st["pts"]
    return (
        f"{home_en} are {h_pos} in the Premier League ({h_pts} pts), "
        f"form: {home_form}. "
        f"{away_en} are {a_pos} ({a_pts} pts), "
        f"form: {away_form}."
    )


# ── Web search ────────────────────────────────────────────────────────────────
def search_team_news(home_en: str, away_en: str) -> str:
    """Search DuckDuckGo for injuries, suspensions, and team news. Returns a short summary."""
    # Domains that never contain useful football team news
    SKIP_DOMAINS = (
        "wikipedia.org", "tripadvisor", "booking.com", "airbnb",
        "visitbournemouth", "timeout.com", "yelp.com", "hotels.com",
        "expedia.com", "lonelyplanet.com", "britannica.com",
    )
    try:
        from duckduckgo_search import DDGS
        query = f"{home_en} FC vs {away_en} FC premier league injury suspension team news"
        results = DDGS().text(query, max_results=6)
        snippets = [
            r["body"] for r in results
            if r.get("body") and not any(d in r.get("href", "") for d in SKIP_DOMAINS)
        ]
        return " | ".join(snippets[:3]) if snippets else "No recent news found."
    except Exception as e:
        print(f"    ⚠  News search failed: {e}")
        return "News search unavailable."


# ── Groq ──────────────────────────────────────────────────────────────────────
def groq_pick(client, home_en: str, away_en: str, ai_ctx: str, odds_wh: dict, news: str = "") -> dict:
    """Ask Groq to suggest a pick. Returns pick dict."""
    odds_str = json.dumps(odds_wh)
    prompt = f"""You are an expert football analyst. Suggest one pick for:
{home_en} vs {away_en}
Context: {ai_ctx}
Latest news (injuries/suspensions): {news}
William Hill odds: {odds_str}

Prefer odds between 1.40 and 2.50 (compatible with accumulator systems).
Return ONLY valid JSON, no markdown:
{{
  "market": "h2h",
  "selection": "home",
  "odd": "1.75",
  "conf": 62,
  "betBG": "Борнемут победа",
  "betEN": "Bournemouth Win"
}}
market values: h2h | btts | over_under
selection values: home | away | draw | yes | no | over_2.5 | under_2.5 | over_1.5"""

    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=200,
        )
        text = resp.choices[0].message.content.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f"    ⚠  Groq pick error: {e}")
        # Fallback: pick home win if odds available
        return {
            "market": "h2h", "selection": "home",
            "odd": odds_wh.get("h", "1.80"),
            "conf": 55,
            "betBG": f"{team_bg(home_en)} победа",
            "betEN": f"{home_en} Win",
        }


def groq_bet_builder(client, matches: list) -> dict | None:
    """Ask Groq to select best match + markets for betBuilder."""
    summary = []
    for m in matches:
        wh = m.get("odds_wh", {})
        summary.append({
            "matchId":  m["id"],
            "homeEn":   m["homeEn"],
            "awayEn":   m["awayEn"],
            "odds_wh":  wh,
            "aiCtx":    m.get("aiCtx", ""),
        })

    prompt = f"""You are an expert football analyst building a bet builder (same-game multi).
Select ONE match and 2-3 markets that combine well. Use ONLY the provided odds.
Matches: {json.dumps(summary)}

Available market keys: h=Home Win, x=Draw, a=Away Win, btts=BTTS Yes, o25=Over 2.5 Goals, o15=Over 1.5 Goals
Only use keys that exist in the match's odds_wh object.

Return ONLY valid JSON, no markdown:
{{
  "matchId": "match_id_here",
  "markets": [
    {{"key": "o15", "odd": "1.25"}},
    {{"key": "btts", "odd": "1.72"}},
    {{"key": "h", "odd": "1.37"}}
  ]
}}
Prefer markets that logically combine (e.g., high-scoring game + BTTS + home win)."""

    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=300,
        )
        text = resp.choices[0].message.content.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f"    ⚠  Groq betBuilder error: {e}")
        return None


# ── GW archive ────────────────────────────────────────────────────────────────
def archive_current_gw(existing: dict) -> None:
    """Save the current GW's results to data/history/ before starting a new GW."""
    label_en = existing.get("labelEn", "")
    m = re.search(r"GW(\d+)", label_en)
    if not m:
        print("  ⚠  Cannot determine GW number for archiving — skipping")
        return
    gw_num  = int(m.group(1))
    gw_tag  = f"GW{gw_num}"

    gw_results = [
        r for r in existing.get("results", [])
        if gw_tag in r.get("competitionEn", "")
    ]
    if not gw_results:
        print(f"  ⚠  No results tagged {gw_tag} — skipping archive")
        return

    correct = sum(1 for r in gw_results if r.get("result") == "W")
    total   = len(gw_results)

    HISTORY_DIR = Path("data/history")
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    archive = {
        "gw":           gw_num,
        "label":        existing.get("label", ""),
        "labelEn":      label_en,
        "date_range":   existing.get("date_range", ""),
        "date_rangeEn": existing.get("date_rangeEn", ""),
        "results":      gw_results,
        "record":       {"correct": correct, "total": total},
    }
    archive_path = HISTORY_DIR / f"gw-{gw_num}.json"
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(archive, f, ensure_ascii=False, indent=2)

    # Update index
    index_path = HISTORY_DIR / "index.json"
    index = []
    if index_path.exists():
        try:
            with open(index_path, encoding="utf-8") as f:
                index = json.load(f)
        except Exception:
            pass
    index = [e for e in index if e.get("gw") != gw_num]
    index.insert(0, {
        "gw":      gw_num,
        "label":   existing.get("label", ""),
        "labelEn": label_en,
        "correct": correct,
        "total":   total,
    })
    index.sort(key=lambda x: x.get("gw", 0), reverse=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"  📦  Archived {gw_tag}: {correct}/{total} correct → {archive_path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    odds_key = os.environ.get("ODDS_API_KEY", "")
    fd_key   = os.environ.get("FOOTBALL_DATA_API_KEY", "")
    groq_key = os.environ.get("GROQ_API_KEY", "")

    if not odds_key:
        sys.exit("❌  Set ODDS_API_KEY env var")
    if not fd_key:
        sys.exit("❌  Set FOOTBALL_DATA_API_KEY env var (free at football-data.org)")
    if not groq_key:
        sys.exit("❌  Set GROQ_API_KEY env var")

    print("\n🚀  Weekly match setup starting…\n")

    # Load existing matchday.json
    MATCHDAY_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if MATCHDAY_FILE.exists():
        with open(MATCHDAY_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    existing_ids = {m["id"] for m in existing.get("upcoming", [])}

    # ── Archive the current GW before overwriting ─────────────────────────────
    if existing.get("results"):
        print("📦  Archiving current GW…")
        archive_current_gw(existing)
        print()

    # ── Step 1: Get upcoming EPL events from Odds API ─────────────────────────
    print("📡  Fetching EPL events from Odds API…")
    all_events = fetch_epl_events(odds_key)
    gw_events  = filter_next_gameweek(all_events)
    if not gw_events:
        print("⚠  No upcoming EPL events found in next 14 days.")
        sys.exit(0)
    print(f"  → {len(gw_events)} events in next gameweek window")

    # ── Step 2: Get EPL standings from football-data.org ─────────────────────
    print("\n📊  Fetching EPL standings from football-data.org…")
    standings_map, current_matchday = fetch_standings(fd_key)

    # ── Step 3: Collect unique team IDs for form fetching ────────────────────
    teams_needed: dict[str, int] = {}  # name_en → team_id
    for ev in gw_events:
        for team_name in (ev["home_team"], ev["away_team"]):
            norm = normalize_team(team_name)
            st = find_standing(norm, standings_map)
            if st and norm not in teams_needed:
                teams_needed[norm] = st["team_id"]

    print(f"\n⚽  Fetching form for {len(teams_needed)} teams (6.5s sleep between calls)…")
    team_forms: dict[str, str] = {}
    for i, (name, tid) in enumerate(teams_needed.items()):
        if i > 0:
            time.sleep(FORM_SLEEP)
        form = fetch_team_form(tid, fd_key)
        team_forms[name] = form
        print(f"  ✓  {name}: {form}")

    # ── Step 4: Build new match entries / refresh aiCtx on existing ──────────
    groq_client = Groq(api_key=groq_key)
    new_matches = []

    for ev in gw_events:
        home_raw  = ev["home_team"]
        away_raw  = ev["away_team"]
        home_en   = normalize_team(home_raw)
        away_en   = normalize_team(away_raw)
        match_id  = make_match_id(home_en, away_en)

        home_st = find_standing(home_en, standings_map) or {"pos": 0, "pts": 0, "team_id": 0}
        away_st = find_standing(away_en, standings_map) or {"pos": 0, "pts": 0, "team_id": 0}
        home_form = team_forms.get(home_en, "N/A")
        away_form = team_forms.get(away_en, "N/A")
        ai_ctx = build_ai_ctx(home_en, away_en, home_st, away_st, home_form, away_form)

        if match_id in existing_ids:
            # Refresh aiCtx on the existing entry so generate_analysis picks it up
            for m in existing.get("upcoming", []):
                if m["id"] == match_id and m.get("status") == "pending":
                    m["aiCtx"] = ai_ctx
                    # Clear aiCtxHash so generate_analysis.py regenerates the text
                    m.pop("aiCtxHash", None)
                    print(f"  ↻  {home_en} vs {away_en} — aiCtx refreshed with real standings")
            continue

        home_bg = team_bg(home_en)
        away_bg = team_bg(away_en)
        h_abbr  = team_abbr(home_en).upper()
        a_abbr  = team_abbr(away_en).upper()

        # Parse date/time
        ct = ev.get("commence_time", "")
        try:
            dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            # Convert to Sofia time (UTC+2 in standard time, UTC+3 summer)
            sofia_offset = timedelta(hours=2)
            dt_sofia = dt + sofia_offset
            date_str = dt_sofia.strftime("%Y-%m-%d")
            time_str = dt_sofia.strftime("%H:%M")
        except Exception:
            date_str = ""
            time_str = ""

        # Odds
        odds_wh = extract_wh_odds(ev)

        # News search + Groq pick
        print(f"\n  🔎  Searching news for {home_en} vs {away_en}…", end=" ", flush=True)
        news = search_team_news(home_en, away_en)
        print("✓")
        print(f"  🤖  Groq pick for {home_en} vs {away_en}…", end=" ", flush=True)
        pick_raw = groq_pick(groq_client, home_en, away_en, ai_ctx, odds_wh, news)

        # Map selection → correct odd from odds_wh
        sel_to_key = {
            "home": "h", "away": "a", "draw": "x",
            "yes": "btts", "no": "btts",
            "over_2.5": "o25", "under_2.5": "o25",
            "over_1.5": "o15", "under_1.5": "o15",
        }
        market = pick_raw.get("market", "h2h")
        sel    = pick_raw.get("selection", "home")
        odds_key_mapped = sel_to_key.get(sel, "h")
        resolved_odd = odds_wh.get(odds_key_mapped, pick_raw.get("odd", "1.80"))

        match_entry = {
            "id":      match_id,
            "home":    home_bg,
            "homeEn":  home_en,
            "homeA":   h_abbr,
            "away":    away_bg,
            "awayEn":  away_en,
            "awayA":   a_abbr,
            "date":    date_str,
            "time":    time_str,
            "status":  "pending",
            "pick": {
                "bet":       bet_bg(market, sel, home_bg, away_bg),
                "betEn":     pick_raw.get("betEN", f"{home_en} Win"),
                "conf":      int(pick_raw.get("conf", 55)),
                "market":    market,
                "selection": sel,
                "odd":       str(resolved_odd),
            },
            "odds_wh": odds_wh,
            "prob": {
                "h": round(100 / float(odds_wh["h"])) if "h" in odds_wh else 50,
                "d": round(100 / float(odds_wh["x"])) if "x" in odds_wh else 25,
                "a": round(100 / float(odds_wh["a"])) if "a" in odds_wh else 25,
            },
            "aiCtx": ai_ctx,
        }
        new_matches.append(match_entry)
        print("✓")

    # ── Step 5: UEFA fixtures (fixtures only, no picks or analysis) ──────────
    print("\n🏆  Fetching UEFA fixtures…")
    all_existing_ids = existing_ids | {m["id"] for m in new_matches}
    uefa_fixtures = fetch_uefa_fixtures(odds_key, all_existing_ids, groq_client)
    if uefa_fixtures:
        print(f"  → {len(uefa_fixtures)} new UEFA fixtures added (picks needed manually)")
    new_matches.extend(uefa_fixtures)

    # ── Step 6: betBuilder selection ─────────────────────────────────────────
    # Only consider EPL matches with real odds and picks (exclude UEFA stubs)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bb_candidates = [
        m for m in existing.get("upcoming", []) + new_matches
        if m.get("pick", {}).get("conf", 0) > 0
        and m.get("odds_wh")
        and m.get("date", "") > today_str   # exclude today's matches
    ]
    bb_result = None
    if bb_candidates:
        print(f"\n  🤖  Groq betBuilder selection…", end=" ", flush=True)
        bb_result = groq_bet_builder(groq_client, bb_candidates)
        if bb_result:
            print("✓")

    # ── Step 7: Build betBuilder entry ────────────────────────────────────────
    bet_builder = None
    if bb_result:
        mid = bb_result.get("matchId", "")
        src = next((m for m in bb_candidates if m["id"] == mid), None)
        if src:
            markets = bb_result.get("markets", [])
            # Recompute totalOdd in Python (never trust AI arithmetic)
            total_odd = 1.0
            for mkt in markets:
                try:
                    total_odd *= float(mkt["odd"])
                except Exception:
                    pass
            bet_builder = {
                "matchId":     mid,
                "home":        src.get("home", ""),
                "homeEn":      src.get("homeEn", ""),
                "away":        src.get("away", ""),
                "awayEn":      src.get("awayEn", ""),
                "markets":     [
                    {
                        **MARKET_LABELS.get(mkt.get("key", ""), {
                            "market":   mkt.get("key", ""),
                            "marketEn": mkt.get("key", ""),
                        }),
                        "odd": mkt.get("odd", ""),
                    }
                    for mkt in markets
                ],
                "totalOdd":    str(round(total_odd, 2)),
                "reasoning":   "",
                "reasoningEn": "",
            }

    # ── Step 8: Update matchday.json ─────────────────────────────────────────
    # Determine label from matchday number
    gw_num  = current_matchday or existing.get("matchday", "")
    now_utc = datetime.now(timezone.utc)

    # Build date range string from new matches
    dates = sorted(set(m["date"] for m in new_matches if m.get("date")))
    if len(dates) == 1:
        date_range_en = dates[0]
        date_range_bg = dates[0]
    elif len(dates) >= 2:
        date_range_en = f"{dates[0]} – {dates[-1]}"
        date_range_bg = f"{dates[0]} – {dates[-1]}"
    else:
        date_range_en = existing.get("date_rangeEn", "")
        date_range_bg = existing.get("date_range", "")

    # Merge: keep existing pending + add new
    merged_upcoming = [m for m in existing.get("upcoming", []) if m.get("status") == "pending"]
    merged_upcoming.extend(new_matches)

    updated = {
        "updated_at":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "label":         f"Висша лига · Кръг {gw_num}",
        "labelEn":       f"Premier League · GW{gw_num}",
        "date_range":    date_range_bg,
        "date_rangeEn":  date_range_en,
        "sport":         "soccer_epl",
        "upcoming":      merged_upcoming,
    }

    if bet_builder:
        updated["betBuilder"] = bet_builder
    elif "betBuilder" in existing:
        updated["betBuilder"] = existing["betBuilder"]

    # Keep results and record
    if "results" in existing:
        updated["results"] = existing["results"]
    if "record" in existing:
        updated["record"] = existing["record"]

    with open(MATCHDAY_FILE, "w", encoding="utf-8") as f:
        json.dump(updated, f, ensure_ascii=False, indent=2)

    print(f"\n✅  Weekly setup done!")
    print(f"    Added {len(new_matches)} new matches to {MATCHDAY_FILE}")
    if bet_builder:
        print(f"    betBuilder: {bet_builder['homeEn']} vs {bet_builder['awayEn']} "
              f"({len(bet_builder['markets'])} markets, totalOdd {bet_builder['totalOdd']})")
    print()


if __name__ == "__main__":
    main()
