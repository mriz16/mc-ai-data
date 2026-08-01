#!/usr/bin/env python3
"""
MC AI — data pipeline (v5, authenticated).

WHY v5
Run #4's log settled it:
    [ok]  schema verified — all 13 player columns valid
    throttled (HTTP 200) — waiting 30s / 60s / 120s / 240s / 480s
    [FAIL] SP stopped at offset 0 :: still throttled after 6 attempts
The columns were always correct. Fandom answers HTTP 200 with a rate-limit
message in the body and does it on EVERY anonymous request from a cloud IP —
GitHub runners share addresses whose anonymous quota is permanently spent.
Waiting cannot fix that; 15 minutes of backoff produced zero rows.

Leaguepedia applies rate limits PER ACCOUNT, so this version logs in with a
Fandom bot password before querying. Anonymous mode still works (useful from a
home IP) but warns that it will likely be throttled on CI.

Credentials come from repo secrets, never the source:
    LP_USERNAME   e.g.  YourName@mcai-bot
    LP_PASSWORD   the bot password string
Create them at:  https://lol.fandom.com/wiki/Special:BotPasswords
"""

import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from datetime import datetime, timedelta, timezone

API = "https://lol.fandom.com/api.php"
UA = "MC-AI-pipeline/5.0 (personal LoL esports analytics; github.com/mriz16/mc-ai-data)"

LIMIT = int(os.environ.get("PAGE_LIMIT", "500"))
DAYS = int(os.environ.get("DAYS", "120"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "400"))
PAUSE = float(os.environ.get("PAUSE", "1.2"))
BACKOFF = [20, 45, 90, 180, 300]

SP_FIELDS = ["Link", "Team", "Champion", "Kills", "Deaths", "Assists", "CS",
             "Role", "TeamKills", "PlayerWin", "GameId", "OverviewPage", "DateTime_UTC"]
SG_FIELDS = ["GameId", "Patch", "Gamelength_Number", "DateTime_UTC"]

ROLE_MAP = {"top": "top", "toplane": "top", "jungle": "jng", "jng": "jng",
            "mid": "mid", "middle": "mid", "bot": "bot", "adc": "bot",
            "botlane": "bot", "support": "sup", "sup": "sup"}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

DIAG = {"version": 5, "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "steps": [], "outcome": "incomplete", "authenticated": False,
        "requests": 0, "rate_limit_waits": 0}

OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
OPENER.addheaders = [("User-Agent", UA), ("Accept", "application/json")]


def note(msg, ok=True, detail=""):
    DIAG["steps"].append({"step": msg, "ok": bool(ok), "detail": str(detail)[:500]})
    print(("  [ok]   " if ok else "  [FAIL] ") + msg + ((" :: " + str(detail)[:220]) if detail else ""), flush=True)


def throttled_text(t):
    t = (t or "").lower()
    return "rate limit" in t or "ratelimited" in t or "exceeded your" in t


def http(params, data=None, timeout=90):
    """One request through the shared (cookie-carrying) opener."""
    DIAG["requests"] += 1
    url = API + "?" + urllib.parse.urlencode(params)
    body = urllib.parse.urlencode(data).encode() if data else None
    try:
        with OPENER.open(urllib.request.Request(url, data=body), timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:                                       # noqa: BLE001
        return 0, "EXCEPTION: %s" % e


def api(params, data=None, tries=len(BACKOFF) + 1):
    params = dict(params)
    params.setdefault("format", "json")
    params.setdefault("maxlag", "5")
    for attempt in range(tries):
        status, body = http(params, data)
        throttle = status in (429, 503) or throttled_text(body)
        if status == 200 and not throttle:
            try:
                parsed = json.loads(body)
            except ValueError:
                raise RuntimeError("non-JSON reply :: %s" % body[:200])
            if "error" in parsed:
                info = parsed["error"].get("info", "") or parsed["error"].get("code", "")
                if throttled_text(info) or parsed["error"].get("code") == "maxlag":
                    throttle = True
                else:
                    raise RuntimeError("API error :: %s" % info[:220])
            if not throttle:
                time.sleep(PAUSE)
                return parsed
        if attempt < tries - 1:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
            DIAG["rate_limit_waits"] += 1
            print("    throttled — waiting %ds" % wait, flush=True)
            time.sleep(wait)
    raise RuntimeError("still throttled after %d attempts" % tries)


def login():
    """Log in with a Fandom bot password. Returns True on success."""
    user = os.environ.get("LP_USERNAME", "").strip()
    pwd = os.environ.get("LP_PASSWORD", "").strip()
    if not user or not pwd:
        note("no credentials set — running anonymously (expect throttling on CI)", False)
        DIAG["outcome"] = "no_credentials"
        return False
    tok = api({"action": "query", "meta": "tokens", "type": "login"})
    lgtoken = tok.get("query", {}).get("tokens", {}).get("logintoken")
    if not lgtoken:
        note("could not obtain login token", False, json.dumps(tok)[:200])
        return False
    res = api({"action": "login"}, data={"lgname": user, "lgpassword": pwd, "lgtoken": lgtoken})
    result = res.get("login", {}).get("result", "")
    if result != "Success":
        note("login rejected: %s" % result, False, json.dumps(res.get("login", {}))[:250])
        return False
    note("logged in as %s" % res["login"].get("lgusername", user))
    DIAG["authenticated"] = True
    return True


def cargo(table, fields, where="", offset=0, limit=None):
    p = {"action": "cargoquery", "tables": table, "fields": ",".join(fields),
         "limit": str(limit or LIMIT), "offset": str(offset)}
    if where:
        p["where"] = where
    return api(p).get("cargoquery", [])


def page_all(table, fields, where, label, sink):
    offset = 0
    for _ in range(MAX_PAGES):
        try:
            batch = cargo(table, fields, where, offset=offset)
        except RuntimeError as e:
            note("%s stopped at offset %d — keeping partial data" % (label, offset), False, e)
            return False
        sink.extend(i.get("title", {}) for i in batch)
        print("    %s offset %6d -> %3d rows (total %d)" % (label, offset, len(batch), len(sink)), flush=True)
        if len(batch) < LIMIT:
            return True
        offset += LIMIT
    return True


def num(v, d=0):
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except (TypeError, ValueError):
        return d


def pick(d, *names, default=""):
    for n in names:
        if n in d and d[n] not in (None, ""):
            return d[n]
    return default


def write(path, obj, compact=True):
    os.makedirs("data", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":")) if compact else json.dump(obj, f, indent=2, ensure_ascii=False)


def run():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    DIAG["since"] = since
    print("MC AI pipeline v5 — %sd window since %s\n" % (DAYS, since), flush=True)

    login()   # continues anonymously if credentials are absent

    try:
        cargo("ScoreboardPlayers", SP_FIELDS, limit=1)
        note("schema verified — %d player columns valid" % len(SP_FIELDS))
    except RuntimeError as e:
        note("schema check failed", False, e)
        DIAG["outcome"] = "blocked_before_fetch"
        return

    where_sp = "ScoreboardPlayers.DateTime_UTC >= '%s 00:00:00'" % since
    where_sg = "ScoreboardGames.DateTime_UTC >= '%s 00:00:00'" % since

    print("\nFetching player rows…", flush=True)
    players = []
    complete = page_all("ScoreboardPlayers", SP_FIELDS, where_sp, "SP", players)
    DIAG["player_rows"] = len(players)
    if not players:
        DIAG["outcome"] = "throttled_no_data" if not DIAG["authenticated"] else "no_rows"
        note("no player rows retrieved", False)
        return

    print("\nFetching game metadata…", flush=True)
    graw = []
    page_all("ScoreboardGames", SG_FIELDS, where_sg, "SG", graw)
    games = {}
    for g in graw:
        gid = str(pick(g, "GameId"))
        if gid:
            games[gid] = g
    DIAG["game_rows"] = len(games)

    rows, seen = [], set()
    for p in players:
        player = str(pick(p, "Link")).strip()
        gid = str(pick(p, "GameId"))
        if not player or not gid or (gid + "|" + player) in seen:
            continue
        seen.add(gid + "|" + player)
        meta = games.get(gid, {})
        page = str(pick(p, "OverviewPage"))
        role = str(pick(p, "Role")).strip().lower()
        glen = num(pick(meta, "Gamelength Number", "Gamelength_Number", default=0), 0)
        rows.append([
            player, str(pick(p, "Team")).strip(), str(pick(p, "Champion")).strip(),
            num(pick(p, "Kills", default=0)), num(pick(p, "Deaths", default=0)),
            num(pick(p, "Assists", default=0)), num(pick(p, "CS", default=0)),
            ROLE_MAP.get(role, role), num(pick(p, "TeamKills", default=0)),
            1 if str(pick(p, "PlayerWin")) in ("Yes", "1", "true", "True") else 0,
            gid, page.split("/")[0] if "/" in page else page,
            str(pick(p, "DateTime UTC", "DateTime_UTC"))[:10],
            num(pick(meta, "Patch", default=0), 0),
            int(round(glen * 60)) if glen else 0,
        ])

    rows.sort(key=lambda r: r[12])
    leagues = {}
    for r in rows:
        leagues[r[11]] = leagues.get(r[11], 0) + 1
    payload = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "since": since, "newest": rows[-1][12], "count": len(rows),
               "complete": complete, "authenticated": DIAG["authenticated"],
               "leagues": sorted(leagues.items(), key=lambda kv: -kv[1]),
               "fields": OUT_FIELDS, "rows": rows}
    write("data/latest.json", payload)
    DIAG["outcome"] = "ok" if complete else "ok_partial"
    DIAG["written"] = {"rows": len(rows), "newest": payload["newest"],
                       "mb": round(os.path.getsize("data/latest.json") / 1e6, 2),
                       "leagues": payload["leagues"][:15]}
    note("wrote data/latest.json — %d rows, newest %s" % (len(rows), payload["newest"]))


if __name__ == "__main__":
    try:
        run()
    except Exception:                                            # noqa: BLE001
        DIAG["outcome"] = "exception"
        DIAG["traceback"] = traceback.format_exc()[-2500:]
        print(DIAG["traceback"], file=sys.stderr)
    DIAG["finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write("data/diagnostic.json", DIAG, compact=False)
    print("\noutcome=%s | authenticated=%s | requests=%d | waits=%d"
          % (DIAG["outcome"], DIAG["authenticated"], DIAG["requests"], DIAG["rate_limit_waits"]), flush=True)
    sys.exit(0)
