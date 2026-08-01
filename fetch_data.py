#!/usr/bin/env python3
"""
MC AI — data pipeline (v4).

Pulls LoL esports match data from Leaguepedia's Cargo API and writes a compact
JSON feed the app fetches through jsDelivr.

WHAT ACTUALLY BROKE RUNS #1-#3
Not the schema. Every failure carried the same message:
    "You've exceeded your rate limit. Please wait some time and try again."
Leaguepedia throttles anonymous API traffic, and GitHub-hosted runners come from
cloud IP ranges that get throttled hard. v3's column probe made it look like a
schema fault because it fired 17 rapid requests and recorded each throttled reply
as a rejected column.

v4 therefore treats throttling as the primary condition to engineer around:
  * Rate-limit responses are RETRIED with exponential backoff (30s → 8min),
    never mistaken for an error.
  * A fixed pause between every request keeps us under the limiter.
  * No column probing — that was self-inflicted request spam. One verification
    query runs first, and only a genuine schema error is reported as such.
  * maxlag is set so the wiki can ask us to slow down politely.
  * Partial progress is kept: if throttling wins late in the run, whatever was
    fetched is still written rather than thrown away.
"""

import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://lol.fandom.com/api.php"
# Fandom asks for a descriptive agent; anonymous generic agents are throttled harder.
UA = "MC-AI-pipeline/4.0 (personal LoL esports analytics; github.com/mriz16/mc-ai-data)"

LIMIT = int(os.environ.get("PAGE_LIMIT", "500"))
DAYS = int(os.environ.get("DAYS", "120"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "400"))
PAUSE = float(os.environ.get("PAUSE", "2.5"))        # seconds between requests
BACKOFF = [30, 60, 120, 240, 480]                    # rate-limit waits

SP_FIELDS = ["Link", "Team", "Champion", "Kills", "Deaths", "Assists", "CS",
             "Role", "TeamKills", "PlayerWin", "GameId", "OverviewPage", "DateTime_UTC"]
SG_FIELDS = ["GameId", "Patch", "Gamelength_Number", "DateTime_UTC"]

ROLE_MAP = {"top": "top", "toplane": "top", "jungle": "jng", "jng": "jng",
            "mid": "mid", "middle": "mid", "bot": "bot", "adc": "bot",
            "botlane": "bot", "support": "sup", "sup": "sup"}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

DIAG = {"version": 4, "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "steps": [], "outcome": "incomplete", "rate_limit_waits": 0, "requests": 0}


def note(msg, ok=True, detail=""):
    DIAG["steps"].append({"step": msg, "ok": bool(ok), "detail": str(detail)[:500]})
    print(("  [ok]   " if ok else "  [FAIL] ") + msg + ((" :: " + str(detail)[:200]) if detail else ""), flush=True)


def is_rate_limited(text):
    t = (text or "").lower()
    return "rate limit" in t or "ratelimited" in t or "exceeded your" in t


def api_get(params, tries=len(BACKOFF) + 1):
    """GET with patient backoff. Rate limiting is a wait, never an error."""
    params = dict(params)
    params.setdefault("format", "json")
    params.setdefault("maxlag", "5")
    url = API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})

    for attempt in range(tries):
        DIAG["requests"] += 1
        body, status = None, 0
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                status, body = r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read().decode("utf-8", "replace")
        except Exception as e:                                   # noqa: BLE001
            body = "EXCEPTION: %s" % e

        throttled = status in (429, 503) or is_rate_limited(body)
        if not throttled and status == 200 and body:
            try:
                data = json.loads(body)
            except ValueError:
                raise RuntimeError("non-JSON reply :: %s" % body[:200])
            if "error" in data:
                info = data["error"].get("info", "") or data["error"].get("code", "")
                if is_rate_limited(info) or data["error"].get("code") == "maxlag":
                    throttled = True
                else:
                    raise RuntimeError("Cargo error :: %s" % info[:220])
            if not throttled:
                time.sleep(PAUSE)
                return data

        if attempt < tries - 1:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
            DIAG["rate_limit_waits"] += 1
            print("    throttled (HTTP %s) — waiting %ds then retrying" % (status, wait), flush=True)
            time.sleep(wait)

    raise RuntimeError("still throttled after %d attempts" % tries)


def cargo(table, fields, where="", offset=0, limit=None):
    p = {"action": "cargoquery", "tables": table, "fields": ",".join(fields),
         "limit": str(limit or LIMIT), "offset": str(offset)}
    if where:
        p["where"] = where
    return api_get(p).get("cargoquery", [])


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
        if compact:
            json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
        else:
            json.dump(obj, f, indent=2, ensure_ascii=False)


def run():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    DIAG["since"] = since
    print("MC AI pipeline v4 — %sd window since %s (pause %.1fs)\n" % (DAYS, since, PAUSE), flush=True)

    # One verification query. Only a real schema error is treated as such.
    try:
        cargo("ScoreboardPlayers", SP_FIELDS, limit=1)
        note("schema verified — all %d player columns valid" % len(SP_FIELDS))
    except RuntimeError as e:
        note("schema check failed", False, e)
        DIAG["outcome"] = "schema_error"
        DIAG["schema_error"] = str(e)[:400]
        return

    where_sp = "ScoreboardPlayers.DateTime_UTC >= '%s 00:00:00'" % since
    where_sg = "ScoreboardGames.DateTime_UTC >= '%s 00:00:00'" % since

    print("\nFetching player rows…", flush=True)
    players = []
    complete_sp = page_all("ScoreboardPlayers", SP_FIELDS, where_sp, "SP", players)
    DIAG["player_rows"] = len(players)

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
        if not player or not gid:
            continue
        k = gid + "|" + player
        if k in seen:
            continue
        seen.add(k)
        meta = games.get(gid, {})
        page = str(pick(p, "OverviewPage"))
        league = page.split("/")[0] if "/" in page else page
        role = str(pick(p, "Role")).strip().lower()
        glen = num(pick(meta, "Gamelength Number", "Gamelength_Number", default=0), 0)
        rows.append([
            player, str(pick(p, "Team")).strip(), str(pick(p, "Champion")).strip(),
            num(pick(p, "Kills", default=0)), num(pick(p, "Deaths", default=0)),
            num(pick(p, "Assists", default=0)), num(pick(p, "CS", default=0)),
            ROLE_MAP.get(role, role), num(pick(p, "TeamKills", default=0)),
            1 if str(pick(p, "PlayerWin")) in ("Yes", "1", "true", "True") else 0,
            gid, league, str(pick(p, "DateTime UTC", "DateTime_UTC"))[:10],
            num(pick(meta, "Patch", default=0), 0),
            int(round(glen * 60)) if glen else 0,
        ])

    if not rows:
        DIAG["outcome"] = "zero_rows"
        note("no rows built", False)
        return

    rows.sort(key=lambda r: r[12])
    leagues = {}
    for r in rows:
        leagues[r[11]] = leagues.get(r[11], 0) + 1

    payload = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "since": since, "newest": rows[-1][12], "count": len(rows),
               "complete": complete_sp,
               "leagues": sorted(leagues.items(), key=lambda kv: -kv[1]),
               "fields": OUT_FIELDS, "rows": rows}
    write("data/latest.json", payload)
    DIAG["outcome"] = "ok" if complete_sp else "ok_partial"
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
    print("\noutcome=%s | requests=%d | throttle waits=%d"
          % (DIAG["outcome"], DIAG["requests"], DIAG["rate_limit_waits"]), flush=True)
    sys.exit(0)
