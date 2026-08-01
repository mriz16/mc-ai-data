#!/usr/bin/env python3
"""
MC AI — data pipeline (Riot esports API).

WHY THIS SOURCE
  * Oracle's Elixir: stale — stopped 2026-07-28 for every league. Automating it
    would only automate the staleness.
  * Leaguepedia Cargo: throttles anonymous cloud IPs to zero rows (run #4 spent
    15 minutes in backoff and retrieved nothing).
  * Riot esports API: the endpoints lolesports.com itself is built on. Completed
    games land within minutes, and GitHub runners can reach it.

FLOW
  getLeagues            -> league id/name/region
  getCompletedEvents    -> recently finished matches, each with its games
  livestats window      -> final per-player stats for one game
Rows land in the same compact schema the app already reads.

The run always writes data/diagnostic.json, including one RAW sample of each
response shape, so any mapping problem is visible without reading CI logs.
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

# Public key used by lolesports.com's own web client.
API_KEY = os.environ.get("RIOT_ESPORTS_KEY", "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z")
ESPORTS = "https://esports-api.lolesports.com/persisted/gw/"
FEED = "https://feed.lolesports.com/livestats/v1/"
UA = "MC-AI-pipeline/7.0"

DAYS = int(os.environ.get("DAYS", "30"))
MAX_GAMES = int(os.environ.get("MAX_GAMES", "600"))
PAUSE = float(os.environ.get("PAUSE", "0.35"))

ROLE_MAP = {"top": "top", "jungle": "jng", "jng": "jng", "mid": "mid",
            "middle": "mid", "bottom": "bot", "bot": "bot", "adc": "bot",
            "support": "sup", "sup": "sup"}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

DIAG = {"version": 7, "source": "riot_esports",
        "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "steps": [], "outcome": "incomplete", "requests": 0, "samples": {}}


def note(msg, ok=True, detail=""):
    DIAG["steps"].append({"step": msg, "ok": bool(ok), "detail": str(detail)[:500]})
    print(("  [ok]   " if ok else "  [FAIL] ") + msg + ((" :: " + str(detail)[:220]) if detail else ""), flush=True)


def get(url, tries=4):
    for attempt in range(tries):
        DIAG["requests"] += 1
        req = urllib.request.Request(url, headers={
            "x-api-key": API_KEY, "User-Agent": UA, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                body = r.read().decode("utf-8", "replace")
            time.sleep(PAUSE)
            return json.loads(body)
        except urllib.error.HTTPError as e:
            txt = e.read().decode("utf-8", "replace")[:200]
            if e.code in (429, 500, 502, 503) and attempt < tries - 1:
                wait = 5 * (attempt + 1)
                print("    HTTP %s — waiting %ds" % (e.code, wait), flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError("HTTP %s :: %s" % (e.code, txt))
        except Exception as e:                                  # noqa: BLE001
            if attempt < tries - 1:
                time.sleep(4 * (attempt + 1))
                continue
            raise RuntimeError(str(e)[:200])
    raise RuntimeError("exhausted retries")


def sample(key, obj):
    if key not in DIAG["samples"]:
        DIAG["samples"][key] = json.loads(json.dumps(obj)[:1500] + ("" if len(json.dumps(obj)) < 1500 else ""))\
            if False else json.dumps(obj)[:1500]


def leagues():
    d = get(ESPORTS + "getLeagues?hl=en-US")
    out = {}
    for lg in d.get("data", {}).get("leagues", []):
        out[str(lg.get("id"))] = lg.get("slug") or lg.get("name") or ""
    sample("getLeagues", d.get("data", {}).get("leagues", [])[:2])
    return out


def completed_events(since_dt):
    """Page backwards through completed events until we pass the window."""
    events, token, pages = [], None, 0
    while pages < 40:
        url = ESPORTS + "getCompletedEvents?hl=en-US" + (("&pageToken=" + urllib.parse.quote(token)) if token else "")
        d = get(url)
        data = d.get("data", {}).get("schedule", {})
        batch = data.get("events", []) or []
        if pages == 0:
            sample("getCompletedEvents", batch[:1])
        if not batch:
            break
        events.extend(batch)
        oldest = min((e.get("startTime") or "9999") for e in batch)
        token = (data.get("pages") or {}).get("older")
        pages += 1
        if oldest[:10] < since_dt or not token:
            break
    return events


def window(game_id):
    return get(FEED + "window/" + str(game_id))


def build_rows(since):
    lg_map = leagues()
    note("league catalogue: %d entries" % len(lg_map), bool(lg_map))

    evs = completed_events(since)
    note("completed events pulled: %d" % len(evs), bool(evs))

    # collect (gameId, league, date, teams) for finished games inside the window
    targets = []
    for e in evs:
        date = (e.get("startTime") or "")[:10]
        if date < since:
            continue
        league = ((e.get("league") or {}).get("slug")
                  or (e.get("league") or {}).get("name") or "")
        match = e.get("match") or {}
        teams = match.get("teams") or []
        tnames = [t.get("name") or t.get("code") or "" for t in teams]
        for g in (match.get("games") or []):
            if str(g.get("state", "")).lower() not in ("completed", "finished"):
                continue
            targets.append({"gameId": str(g.get("id")), "league": league,
                            "date": date, "teams": tnames})
    note("completed games in window: %d" % len(targets), bool(targets))
    DIAG["games_found"] = len(targets)
    if not targets:
        return []

    targets = targets[:MAX_GAMES]
    rows, ok_games, failed = [], 0, 0
    for i, t in enumerate(targets):
        try:
            w = window(t["gameId"])
        except RuntimeError as e:
            failed += 1
            if failed <= 3:
                note("window failed for game %s" % t["gameId"], False, e)
            continue
        if i == 0:
            sample("window", {k: w.get(k) for k in ("esportsGameId", "gameMetadata")})

        meta = w.get("gameMetadata") or {}
        frames = w.get("frames") or []
        if not frames:
            failed += 1
            continue
        last = frames[-1]
        patch = meta.get("patchVersion") or ""
        try:
            patch_f = float(".".join(patch.split(".")[:2])) if patch else 0
        except ValueError:
            patch_f = 0

        # participant metadata carries names + roles, keyed by participantId
        pmeta = {}
        for side in ("blueTeamMetadata", "redTeamMetadata"):
            for pm in ((meta.get(side) or {}).get("participantMetadata") or []):
                pmeta[pm.get("participantId")] = {
                    "name": (pm.get("summonerName") or "").split(" ", 1)[-1].strip(),
                    "role": ROLE_MAP.get(str(pm.get("role", "")).lower(), str(pm.get("role", "")).lower()),
                    "champ": pm.get("championId") or "",
                    "side": "blue" if side.startswith("blue") else "red",
                }

        blue = last.get("blueTeam") or {}
        red = last.get("redTeam") or {}
        team_of = {"blue": (t["teams"][0] if len(t["teams"]) > 0 else ""),
                   "red": (t["teams"][1] if len(t["teams"]) > 1 else "")}
        kills_of = {"blue": blue.get("totalKills", 0) or 0, "red": red.get("totalKills", 0) or 0}
        win_of = {"blue": 1 if kills_of["blue"] >= kills_of["red"] else 0,
                  "red": 1 if kills_of["red"] > kills_of["blue"] else 0}
        # participants live on each side's object
        glen = 0
        try:
            t0 = frames[0].get("rfc460Timestamp", "")
            t1 = last.get("rfc460Timestamp", "")
            if t0 and t1:
                f = "%Y-%m-%dT%H:%M:%S"
                glen = int((datetime.strptime(t1[:19], f) - datetime.strptime(t0[:19], f)).total_seconds())
        except Exception:                                        # noqa: BLE001
            glen = 0

        for side_key, side_obj in (("blue", blue), ("red", red)):
            for p in (side_obj.get("participants") or []):
                pid = p.get("participantId")
                m = pmeta.get(pid, {})
                rows.append([
                    m.get("name", "") or ("P%s" % pid),
                    team_of[side_key],
                    m.get("champ", ""),
                    p.get("kills", 0) or 0,
                    p.get("deaths", 0) or 0,
                    p.get("assists", 0) or 0,
                    p.get("creepScore", 0) or 0,
                    m.get("role", ""),
                    kills_of[side_key],
                    win_of[side_key],
                    t["gameId"],
                    t["league"],
                    t["date"],
                    patch_f,
                    glen,
                ])
        ok_games += 1
        if ok_games % 25 == 0:
            print("    %d/%d games processed (%d rows)" % (ok_games, len(targets), len(rows)), flush=True)

    DIAG["games_ok"] = ok_games
    DIAG["games_failed"] = failed
    note("processed %d games (%d failed) -> %d player rows" % (ok_games, failed, len(rows)), bool(rows))
    return rows


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
    print("MC AI pipeline v7 (Riot esports) — %sd window since %s\n" % (DAYS, since), flush=True)

    rows = build_rows(since)
    if not rows:
        DIAG["outcome"] = "no_rows"
        return

    rows.sort(key=lambda r: r[12])
    lgs = {}
    for r in rows:
        lgs[r[11]] = lgs.get(r[11], 0) + 1
    payload = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "source": "riot_esports", "since": since, "newest": rows[-1][12],
               "count": len(rows), "complete": True,
               "leagues": sorted(lgs.items(), key=lambda kv: -kv[1]),
               "fields": OUT_FIELDS, "rows": rows}
    write("data/latest.json", payload)
    DIAG["outcome"] = "ok"
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
    print("\noutcome=%s | requests=%d" % (DIAG["outcome"], DIAG["requests"]), flush=True)
    sys.exit(0)
