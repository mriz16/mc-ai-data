#!/usr/bin/env python3
"""
MC AI — data pipeline (v3).

Pulls LoL esports match data from Leaguepedia's Cargo API and writes a compact
JSON feed that the app fetches through jsDelivr.

v3 exists because runs #1 and #2 failed fast and the only way to see why was to
read Action logs by hand. This version NEVER fails the job during diagnosis:
it writes everything it learns to data/diagnostic.json and commits it, so the
findings are readable straight from the repo.

Ladder it walks, recording each result:
  1. Is the MediaWiki API reachable at all?          (action=query&meta=siteinfo)
  2. Does Cargo respond for this table?              (fields=_pageName)
  3. Which columns does the wiki actually accept?    (each tested alone)
  4. Fetch, merge and write the feed.
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
UA = "MC-AI-pipeline/3.0 (personal analytics project)"
LIMIT = 500

DAYS = int(os.environ.get("DAYS", "120"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "400"))

SP_CORE = ["Link", "Team", "Champion", "Kills", "Deaths", "Assists",
           "Role", "GameId", "OverviewPage", "DateTime_UTC"]
SP_OPTIONAL = ["CS", "TeamKills", "PlayerWin"]
SG_CORE = ["GameId"]
SG_OPTIONAL = ["Patch", "Gamelength_Number", "DateTime_UTC"]

ROLE_MAP = {"top": "top", "toplane": "top", "jungle": "jng", "jng": "jng",
            "mid": "mid", "middle": "mid", "bot": "bot", "adc": "bot",
            "botlane": "bot", "support": "sup", "sup": "sup"}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

DIAG = {"started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "api": API, "steps": [], "columns": {}, "outcome": "incomplete"}


def note(step, ok, detail=""):
    DIAG["steps"].append({"step": step, "ok": bool(ok), "detail": str(detail)[:600]})
    print(("  [ok]   " if ok else "  [FAIL] ") + step + ((" :: " + str(detail)[:220]) if detail else ""), flush=True)


def raw_get(params, timeout=45):
    url = API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, body, url
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace"), url
    except Exception as e:                                    # noqa: BLE001
        return 0, "EXCEPTION: %s" % e, url


def cargo(table, fields, where="", offset=0, limit=LIMIT):
    params = {"action": "cargoquery", "tables": table, "fields": ",".join(fields),
              "limit": str(limit), "offset": str(offset), "format": "json"}
    if where:
        params["where"] = where
    status, body, url = raw_get(params)
    if status != 200:
        raise RuntimeError("HTTP %s :: %s" % (status, body[:220]))
    try:
        data = json.loads(body)
    except ValueError:
        raise RuntimeError("non-JSON response :: %s" % body[:220])
    if "error" in data:
        err = data["error"]
        raise RuntimeError(err.get("info") or err.get("code") or json.dumps(err)[:220])
    return data.get("cargoquery", [])


def probe_columns(table, core, optional):
    candidate = list(core) + list(optional)
    try:
        cargo(table, candidate, limit=1)
        note("%s: all %d columns accepted" % (table, len(candidate)), True)
        DIAG["columns"][table] = {"accepted": candidate, "rejected": []}
        return candidate
    except RuntimeError as e:
        note("%s: combined query rejected" % table, False, e)

    good, bad = [], {}
    for f in candidate:
        try:
            cargo(table, [f], limit=1)
            good.append(f)
        except RuntimeError as e:
            bad[f] = str(e)[:200]
        time.sleep(0.2)
    DIAG["columns"][table] = {"accepted": good, "rejected": bad}
    note("%s: accepted=%s" % (table, ",".join(good) or "none"), bool(good))
    note("%s: rejected=%s" % (table, ",".join(bad) or "none"), not bad)
    return good


def page_all(table, fields, where, label):
    rows, offset = [], 0
    for _ in range(MAX_PAGES):
        for attempt in range(4):
            try:
                batch = cargo(table, fields, where, offset=offset)
                break
            except RuntimeError as e:
                wait = 6 * (attempt + 1)
                print("    retry %d :: %s (waiting %ds)" % (attempt + 1, str(e)[:120], wait), flush=True)
                time.sleep(wait)
        else:
            raise RuntimeError("%s failed repeatedly at offset %d" % (label, offset))
        rows.extend(i.get("title", {}) for i in batch)
        print("    %s offset %6d -> %3d (total %d)" % (label, offset, len(batch), len(rows)), flush=True)
        if len(batch) < LIMIT:
            break
        offset += LIMIT
        time.sleep(0.4)
    return rows


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


def write_diag():
    os.makedirs("data", exist_ok=True)
    DIAG["finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open("data/diagnostic.json", "w", encoding="utf-8") as f:
        json.dump(DIAG, f, indent=2, ensure_ascii=False)
    print("\nWrote data/diagnostic.json (outcome=%s)" % DIAG["outcome"], flush=True)


def run():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    DIAG["since"] = since
    print("MC AI pipeline v3 — %sd window since %s\n" % (DAYS, since), flush=True)

    # 1. reachability
    status, body, _ = raw_get({"action": "query", "meta": "siteinfo", "format": "json"})
    DIAG["siteinfo_status"] = status
    DIAG["siteinfo_sample"] = body[:300]
    note("MediaWiki API reachable (HTTP %s)" % status, status == 200, body[:200] if status != 200 else "")
    if status != 200:
        DIAG["outcome"] = "api_unreachable"
        return

    # 2. does Cargo answer for these tables
    for tbl in ("ScoreboardPlayers", "ScoreboardGames"):
        try:
            cargo(tbl, ["_pageName"], limit=1)
            note("Cargo responds for %s" % tbl, True)
        except RuntimeError as e:
            note("Cargo failed for %s" % tbl, False, e)
            DIAG["outcome"] = "cargo_table_unavailable"

    # 3. column probe
    sp = probe_columns("ScoreboardPlayers", SP_CORE, SP_OPTIONAL)
    sg = probe_columns("ScoreboardGames", SG_CORE, SG_OPTIONAL)

    missing = [c for c in SP_CORE if c not in sp]
    if missing:
        DIAG["outcome"] = "missing_core_columns"
        DIAG["missing_core"] = missing
        note("cannot build feed, missing core columns: %s" % ",".join(missing), False)
        return

    # 4. fetch
    date_col = "DateTime_UTC" if "DateTime_UTC" in sp else None
    where_sp = ("ScoreboardPlayers.%s >= '%s 00:00:00'" % (date_col, since)) if date_col else ""
    players = page_all("ScoreboardPlayers", sp, where_sp, "SP")
    DIAG["player_rows"] = len(players)
    if players:
        DIAG["sample_player_row"] = {k: str(v)[:60] for k, v in list(players[0].items())[:20]}

    games = {}
    if len(sg) > 1:
        sg_date = "DateTime_UTC" if "DateTime_UTC" in sg else None
        where_sg = ("ScoreboardGames.%s >= '%s 00:00:00'" % (sg_date, since)) if sg_date else ""
        for g in page_all("ScoreboardGames", sg, where_sg, "SG"):
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
        note("query succeeded but produced no rows", False)
        return

    rows.sort(key=lambda r: r[12])
    leagues = {}
    for r in rows:
        leagues[r[11]] = leagues.get(r[11], 0) + 1
    payload = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "since": since, "newest": rows[-1][12], "count": len(rows),
               "leagues": sorted(leagues.items(), key=lambda kv: -kv[1]),
               "fields": OUT_FIELDS, "rows": rows}
    os.makedirs("data", exist_ok=True)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
    DIAG["outcome"] = "ok"
    DIAG["written"] = {"rows": len(rows), "newest": payload["newest"],
                       "mb": round(os.path.getsize("data/latest.json") / 1e6, 2),
                       "leagues": payload["leagues"][:15]}
    note("wrote data/latest.json — %d rows, newest %s" % (len(rows), payload["newest"]), True)


if __name__ == "__main__":
    try:
        run()
    except Exception:                                          # noqa: BLE001
        DIAG["outcome"] = "exception"
        DIAG["traceback"] = traceback.format_exc()[-2500:]
        print(DIAG["traceback"], file=sys.stderr)
    write_diag()
    # Always exit 0 so the diagnostic gets committed and can be read from the repo.
    sys.exit(0)
