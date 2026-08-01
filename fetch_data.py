#!/usr/bin/env python3
"""
MC AI — data pipeline (v2, self-diagnosing).

Pulls recent LoL esports match data from Leaguepedia's Cargo API and writes a
compact JSON feed that the app fetches through jsDelivr.

Why v2: run #1 failed in 6 seconds, which means Leaguepedia answered and REJECTED
the query — a bad column or bad join, not a network problem. This version removes
the failure modes rather than guessing which one it was:

  * No SQL join. Two independent queries (ScoreboardPlayers for player rows,
    ScoreboardGames for patch/gamelength) merged locally on GameId.
  * Field probing. If a query is rejected, each column is retested on its own so
    the log names the exact offender instead of failing generically.
  * Graceful degradation. Optional columns that the wiki rejects are dropped and
    the run continues; only core columns are mandatory.
  * The full request URL and raw error text are printed.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://lol.fandom.com/api.php"
UA = "MC-AI-pipeline/2.0 (personal analytics project)"
LIMIT = 500

DAYS = int(os.environ.get("DAYS", "120"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "400"))

# Mandatory columns — the run stops loudly if the wiki won't serve these.
SP_CORE = ["Link", "Team", "Champion", "Kills", "Deaths", "Assists",
           "Role", "GameId", "OverviewPage", "DateTime_UTC"]
# Nice-to-have columns — dropped with a warning if rejected.
SP_OPTIONAL = ["CS", "TeamKills", "PlayerWin"]

SG_CORE = ["GameId"]
SG_OPTIONAL = ["Patch", "Gamelength_Number"]

ROLE_MAP = {"top": "top", "toplane": "top", "jungle": "jng", "jng": "jng",
            "mid": "mid", "middle": "mid", "bot": "bot", "adc": "bot",
            "botlane": "bot", "support": "sup", "sup": "sup"}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]


def request(params):
    url = API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8")), url
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]
        raise RuntimeError("HTTP %s from Leaguepedia. Body: %s" % (e.code, body))


def cargo(table, fields, where, offset=0, limit=LIMIT):
    params = {
        "action": "cargoquery",
        "tables": table,
        "fields": ",".join(fields),
        "limit": str(limit),
        "offset": str(offset),
        "format": "json",
    }
    if where:
        params["where"] = where
    data, url = request(params)
    if "error" in data:
        info = data["error"].get("info") or data["error"].get("code") or json.dumps(data["error"])[:300]
        raise RuntimeError("Cargo rejected the query: %s\n  URL: %s" % (info, url))
    return data.get("cargoquery", [])


def usable_fields(table, core, optional):
    """Return the columns this wiki actually accepts, naming any casualties."""
    candidate = list(core) + list(optional)
    try:
        cargo(table, candidate, "", limit=1)
        print("  [%s] all %d columns accepted" % (table, len(candidate)), flush=True)
        return candidate
    except RuntimeError as e:
        print("  [%s] combined query rejected — probing columns individually" % table, flush=True)
        print("    %s" % str(e).splitlines()[0], flush=True)

    good, bad = [], []
    for f in candidate:
        try:
            cargo(table, [f], "", limit=1)
            good.append(f)
        except RuntimeError:
            bad.append(f)
        time.sleep(0.25)

    print("    accepted: %s" % (", ".join(good) or "none"), flush=True)
    print("    REJECTED: %s" % (", ".join(bad) or "none"), flush=True)

    missing = [f for f in core if f in bad]
    if missing:
        print("FATAL: required column(s) unavailable: %s" % ", ".join(missing), file=sys.stderr)
        print("       Send this log line and the columns will be remapped.", file=sys.stderr)
        sys.exit(1)
    return good


def page_all(table, fields, where, label):
    rows, offset = [], 0
    for _ in range(MAX_PAGES):
        for attempt in range(4):
            try:
                batch = cargo(table, fields, where, offset=offset)
                break
            except RuntimeError as e:
                wait = 8 * (attempt + 1)
                print("    retry %d (%s) waiting %ds"
                      % (attempt + 1, str(e).splitlines()[0][:110], wait), flush=True)
                time.sleep(wait)
        else:
            print("FATAL: %s failed repeatedly at offset %d" % (label, offset), file=sys.stderr)
            sys.exit(1)

        rows.extend(item.get("title", {}) for item in batch)
        print("    %s offset %6d -> %3d rows (total %d)" % (label, offset, len(batch), len(rows)), flush=True)
        if len(batch) < LIMIT:
            break
        offset += LIMIT
        time.sleep(0.5)
    return rows


def num(v, default=0):
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except (TypeError, ValueError):
        return default


def pick(d, *names, default=""):
    for n in names:
        if n in d and d[n] not in (None, ""):
            return d[n]
    return default


def main():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    print("MC AI pipeline — %sd window (since %s)\n" % (DAYS, since), flush=True)

    print("Checking schema…", flush=True)
    sp_fields = usable_fields("ScoreboardPlayers", SP_CORE, SP_OPTIONAL)
    sg_fields = usable_fields("ScoreboardGames", SG_CORE, SG_OPTIONAL)
    print("", flush=True)

    where_sp = "ScoreboardPlayers.DateTime_UTC >= '%s 00:00:00'" % since
    where_sg = "ScoreboardGames.DateTime_UTC >= '%s 00:00:00'" % since

    print("Fetching player rows…", flush=True)
    players = page_all("ScoreboardPlayers", sp_fields, where_sp, "SP")

    games = {}
    if len(sg_fields) > 1:
        print("\nFetching game metadata…", flush=True)
        for g in page_all("ScoreboardGames", sg_fields, where_sg, "SG"):
            gid = str(pick(g, "GameId"))
            if gid:
                games[gid] = g
    else:
        print("\nNo usable game-metadata columns; patch/length will be blank.", flush=True)

    rows, seen = [], set()
    for p in players:
        player = str(pick(p, "Link")).strip()
        gid = str(pick(p, "GameId"))
        if not player or not gid:
            continue
        key = gid + "|" + player
        if key in seen:
            continue
        seen.add(key)

        meta = games.get(gid, {})
        page = str(pick(p, "OverviewPage"))
        league = page.split("/")[0] if "/" in page else page
        role = str(pick(p, "Role")).strip().lower()
        win = str(pick(p, "PlayerWin"))
        glen_min = num(pick(meta, "Gamelength Number", "Gamelength_Number", default=0), 0)

        rows.append([
            player,
            str(pick(p, "Team")).strip(),
            str(pick(p, "Champion")).strip(),
            num(pick(p, "Kills", default=0)),
            num(pick(p, "Deaths", default=0)),
            num(pick(p, "Assists", default=0)),
            num(pick(p, "CS", default=0)),
            ROLE_MAP.get(role, role),
            num(pick(p, "TeamKills", default=0)),
            1 if win in ("Yes", "1", "true", "True") else 0,
            gid,
            league,
            str(pick(p, "DateTime UTC", "DateTime_UTC"))[:10],
            num(pick(meta, "Patch", default=0), 0),
            int(round(glen_min * 60)) if glen_min else 0,
        ])

    if not rows:
        print("FATAL: zero rows built — refusing to overwrite the feed.", file=sys.stderr)
        sys.exit(1)

    rows.sort(key=lambda r: r[12])
    leagues = {}
    for r in rows:
        leagues[r[11]] = leagues.get(r[11], 0) + 1

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "since": since,
        "newest": rows[-1][12],
        "count": len(rows),
        "leagues": sorted(leagues.items(), key=lambda kv: -kv[1]),
        "fields": OUT_FIELDS,
        "rows": rows,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

    print("\nWrote data/latest.json — %d rows, newest %s, %.2f MB"
          % (len(rows), payload["newest"], os.path.getsize("data/latest.json") / 1e6))
    print("Leagues: %s" % ", ".join("%s(%d)" % (k, v) for k, v in payload["leagues"][:12]))


if __name__ == "__main__":
    main()
