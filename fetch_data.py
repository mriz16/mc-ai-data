#!/usr/bin/env python3
"""
MC AI — data pipeline.

Pulls recent LoL esports match data from Leaguepedia's public Cargo API and
writes a compact JSON feed that the app fetches through jsDelivr.

Why this exists: the artifact sandbox blocks nearly all outbound requests
(direct fetch, JSONP and CORS proxies all fail) but ALLOWS cdn.jsdelivr.net,
and jsDelivr serves any GitHub repo. GitHub Actions runners have unrestricted
network access, so the fetch happens here and the phone only ever talks to a CDN.

Output: data/latest.json
  {
    "generated": "2026-08-01T12:00:00Z",
    "since": "2026-04-03",
    "count": 61234,
    "fields": ["player","team","champion","kills",...],
    "rows": [[...], [...], ...]        # arrays, not objects, to keep it small
  }
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://lol.fandom.com/api.php"
UA = "MC-AI-pipeline/1.0 (personal analytics; contact via GitHub)"

# Cargo fields -> output keys. SP = ScoreboardPlayers, SG = ScoreboardGames.
FIELD_MAP = [
    ("SP.Link", "player"),
    ("SP.Team", "team"),
    ("SP.Champion", "champion"),
    ("SP.Kills", "kills"),
    ("SP.Deaths", "deaths"),
    ("SP.Assists", "assists"),
    ("SP.CS", "cs"),
    ("SP.Role", "role"),
    ("SP.TeamKills", "teamkills"),
    ("SP.PlayerWin", "win"),
    ("SP.GameId", "gameid"),
    ("SP.OverviewPage", "page"),
    ("SG.DateTime_UTC", "date"),
    ("SG.Patch", "patch"),
    ("SG.Gamelength_Number", "glen"),
]

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

ROLE_MAP = {
    "top": "top", "toplane": "top",
    "jungle": "jng", "jng": "jng",
    "mid": "mid", "middle": "mid",
    "bot": "bot", "adc": "bot", "botlane": "bot",
    "support": "sup", "sup": "sup",
}

DAYS = int(os.environ.get("DAYS", "120"))
LIMIT = 500
MAX_PAGES = int(os.environ.get("MAX_PAGES", "400"))


def cargo(offset, since):
    params = {
        "action": "cargoquery",
        "tables": "ScoreboardPlayers=SP,ScoreboardGames=SG",
        "join_on": "SP.GameId=SG.GameId",
        "fields": ",".join("%s=%s" % (c, a) for c, a in FIELD_MAP),
        "where": "SG.DateTime_UTC >= '%s 00:00:00'" % since,
        "order_by": "SG.DateTime_UTC ASC",
        "limit": str(LIMIT),
        "offset": str(offset),
        "format": "json",
    }
    url = API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def num(v, default=0):
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except (TypeError, ValueError):
        return default


def to_row(t):
    page = str(t.get("page") or "")
    league = page.split("/")[0] if "/" in page else page
    role = str(t.get("role") or "").strip().lower()
    win = str(t.get("win") or "")
    date = str(t.get("date") or "")[:10]
    glen_min = num(t.get("glen"), 0)
    return [
        str(t.get("player") or "").strip(),
        str(t.get("team") or "").strip(),
        str(t.get("champion") or "").strip(),
        num(t.get("kills")),
        num(t.get("deaths")),
        num(t.get("assists")),
        num(t.get("cs")),
        ROLE_MAP.get(role, role),
        num(t.get("teamkills")),
        1 if win in ("Yes", "1", "true", "True") else 0,
        str(t.get("gameid") or ""),
        league,
        date,
        num(t.get("patch"), 0),
        int(round(glen_min * 60)) if glen_min else 0,
    ]


def main():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    print("Fetching Leaguepedia since %s (window %sd)" % (since, DAYS), flush=True)

    rows, seen = [], set()
    for page in range(MAX_PAGES):
        offset = page * LIMIT
        for attempt in range(5):
            try:
                data = cargo(offset, since)
                break
            except Exception as e:                       # noqa: BLE001
                wait = 10 * (attempt + 1)
                print("  retry %d after error: %s (waiting %ds)" % (attempt + 1, e, wait), flush=True)
                time.sleep(wait)
        else:
            print("FATAL: repeated failures at offset %d" % offset, file=sys.stderr)
            sys.exit(1)

        if "error" in data:
            print("FATAL: Cargo error: %s" % data["error"], file=sys.stderr)
            sys.exit(1)

        batch = data.get("cargoquery", [])
        for item in batch:
            row = to_row(item.get("title", {}))
            if not row[0] or not row[10]:                # need player + gameid
                continue
            key = row[10] + "|" + row[0]
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

        print("  offset %6d -> %3d rows (total %d)" % (offset, len(batch), len(rows)), flush=True)
        if len(batch) < LIMIT:
            break
        time.sleep(0.6)                                  # be polite to the API

    if not rows:
        print("FATAL: no rows returned — refusing to overwrite the feed", file=sys.stderr)
        sys.exit(1)

    rows.sort(key=lambda r: r[12])
    newest = rows[-1][12]
    leagues = {}
    for r in rows:
        leagues[r[11]] = leagues.get(r[11], 0) + 1

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "since": since,
        "newest": newest,
        "count": len(rows),
        "leagues": sorted(leagues.items(), key=lambda kv: -kv[1]),
        "fields": OUT_FIELDS,
        "rows": rows,
    }

    os.makedirs("data", exist_ok=True)
    out = "data/latest.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

    size_mb = os.path.getsize(out) / 1e6
    print("\nWrote %s — %d rows, newest %s, %.2f MB" % (out, len(rows), newest, size_mb))
    print("Top leagues: %s" % ", ".join("%s(%d)" % (k, v) for k, v in payload["leagues"][:10]))


if __name__ == "__main__":
    main()
