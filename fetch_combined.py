#!/usr/bin/env python3
"""
MC AI — data pipeline (combined source).

WHAT THE EVIDENCE FORCED
  * Riot esports API: games appear minutes after they finish, but per-game stats are
    only retained ~3 weeks. Measured: 2,659 events reachable back to 2023, yet just
    555 games with usable stats. Pools ended up 7-11 games deep and every projection
    collapsed toward position priors.
  * Oracle's Elixir: stopped updating 2026-07-28, so it cannot carry current form —
    but its HISTORY does not rot. Months of depth, and the exact column semantics the
    model was validated against.

Neither alone is sufficient. Combined they are complementary:
    Oracle's Elixir  -> everything up to its last published date  (DEPTH)
    Riot esports API -> every day after that                      (FRESHNESS)
Split at OE's newest date, so the two never overlap and nothing is double counted.
"""

import csv
import io
import json
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from datetime import datetime, timedelta, timezone

# ── Oracle's Elixir (history) ──
FOLDER_ID = os.environ.get("OE_FOLDER_ID", "1gLSw0RLjBbtaNy0dgnGQDAZOHIgCe-HH")
OE_YEAR = os.environ.get("OE_YEAR") or str(datetime.now(timezone.utc).year)

# ── Riot esports (fresh tail) ──
API_KEY = os.environ.get("RIOT_ESPORTS_KEY", "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z")
ESPORTS = "https://esports-api.lolesports.com/persisted/gw/"
FEED = "https://feed.lolesports.com/livestats/v1/"

UA = "MC-AI-pipeline/8.0"
DAYS = int(os.environ.get("DAYS", "120"))
MAX_GAMES = int(os.environ.get("MAX_GAMES", "900"))
PAUSE = float(os.environ.get("PAUSE", "0.3"))

ROLE_MAP = {"top": "top", "jungle": "jng", "jng": "jng", "mid": "mid", "middle": "mid",
            "bottom": "bot", "bot": "bot", "adc": "bot", "support": "sup", "sup": "sup"}

LEAGUE_MAP = {
    "Esports World Cup": "EWC", "Prime League": "PRM", "Arabian League": "AL",
    "La Ligue Française": "LFL", "La Ligue Francaise": "LFL", "Circuito Desafiante": "CD",
    "Hellenic Legends League": "HLL", "LoL Italian Tournament": "LIT",
    "LCK Challengers": "LCKC", "LCK Challengers League": "LCKC",
    "Liga Portuguesa": "LPLOL", "Road of Legends": "ROL", "Esports Balkan League": "EBL",
    "North American Challengers League": "NACL", "Worlds": "WLDs",
    "World Championship": "WLDs", "First Stand": "FST",
    "Liga Latinoamérica": "LLA", "Liga Latinoamerica": "LLA",
    "Tencent LoL Pro League": "LPL", "LoL Champions Korea": "LCK",
    "LoL EMEA Championship": "LEC", "League Championship Series": "LCS",
    "League of Legends Championship Pacific": "LCP", "Ultraliga": "UL",
    "Nordic Legends League": "NLC", "Liga Nexo": "LRN", "Elite Series": "LES",
    "CBLOL Academy": "CBLOLA",
}

OUT_FIELDS = ["player", "team", "champion", "kills", "deaths", "assists", "cs",
              "position", "teamkills", "result", "gameid", "league", "date",
              "patch", "gamelength"]

DIAG = {"version": 8, "source": "oracles_elixir + riot_esports",
        "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "steps": [], "outcome": "incomplete", "requests": 0}


def note(msg, ok=True, detail=""):
    DIAG["steps"].append({"step": msg, "ok": bool(ok), "detail": str(detail)[:500]})
    print(("  [ok]   " if ok else "  [FAIL] ") + msg + ((" :: " + str(detail)[:220]) if detail else ""), flush=True)


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


# ───────────────────────── Oracle's Elixir ─────────────────────────
def oe_get(url, timeout=240):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; MC-AI/8.0)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def oe_file_id():
    """Locate this year's CSV inside the public Drive folder."""
    override = os.environ.get("OE_FILE_ID", "").strip()
    if override:
        return override
    html = oe_get("https://drive.google.com/drive/folders/" + FOLDER_ID).decode("utf-8", "replace")
    pat = r'"([a-zA-Z0-9_-]{25,})"(?=[^}]{0,400}' + re.escape(OE_YEAR) + r'[^}]{0,200}\.csv)'
    m = re.search(pat, html) or re.search(r'"([a-zA-Z0-9_-]{25,})"(?=[^}]{0,400}\.csv)', html)
    if not m:
        raise RuntimeError("could not find this year's CSV in the Drive folder")
    return m.group(1)


def oe_download(fid):
    """Fetch a large public Drive file.

    Two failure modes seen on the runner, both handled here:
      * gdown's signature varies by version — run #17 died on an unexpected
        `fuzzy` argument, so each call form is tried in turn.
      * Drive gates large files behind a CONFIRMATION FORM, not a redirect. The
        naive `confirm=` guess returned the interstitial HTML again. The manual
        path now parses the form's action plus every hidden input and submits it
        with the session cookies, which is what the browser actually does.
    """
    out = "oe_raw.csv"

    try:
        import gdown                                            # noqa: PLC0415
        forms = [
            lambda: gdown.download(id=fid, output=out, quiet=True),
            lambda: gdown.download("https://drive.google.com/uc?id=" + fid, out, quiet=True),
            lambda: gdown.download("https://drive.google.com/uc?id=" + fid, output=out, quiet=True),
        ]
        for f in forms:
            try:
                if os.path.exists(out):
                    os.remove(out)
                f()
            except TypeError:
                continue                                        # wrong signature for this version
            except Exception:                                   # noqa: BLE001
                continue
            if os.path.exists(out) and os.path.getsize(out) > 1_000_000:
                with open(out, "rb") as fh:
                    return fh.read()
        note("gdown produced no usable file — using manual download", False)
    except ImportError:
        note("gdown not installed — using manual download", False)

    # Manual: carry cookies and complete Drive's confirmation form.
    jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [("User-Agent", "Mozilla/5.0 (compatible; MC-AI/8.0)")]

    def fetch(u):
        with opener.open(u, timeout=300) as r:
            return r.read()

    data = fetch("https://drive.google.com/uc?export=download&id=" + fid)
    if b"<html" in data[:600].lower():
        html = data.decode("utf-8", "replace")
        m = re.search(r'action="([^"]+)"', html)
        action = (m.group(1).replace("&amp;", "&") if m
                  else "https://drive.usercontent.google.com/download")
        fields = dict(re.findall(r'name="([^"]+)"\s+value="([^"]*)"', html))
        fields.setdefault("id", fid)
        fields.setdefault("export", "download")
        fields.setdefault("confirm", "t")
        url = action + ("&" if "?" in action else "?") + urllib.parse.urlencode(fields)
        DIAG["oe_confirm_url"] = url[:160]
        data = fetch(url)

    if b"<html" in data[:600].lower() or len(data) < 1_000_000:
        raise RuntimeError("Drive returned %d bytes of HTML, not the CSV" % len(data))
    return data


def oe_rows(since):
    fid = oe_file_id()
    DIAG["oe_file_id"] = fid
    raw = oe_download(fid)
    DIAG["oe_mb"] = round(len(raw) / 1e6, 2)

    rows = []
    rdr = csv.DictReader(io.StringIO(raw.decode("utf-8", "replace")))
    for r in rdr:
        pid = (r.get("participantid") or "").strip()
        if not pid.isdigit() or not (1 <= int(pid) <= 10):
            continue
        date = (r.get("date") or "")[:10]
        if date < since:
            continue
        cs = r.get("total cs")
        if cs in (None, ""):
            cs = num(r.get("minionkills")) + num(r.get("monsterkills"))
        rows.append([(r.get("playername") or "").strip(), (r.get("teamname") or "").strip(),
                     (r.get("champion") or "").strip(), num(r.get("kills")), num(r.get("deaths")),
                     num(r.get("assists")), num(cs), (r.get("position") or "").strip(),
                     num(r.get("teamkills")), num(r.get("result")), (r.get("gameid") or "").strip(),
                     (r.get("league") or "").strip(), date, num(r.get("patch")), num(r.get("gamelength"))])
    return rows


# ───────────────────────── Riot esports ─────────────────────────
def riot_get(url, tries=4):
    for attempt in range(tries):
        DIAG["requests"] += 1
        req = urllib.request.Request(url, headers={"x-api-key": API_KEY, "User-Agent": UA,
                                                   "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                body = r.read().decode("utf-8", "replace")
            time.sleep(PAUSE)
            return json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < tries - 1:
                time.sleep(4 * (attempt + 1)); continue
            raise RuntimeError("HTTP %s" % e.code)
        except Exception as e:                                   # noqa: BLE001
            if attempt < tries - 1:
                time.sleep(3 * (attempt + 1)); continue
            raise RuntimeError(str(e)[:150])
    raise RuntimeError("retries exhausted")


def _iso10(dt):
    dt = dt.replace(microsecond=0, second=(dt.second // 10) * 10)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def riot_final(game_id):
    """Opening frames give the true start; a later startingTime gives the final box score."""
    try:
        head = riot_get(FEED + "window/" + str(game_id))
    except RuntimeError:
        return None, None, 0
    meta, hf = head.get("gameMetadata") or {}, head.get("frames") or []
    if not hf:
        return None, None, 0
    try:
        t0 = datetime.strptime(hf[0].get("rfc460Timestamp", "")[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:                                            # noqa: BLE001
        return None, None, 0
    best, best_k, best_ts = None, -1, None
    for mins in (110, 75, 50, 38, 30, 24):
        try:
            w = riot_get(FEED + "window/" + str(game_id) + "?startingTime=" + _iso10(t0 + timedelta(minutes=mins)))
        except RuntimeError:
            continue
        fr = w.get("frames") or []
        if not fr:
            continue
        f = fr[-1]
        tk = ((f.get("blueTeam") or {}).get("totalKills") or 0) + ((f.get("redTeam") or {}).get("totalKills") or 0)
        if tk > best_k:
            best, best_k, best_ts = f, tk, f.get("rfc460Timestamp", "")
        if tk > 0 and mins <= 75:
            break
    if best is None:
        return None, None, 0
    dur = 0
    try:
        dur = int((datetime.strptime(best_ts[:19], "%Y-%m-%dT%H:%M:%S") - t0).total_seconds())
    except Exception:                                            # noqa: BLE001
        dur = 0
    return meta, best, max(0, dur)


def riot_rows(since):
    lgs = riot_get(ESPORTS + "getLeagues?hl=en-US").get("data", {}).get("leagues", [])
    ids = [str(l.get("id")) for l in lgs if l.get("id")]
    events, seen = [], set()

    def add(batch):
        for e in batch:
            key = str((e.get("match") or {}).get("id") or "") + "|" + str(e.get("startTime") or "")
            if key not in seen:
                seen.add(key); events.append(e)

    token, pages = None, 0
    while pages < 10:
        d = riot_get(ESPORTS + "getCompletedEvents?hl=en-US" + (("&pageToken=" + urllib.parse.quote(token)) if token else ""))
        sc = d.get("data", {}).get("schedule", {})
        b = sc.get("events", []) or []
        if not b:
            break
        add(b)
        token = (sc.get("pages") or {}).get("older")
        pages += 1
        if not token:
            break
    for lid in ids:
        token, pages = None, 0
        while pages < 6:
            try:
                d = riot_get(ESPORTS + "getSchedule?hl=en-US&leagueId=" + lid + (("&pageToken=" + urllib.parse.quote(token)) if token else ""))
            except RuntimeError:
                break
            sc = d.get("data", {}).get("schedule", {})
            b = sc.get("events", []) or []
            if not b:
                break
            add([e for e in b if str(e.get("state", "")).lower() == "completed"])
            oldest = min((e.get("startTime") or "9999") for e in b)[:10]
            token = (sc.get("pages") or {}).get("older")
            pages += 1
            if not token or oldest < since:
                break
    DIAG["riot_events"] = len(events)

    targets = []
    for e in events:
        date = (e.get("startTime") or "")[:10]
        if date < since:
            continue
        lg = e.get("league") or {}
        raw = lg.get("name") or lg.get("slug") or ""
        league = LEAGUE_MAP.get(raw, raw)
        match = e.get("match") or {}
        teams = match.get("teams") or []
        tn = [t.get("name") or t.get("code") or "" for t in teams]
        played = sum(int(((t.get("result") or {}).get("gameWins") or 0)) for t in teams)
        games = e.get("games") or match.get("games") or []
        if played:
            games = games[:played]
        for g in games:
            if g.get("id"):
                targets.append({"gameId": str(g["id"]), "league": league, "date": date, "teams": tn})
    DIAG["riot_games_found"] = len(targets)

    rows, ok, bad = [], 0, 0
    for t in targets[:MAX_GAMES]:
        meta, last, dur = riot_final(t["gameId"])
        if not meta or not last:
            bad += 1; continue
        patch = meta.get("patchVersion") or ""
        try:
            pf = float(".".join(patch.split(".")[:2])) if patch else 0
        except ValueError:
            pf = 0
        pm = {}
        for side in ("blueTeamMetadata", "redTeamMetadata"):
            for x in ((meta.get(side) or {}).get("participantMetadata") or []):
                pm[x.get("participantId")] = {
                    "name": (x.get("summonerName") or "").split(" ", 1)[-1].strip(),
                    "role": ROLE_MAP.get(str(x.get("role", "")).lower(), str(x.get("role", "")).lower()),
                    "champ": x.get("championId") or ""}
        blue, red = last.get("blueTeam") or {}, last.get("redTeam") or {}
        tk = {"blue": blue.get("totalKills", 0) or 0, "red": red.get("totalKills", 0) or 0}
        win = {"blue": 1 if tk["blue"] >= tk["red"] else 0, "red": 1 if tk["red"] > tk["blue"] else 0}
        tm = {"blue": t["teams"][0] if len(t["teams"]) > 0 else "",
              "red": t["teams"][1] if len(t["teams"]) > 1 else ""}
        for side, obj in (("blue", blue), ("red", red)):
            for p in (obj.get("participants") or []):
                m = pm.get(p.get("participantId"), {})
                rows.append([m.get("name", ""), tm[side], m.get("champ", ""),
                             p.get("kills", 0) or 0, p.get("deaths", 0) or 0, p.get("assists", 0) or 0,
                             p.get("creepScore", 0) or 0, m.get("role", ""), tk[side], win[side],
                             t["gameId"], t["league"], t["date"], pf, dur])
        ok += 1
        if ok % 50 == 0:
            print("    riot: %d/%d games (%d rows)" % (ok, len(targets[:MAX_GAMES]), len(rows)), flush=True)
    DIAG["riot_games_ok"] = ok
    DIAG["riot_games_failed"] = bad
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
    print("MC AI pipeline v8 (combined) — %sd window since %s\n" % (DAYS, since), flush=True)

    hist = []
    try:
        hist = oe_rows(since)
        note("Oracle's Elixir history: %d rows (%.1f MB)" % (len(hist), DIAG.get("oe_mb", 0)))
    except Exception as e:                                       # noqa: BLE001
        note("Oracle's Elixir unavailable — continuing with Riot only", False, e)
    DIAG["oe_rows"] = len(hist)
    oe_max = max((r[12] for r in hist), default="")
    DIAG["oe_newest"] = oe_max
    if oe_max:
        note("history covers %s .. %s" % (min(r[12] for r in hist), oe_max))

    fresh = []
    try:
        fresh = riot_rows(since)
        note("Riot live rows: %d" % len(fresh))
    except Exception as e:                                       # noqa: BLE001
        note("Riot fetch failed", False, e)
    DIAG["riot_rows_all"] = len(fresh)

    # Split at OE's last day so the two sources never cover the same game.
    tail = [r for r in fresh if not oe_max or r[12] > oe_max]
    DIAG["riot_rows_used"] = len(tail)
    note("using %d Riot rows after %s (dropped %d overlapping)"
         % (len(tail), oe_max or "n/a", len(fresh) - len(tail)))

    rows = hist + tail
    if not rows:
        DIAG["outcome"] = "no_rows"
        note("no data from either source — refusing to overwrite", False)
        return

    seen, dedup = set(), []
    for r in rows:
        k = r[10] + "|" + r[0]
        if k in seen:
            continue
        seen.add(k); dedup.append(r)
    dedup.sort(key=lambda r: r[12])

    lg = {}
    for r in dedup:
        lg[r[11]] = lg.get(r[11], 0) + 1
    payload = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "source": "oracles_elixir+riot", "since": since, "newest": dedup[-1][12],
               "count": len(dedup), "complete": True,
               "history_through": oe_max, "leagues": sorted(lg.items(), key=lambda kv: -kv[1]),
               "fields": OUT_FIELDS, "rows": dedup}
    write("data/latest.json", payload)
    DIAG["outcome"] = "ok"
    DIAG["written"] = {"rows": len(dedup), "newest": payload["newest"],
                       "oldest": dedup[0][12], "history_through": oe_max,
                       "mb": round(os.path.getsize("data/latest.json") / 1e6, 2),
                       "leagues": payload["leagues"][:15]}
    note("wrote %d rows spanning %s .. %s" % (len(dedup), dedup[0][12], payload["newest"]))


if __name__ == "__main__":
    try:
        run()
    except Exception:                                            # noqa: BLE001
        DIAG["outcome"] = "exception"
        DIAG["traceback"] = traceback.format_exc()[-2500:]
        print(DIAG["traceback"], file=sys.stderr)
    DIAG["finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write("data/diagnostic.json", DIAG, compact=False)
    print("\noutcome=%s" % DIAG["outcome"], flush=True)
    sys.exit(0)
