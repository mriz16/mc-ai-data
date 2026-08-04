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


HISTORY_FILE = os.environ.get("HISTORY_FILE", "history/oracles_elixir.json")


def history_rows(since):
    """Reads history/oracles_elixir.json.gz if present, else the plain .json.

    The uncompressed file is ~7 MB, which exceeds what GitHub's in-browser editor will
    accept as a paste. The gzipped copy is ~1 MB and uploads without trouble.
    """
    """Read the committed history file — the depth half of the feed.

    Google Drive refuses these downloads from CI: runs #16-#18 all came back with a
    quota/interstitial HTML page (2,009 bytes) through gdown AND through a correctly
    submitted confirmation form. Rather than keep fighting it, the history is committed
    to the repo once. It never goes stale in a way that matters — history is history —
    and Riot keeps the recent tail current on every run.
    """
    # Look everywhere the file might reasonably be. GitHub's upload page gives no way to
    # choose a folder, so the file usually lands in the repo root — searching instead of
    # demanding one exact path removes that whole problem.
    names = ["oracles_elixir.json.gz", "oracles_elixir.json",
             "history.json.gz", "history.json"]
    dirs = ["history", ".", "data"]
    candidates = [HISTORY_FILE, HISTORY_FILE + ".gz"]
    for dpath in dirs:
        for nm in names:
            candidates.append(os.path.join(dpath, nm))
    # anything that looks like a history file, wherever it sits
    for dpath in dirs:
        if os.path.isdir(dpath):
            for f in sorted(os.listdir(dpath)):
                if "oracles" in f.lower() or "history" in f.lower():
                    if f.endswith(".json") or f.endswith(".json.gz"):
                        candidates.append(os.path.join(dpath, f))

    found = None
    for c in candidates:
        if c and os.path.exists(c) and os.path.getsize(c) > 100_000:
            found = c
            break
    if not found:
        raise RuntimeError("no history file found. Looked in: " + ", ".join(dirs)
                           + " for oracles_elixir.json(.gz). Upload it anywhere in the repo.")

    if found.endswith(".gz"):
        import gzip                                            # noqa: PLC0415
        with gzip.open(found, "rt", encoding="utf-8") as f:
            payload = json.load(f)
    else:
        with open(found, "r", encoding="utf-8") as f:
            payload = json.load(f)
    DIAG["history_source"] = found
    fields = payload.get("fields") or []
    if fields != OUT_FIELDS:
        idx = {f: i for i, f in enumerate(fields)}
        rows = [[r[idx[c]] if c in idx else 0 for c in OUT_FIELDS] for r in payload.get("rows", [])]
    else:
        rows = payload.get("rows", [])
    DIAG["history_file_rows"] = len(rows)
    # Deliberately NOT filtered by the DAYS window. History is already a curated file and
    # trimming it only ever destroys pool depth. A scheduled run using a short default
    # silently cut the feed to 30 days and reset every player's pool — that must not be
    # possible. DAYS now bounds only the live Riot fetch.
    return rows


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
        tc = [t.get("code") or "" for t in teams]
        played = sum(int(((t.get("result") or {}).get("gameWins") or 0)) for t in teams)
        games = e.get("games") or match.get("games") or []
        if played:
            games = games[:played]
        for g in games:
            if g.get("id"):
                targets.append({"gameId": str(g["id"]), "league": league, "date": date,
                            "teams": tn, "codes": tc})
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
        # Sides SWAP between games of a series, so match.teams order says nothing about who
        # is blue in this game. Assuming it did mislabelled 59% of fresh rows — T1's five
        # showing up as KT Rolster — and because pools key on player+team, every mislabelled
        # player lost their history and fell back to position priors. Resolve the side from
        # the players' own summoner prefixes ("T1 Faker" -> T1), which cannot swap.
        tm = {"blue": t["teams"][0] if len(t["teams"]) > 0 else "",
              "red": t["teams"][1] if len(t["teams"]) > 1 else ""}
        codes = {}
        for nm, cd in zip(t.get("teams") or [], t.get("codes") or []):
            if cd:
                codes[re.sub(r"[^a-z0-9]", "", str(cd).lower())] = nm
            if nm:
                codes[re.sub(r"[^a-z0-9]", "", str(nm).lower())] = nm

        def side_from_prefix(side_key):
            votes = {}
            for x in ((meta.get(side_key) or {}).get("participantMetadata") or []):
                sn = str(x.get("summonerName") or "")
                if " " not in sn:
                    continue
                pre = re.sub(r"[^a-z0-9]", "", sn.split(" ")[0].lower())
                if pre in codes:
                    votes[codes[pre]] = votes.get(codes[pre], 0) + 1
            return max(votes.items(), key=lambda kv: kv[1])[0] if votes else None

        b_name, r_name = side_from_prefix("blueTeamMetadata"), side_from_prefix("redTeamMetadata")
        if b_name and r_name and b_name != r_name:
            tm = {"blue": b_name, "red": r_name}
            DIAG["side_resolved"] = DIAG.get("side_resolved", 0) + 1
        elif b_name or r_name:
            known = b_name or r_name
            other = next((x for x in (t.get("teams") or []) if x != known), "")
            tm = {"blue": known, "red": other} if b_name else {"blue": other, "red": known}
            DIAG["side_resolved"] = DIAG.get("side_resolved", 0) + 1
        else:
            DIAG["side_fallback"] = DIAG.get("side_fallback", 0) + 1
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
        hist = history_rows(since)
        note("history file: %d rows (full file, not windowed)" % len(hist))
    except Exception as e:                                       # noqa: BLE001
        note("no committed history file — trying Drive", False, e)
        try:
            hist = oe_rows(since)
            note("Oracle's Elixir via Drive: %d rows (%.1f MB)" % (len(hist), DIAG.get("oe_mb", 0)))
        except Exception as e2:                                  # noqa: BLE001
            note("Drive unavailable too — continuing with Riot only", False, e2)
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

    # ── reconcile Riot team names onto the historical naming ──
    # Riot writes "DNS Challengers" where history says "DN SOOPers Challengers", and
    # "RED Kalunga Academy" vs "RED Academy". Case-folding cannot catch that. Instead,
    # look at WHO plays for a Riot-named team: if most of its players historically
    # belong to one team, the two names are the same club and are merged. Purely
    # evidence-driven, so it keeps working as rosters and branding change.
    if hist and tail:
        hist_team = {}
        for r in hist:
            hist_team.setdefault(r[0], {}).setdefault(r[1], 0)
            hist_team[r[0]][r[1]] += 1
        hist_names = {r[1] for r in hist}
        by_new = {}
        for r in tail:
            by_new.setdefault(r[1], []).append(r[0])
        alias, merged_rows = {}, 0
        for new_name, players in by_new.items():
            if new_name in hist_names:
                continue                                   # already matches history
            votes = {}
            for p in set(players):
                known = hist_team.get(p)
                if not known:
                    continue
                best = max(known.items(), key=lambda kv: kv[1])[0]
                votes[best] = votes.get(best, 0) + 1
            if not votes:
                continue
            best, n = max(votes.items(), key=lambda kv: kv[1])
            # Guards against a wrong merge, which would be worse than no merge:
            #  * near-unanimous roster agreement (4 of 5), not a bare majority
            #  * the target must not itself be playing in the fresh slice under its own
            #    name — if both are active they are different clubs
            #  * some textual kinship, so "DK Challengers" cannot absorb "T1 Academy"
            if n < 4 or best in by_new:
                continue
            a_tok = set(re.sub(r"[^a-z0-9 ]", "", new_name.lower()).split())
            b_tok = set(re.sub(r"[^a-z0-9 ]", "", best.lower()).split())
            a_flat = re.sub(r"[^a-z0-9]", "", new_name.lower())
            b_flat = re.sub(r"[^a-z0-9]", "", best.lower())
            related = bool(a_tok & b_tok) or a_flat in b_flat or b_flat in a_flat
            if related:
                alias[new_name] = best
            else:
                DIAG.setdefault("alias_rejected", {})[new_name] = best + " (no name kinship)"
        if alias:
            for r in rows:
                if r[1] in alias:
                    r[1] = alias[r[1]]; merged_rows += 1
            DIAG["team_aliases"] = alias
            note("reconciled %d Riot team names to history (%d rows): %s"
                 % (len(alias), merged_rows,
                    ", ".join("%s->%s" % (k, v) for k, v in list(alias.items())[:6])))

    # ── canonicalise names across sources ──
    # Oracle's Elixir and Riot capitalise differently ("Dplus Kia" vs "Dplus KIA",
    # "Kiwoom DRX" vs "KIWOOM DRX", "GiantX" vs "GIANTX"). Left alone this splits a
    # team's pool in two and strands the FRESH Riot games away from the history —
    # measured at 12 teams / 1,690 rows. Collapse each variant onto the spelling that
    # appears most often, so history and new games land in the same pool.
    def canon_key(s):
        return "".join(c for c in s.lower() if c.isalnum())

    for col in (1, 0):                      # 1 = team, 0 = player
        counts = {}
        for r in rows:
            counts.setdefault(canon_key(r[col]), {}).setdefault(r[col], 0)
            counts[canon_key(r[col])][r[col]] += 1
        canon, fixed = {}, 0
        for key, variants in counts.items():
            if len(variants) < 2:
                continue
            best = max(variants.items(), key=lambda kv: kv[1])[0]
            for v in variants:
                if v != best:
                    canon[v] = best
        if canon:
            for r in rows:
                if r[col] in canon:
                    r[col] = canon[r[col]]; fixed += 1
            label = "team" if col == 1 else "player"
            DIAG["canonicalised_" + label] = {"names": len(canon), "rows": fixed}
            note("merged %d %s name variants (%d rows)" % (len(canon), label, fixed))

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
