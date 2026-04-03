#!/usr/bin/env python3
"""
BEARWATCH — YouTube Data Fetcher
Pulls current channel stats for the Build-A-Bear official channel and key
unboxing/toy channels. Counts BBW-related videos uploaded in the last 30 days
per unboxing channel as a demand leading indicator.

Outputs:
  data/youtube_bbw_latest.json   — current snapshot (overwritten each run)
  data/youtube_bbw_monthly.csv   — accumulating monthly history (appended)

Run monthly (first of each month recommended):
  python3 scripts/fetch_youtube_bbw.py

YouTube Data API v3 key: set YOUTUBE_API_KEY in .env or environment.
Key is shared with Warhammer project: /Users/emmania/Desktop/warhammer_demand/.env
"""

import os, csv, json, time, sys
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import requests

# ── Load API key ──────────────────────────────────────────────────────────────
# Try local .env first, then fall back to warhammer .env
for env_path in [
    Path(__file__).parent.parent / ".env",
    Path("/Users/emmania/Desktop/warhammer_demand/.env"),
]:
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip().lstrip("export ")
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        break

KEY = os.getenv("YOUTUBE_API_KEY")
if not KEY:
    raise SystemExit("Missing YOUTUBE_API_KEY — not found in .env or environment")

# ── Channel config ─────────────────────────────────────────────────────────────
# Verified channel IDs via YouTube Data API v3 (April 2026)
CHANNELS = [
    {
        "label":      "Build-A-Bear Workshop",
        "channel_id": "UCLKFj0lOHWcbjx8Nrpcw_fg",
        "role":       "official",
        "note":       "@buildabear — US official channel",
    },
    {
        "label":      "Lots of Toys",
        "channel_id": "UC9OSlGcOPOYBYq9upfioL5Q",
        "role":       "unboxing",
        "note":       "Toy unboxing channel — BBW hauls",
    },
    {
        "label":      "DOLLASTIC PLAYS!",
        "channel_id": "UC5hvHi9B93qzv36JB3e2Q2A",
        "role":       "unboxing",
        "note":       "Major toy/craft channel — frequent BBW content",
    },
    {
        "label":      "Karina Garcia",
        "channel_id": "UCTTJMptGhfJA67e40DlqbNw",
        "role":       "unboxing",
        "note":       "9M+ sub lifestyle/craft channel — occasional BBW",
    },
    # NOTE: 'Toy Caboodle' (890K subs) could not be confirmed via YouTube API.
    # Add manually if the channel ID is found:
    # {"label": "Toy Caboodle", "channel_id": "UC_REPLACE_ME", "role": "unboxing"},
]

# ── Output paths ───────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent.parent
OUT_JSON = ROOT / "data" / "youtube_bbw_latest.json"
OUT_CSV  = ROOT / "data" / "youtube_bbw_monthly.csv"
OUT_JSON.parent.mkdir(exist_ok=True)

CSV_COLS = [
    "date", "label", "channel_id", "role",
    "subscribers", "total_views", "video_count",
    "bbw_videos_30d", "bbw_views_30d",
]

today            = str(date.today())
published_after  = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Helpers ────────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def yt_get(endpoint, params, retries=2):
    url = f"https://www.googleapis.com/youtube/v3/{endpoint}"
    params["key"] = KEY
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if r.status_code == 403:
                log(f"  403 Forbidden — quota exceeded or key issue: {r.text[:200]}")
                raise
            if attempt == retries:
                raise
            time.sleep(1.5)
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(1.5)


# ── Step 1: Fetch channel stats in one batch call ─────────────────────────────
log("Fetching channel stats...")
all_ids = [c["channel_id"] for c in CHANNELS]
data = yt_get("channels", {
    "part": "snippet,statistics",
    "id":   ",".join(all_ids),
})

stats_map = {}
for item in data.get("items", []):
    cid = item["id"]
    s   = item.get("statistics", {})
    stats_map[cid] = {
        "title":       item["snippet"]["title"],
        "subscribers": int(s.get("subscriberCount", 0) or 0),
        "total_views": int(s.get("viewCount",       0) or 0),
        "video_count": int(s.get("videoCount",      0) or 0),
    }
    st = stats_map[cid]
    subs_fmt = f"{st['subscribers']/1e6:.2f}M" if st['subscribers'] >= 1_000_000 else f"{st['subscribers']/1000:.0f}K"
    log(f"  {st['title']}: {subs_fmt} subs | {st['total_views']/1e6:.1f}M total views")


# ── Step 2: BBW video count per unboxing channel (last 30 days) ───────────────
log("\nCounting BBW-related uploads per unboxing channel (last 30d)...")
unboxing_results = {}

for ch in CHANNELS:
    if ch["role"] != "unboxing":
        continue
    cid   = ch["channel_id"]
    label = ch["label"]

    bbw_video_count = 0
    bbw_views_total = 0

    try:
        # Search for videos mentioning Build-A-Bear on this channel in last 30d
        resp = yt_get("search", {
            "part":          "id",
            "channelId":     cid,
            "q":             "build a bear",
            "type":          "video",
            "publishedAfter": published_after,
            "maxResults":    50,
        })
        video_ids = [it["id"]["videoId"] for it in resp.get("items", [])]

        if video_ids:
            # Get view counts for those videos
            vdata = yt_get("videos", {
                "part": "statistics,snippet",
                "id":   ",".join(video_ids),
            })
            for vitem in vdata.get("items", []):
                title = vitem["snippet"].get("title", "")
                views = int(vitem["statistics"].get("viewCount", 0) or 0)
                bbw_video_count += 1
                bbw_views_total += views
                log(f"    ↳ {title[:60]} · {views:,} views")

        time.sleep(0.4)

    except Exception as e:
        log(f"  WARNING — {label}: {e}")

    unboxing_results[cid] = {
        "bbw_videos_30d": bbw_video_count,
        "bbw_views_30d":  bbw_views_total,
    }
    log(f"  {label}: {bbw_video_count} BBW videos last 30d | {bbw_views_total:,} total views")


# ── Step 3: Build snapshot ─────────────────────────────────────────────────────
snapshot = {"date": today, "channels": []}

for ch in CHANNELS:
    cid = ch["channel_id"]
    st  = stats_map.get(cid, {})
    ub  = unboxing_results.get(cid, {})

    record = {
        "date":          today,
        "label":         ch["label"],
        "channel_id":    cid,
        "role":          ch["role"],
        "subscribers":   st.get("subscribers", 0),
        "total_views":   st.get("total_views", 0),
        "video_count":   st.get("video_count", 0),
        "bbw_videos_30d": ub.get("bbw_videos_30d", None if ch["role"] == "official" else 0),
        "bbw_views_30d":  ub.get("bbw_views_30d",  None if ch["role"] == "official" else 0),
    }
    snapshot["channels"].append(record)


# ── Step 4: Save JSON ──────────────────────────────────────────────────────────
OUT_JSON.write_text(json.dumps(snapshot, indent=2))
log(f"\nSaved → {OUT_JSON}")


# ── Step 5: Append to CSV (dedup on date + channel_id) ────────────────────────
rows_new = [{c: rec.get(c, "") for c in CSV_COLS} for rec in snapshot["channels"]]

if OUT_CSV.exists():
    with open(OUT_CSV, newline="") as f:
        existing = list(csv.DictReader(f))
    existing_keys = {(r["date"], r["channel_id"]) for r in existing}
    rows_to_add   = [r for r in rows_new if (r["date"], r["channel_id"]) not in existing_keys]
    all_rows      = existing + rows_to_add
else:
    all_rows = rows_new

with open(OUT_CSV, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=CSV_COLS)
    w.writeheader()
    w.writerows(all_rows)

log(f"Saved → {OUT_CSV}  ({len(all_rows)} total rows, {len({r['date'] for r in all_rows})} date snapshots)")


# ── Step 6: Print bearwatch.html patch summary ─────────────────────────────────
print("\n" + "="*60)
print("bearwatch.html patch values")
print("="*60)
for rec in snapshot["channels"]:
    subs = rec["subscribers"]
    subs_fmt = f"{subs/1e6:.1f}M" if subs >= 1_000_000 else f"{subs/1000:.0f}K"
    if rec["role"] == "official":
        views_fmt = f"{rec['total_views']/1e6:.0f}M"
        print(f"  Build-A-Bear official : {subs_fmt} subs | {views_fmt} total views")
    else:
        bbv = rec.get("bbw_videos_30d", 0)
        bbvv = rec.get("bbw_views_30d", 0)
        trend = "—" if bbv == 0 else ("Rising" if bbv >= 3 else "Low")
        print(f"  {rec['label']:<24}: {subs_fmt:>6} subs | {bbv:>2} BBW vids/30d | {bbvv:>8,} views | Trend: {trend}")
print()
