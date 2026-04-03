"""
BEARWATCH — Reddit Historical Data Pull
Uses Arctic Shift (free, no credentials) to pull 3 years of Reddit history.

Pulls for: r/buildabear, r/squishmallows, r/stuffedanimals
Goes back to: January 2022
Saves to: data/reddit_history_monthly.csv and data/reddit_history_raw.json

Run ONCE to build historical foundation:
    python3 scripts/fetch_reddit_history.py

Then run weekly for ongoing updates:
    python3 update_data.py (subscriber snapshots, already working)

Arctic Shift docs: https://arctic-shift.photon-reddit.com/api-docs
"""

import json
import os
import time
import csv
from datetime import datetime, timezone
from collections import defaultdict

import requests

# ── CONFIG ────────────────────────────────────────────────────
SUBREDDITS = [
    "buildabear",
    "squishmallows",   # competitor
    "stuffedanimals",  # category
]

# Go back to Jan 2022 — gives us 3+ years of history
START_DATE = datetime(2022, 1, 1, tzinfo=timezone.utc)
END_DATE   = datetime.now(timezone.utc)

# Arctic Shift base URL
ARCTIC_BASE = "https://arctic-shift.photon-reddit.com/api"

SLEEP = 1.5  # seconds between requests — be polite

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
MONTHLY_CSV = os.path.join(DATA_DIR, "reddit_history_monthly.csv")
RAW_JSON    = os.path.join(DATA_DIR, "reddit_history_raw.json")
SPIKES_JSON = os.path.join(DATA_DIR, "reddit_spikes.json")

# Known BBW release/event dates for spike correlation
# Add more as you research them
BBW_EVENTS = [
    {"date": "2022-11-01", "name": "Holiday collection launch"},
    {"date": "2023-02-01", "name": "Valentine's Day / Pay Your Age"},
    {"date": "2023-05-01", "name": "Star Wars Day collab"},
    {"date": "2023-11-01", "name": "Holiday 2023"},
    {"date": "2024-02-01", "name": "Valentine's Day 2024"},
    {"date": "2024-05-04", "name": "Star Wars Day 2024"},
    {"date": "2024-11-01", "name": "Holiday 2024"},
    {"date": "2025-02-01", "name": "Valentine's Day 2025"},
    {"date": "2025-05-04", "name": "Star Wars Day 2025"},
    {"date": "2025-11-01", "name": "Holiday 2025"},
    # Add confirmed 2026 dates when known
]
# ─────────────────────────────────────────────────────────────


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def fetch_posts_month(subreddit, year, month):
    """
    Fetch all posts from a subreddit for a given month using Arctic Shift.
    Returns list of post dicts.
    """
    # Build month start/end timestamps
    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    after_ts  = int(month_start.timestamp())
    before_ts = int(month_end.timestamp())

    all_posts = []
    after_param = after_ts

    while True:
        try:
            url = (
                f"{ARCTIC_BASE}/posts/search"
                f"?subreddit={subreddit}"
                f"&after={after_param}"
                f"&before={before_ts}"
                f"&limit=100"
                f"&sort=asc"
            )
            resp = requests.get(url, timeout=20)

            if resp.status_code == 429:
                log(f"  Rate limited — waiting 10s")
                time.sleep(10)
                continue

            resp.raise_for_status()
            data = resp.json()
            posts = data.get("data", [])

            if not posts:
                break

            all_posts.extend(posts)

            # If we got less than 100, we're done for this month
            if len(posts) < 100:
                break

            # Otherwise paginate — use last post's created_utc + 1 as next after
            last_ts = posts[-1].get("created_utc", 0)
            if isinstance(last_ts, str):
                last_ts = int(last_ts)
            after_param = last_ts + 1

            if after_param >= before_ts:
                break

            time.sleep(SLEEP)

        except requests.exceptions.RequestException as e:
            log(f"  Request error for r/{subreddit} {year}-{month:02d}: {e}")
            time.sleep(5)
            break

    return all_posts


def fetch_comments_month(subreddit, year, month):
    """
    Fetch comment count for a subreddit/month from Arctic Shift aggregate endpoint.
    """
    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    after_ts  = int(month_start.timestamp())
    before_ts = int(month_end.timestamp())

    total_comments = 0
    after_param    = after_ts

    while True:
        try:
            url = (
                f"{ARCTIC_BASE}/comments/search"
                f"?subreddit={subreddit}"
                f"&after={after_param}"
                f"&before={before_ts}"
                f"&limit=100"
                f"&sort=asc"
            )
            resp = requests.get(url, timeout=20)

            if resp.status_code == 429:
                log(f"  Rate limited on comments — waiting 10s")
                time.sleep(10)
                continue

            resp.raise_for_status()
            data  = resp.json()
            items = data.get("data", [])

            total_comments += len(items)

            if len(items) < 100:
                break

            last_ts = items[-1].get("created_utc", 0)
            if isinstance(last_ts, str):
                last_ts = int(last_ts)
            after_param = last_ts + 1

            if after_param >= before_ts:
                break

            time.sleep(SLEEP)

        except requests.exceptions.RequestException as e:
            log(f"  Comment fetch error for r/{subreddit} {year}-{month:02d}: {e}")
            break

    return total_comments


def aggregate_posts(posts):
    """Compute monthly metrics from raw post list."""
    if not posts:
        return {
            "post_count":       0,
            "total_score":      0,
            "avg_score":        0,
            "total_comments":   0,
            "avg_comments":     0,
            "avg_upvote_ratio": 0,
            "top_post_title":   "",
            "top_post_score":   0,
        }

    scores   = [p.get("score", 0) or 0         for p in posts]
    comments = [p.get("num_comments", 0) or 0  for p in posts]
    ratios   = [p.get("upvote_ratio", 0) or 0  for p in posts]

    top_post = max(posts, key=lambda p: p.get("score", 0) or 0)

    return {
        "post_count":       len(posts),
        "total_score":      sum(scores),
        "avg_score":        round(sum(scores) / len(scores), 1) if scores else 0,
        "total_comments":   sum(comments),
        "avg_comments":     round(sum(comments) / len(comments), 1) if comments else 0,
        "avg_upvote_ratio": round(sum(ratios) / len(ratios), 3) if ratios else 0,
        "top_post_title":   top_post.get("title", "")[:120],
        "top_post_score":   top_post.get("score", 0) or 0,
    }


def detect_spikes(monthly_data, subreddit, window=3):
    """
    Detect months where post_count is >1.5x the rolling average of prior `window` months.
    Returns list of spike dicts.
    """
    rows   = sorted(monthly_data, key=lambda r: r["month"])
    spikes = []

    for i, row in enumerate(rows):
        if i < window:
            continue
        prior  = rows[i - window: i]
        avg    = sum(p["post_count"] for p in prior) / window
        if avg == 0:
            continue
        ratio  = row["post_count"] / avg
        if ratio >= 1.5:
            # Find nearest BBW event
            nearest_event = None
            nearest_days  = 999
            row_date      = datetime.strptime(row["month"], "%Y-%m")
            for evt in BBW_EVENTS:
                evt_date = datetime.strptime(evt["date"][:7], "%Y-%m")
                diff     = abs((row_date - evt_date).days)
                if diff < nearest_days:
                    nearest_days  = diff
                    nearest_event = evt["name"]

            spikes.append({
                "month":         row["month"],
                "subreddit":     subreddit,
                "post_count":    row["post_count"],
                "rolling_avg":   round(avg, 1),
                "spike_ratio":   round(ratio, 2),
                "nearest_event": nearest_event,
                "days_from_event": nearest_days,
            })

    return spikes


def generate_months(start, end):
    """Yield (year, month) tuples from start to end."""
    current = start.replace(day=1)
    while current < end:
        yield current.year, current.month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    log("=" * 60)
    log("BEARWATCH Reddit Historical Pull (Arctic Shift)")
    log(f"Period: {START_DATE.strftime('%Y-%m')} → {END_DATE.strftime('%Y-%m')}")
    log(f"Subreddits: {', '.join(SUBREDDITS)}")
    log("=" * 60)

    all_monthly = []
    all_raw     = {}
    all_spikes  = []

    for subreddit in SUBREDDITS:
        log(f"\nPulling r/{subreddit} ...")
        sub_monthly = []
        all_raw[subreddit] = {}

        months = list(generate_months(START_DATE, END_DATE))
        total  = len(months)

        for idx, (year, month) in enumerate(months):
            month_key = f"{year}-{month:02d}"
            log(f"  [{idx+1}/{total}] r/{subreddit} {month_key} ...")

            posts = fetch_posts_month(subreddit, year, month)
            metrics = aggregate_posts(posts)

            row = {
                "month":      month_key,
                "subreddit":  subreddit,
                **metrics,
            }
            sub_monthly.append(row)
            all_monthly.append(row)
            all_raw[subreddit][month_key] = {
                "metrics": metrics,
                "post_count_raw": len(posts),
            }

            log(
                f"    posts={metrics['post_count']:3d}  "
                f"avg_score={metrics['avg_score']:5.1f}  "
                f"comments={metrics['total_comments']:4d}  "
                f"top: {metrics['top_post_title'][:50]}"
            )
            time.sleep(SLEEP)

        # Detect spikes for this subreddit
        spikes = detect_spikes(sub_monthly, subreddit)
        all_spikes.extend(spikes)
        if spikes:
            log(f"\n  *** SPIKES detected in r/{subreddit} ***")
            for s in spikes:
                log(f"    {s['month']}: {s['spike_ratio']}x baseline — nearest event: {s['nearest_event']} ({s['days_from_event']}d away)")

    # ── Save monthly CSV ──────────────────────────────────────
    fieldnames = [
        "month", "subreddit", "post_count", "total_score",
        "avg_score", "total_comments", "avg_comments",
        "avg_upvote_ratio", "top_post_title", "top_post_score"
    ]
    with open(MONTHLY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(all_monthly, key=lambda r: (r["subreddit"], r["month"])))
    log(f"\nMonthly CSV saved → {MONTHLY_CSV} ({len(all_monthly)} rows)")

    # ── Save raw JSON ─────────────────────────────────────────
    with open(RAW_JSON, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period": {
                "start": START_DATE.strftime("%Y-%m"),
                "end":   END_DATE.strftime("%Y-%m"),
            },
            "subreddits": all_raw,
        }, f, indent=2)
    log(f"Raw JSON saved → {RAW_JSON}")

    # ── Save spikes ───────────────────────────────────────────
    with open(SPIKES_JSON, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_spikes": len(all_spikes),
            "spikes":       all_spikes,
        }, f, indent=2)
    log(f"Spikes JSON saved → {SPIKES_JSON} ({len(all_spikes)} spikes detected)")

    # ── Print summary table ───────────────────────────────────
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    for sub in SUBREDDITS:
        sub_rows = [r for r in all_monthly if r["subreddit"] == sub]
        if not sub_rows:
            continue
        total_posts    = sum(r["post_count"]     for r in sub_rows)
        total_comments = sum(r["total_comments"] for r in sub_rows)
        avg_monthly    = round(total_posts / len(sub_rows), 1)
        peak_row       = max(sub_rows, key=lambda r: r["post_count"])
        sub_spikes     = [s for s in all_spikes if s["subreddit"] == sub]
        log(f"\nr/{sub}")
        log(f"  Total posts (3yr):   {total_posts:,}")
        log(f"  Total comments:      {total_comments:,}")
        log(f"  Avg posts/month:     {avg_monthly}")
        log(f"  Peak month:          {peak_row['month']} ({peak_row['post_count']} posts)")
        log(f"  Spikes detected:     {len(sub_spikes)}")

    log("\nDone. Paste terminal output into claude.ai chat to update the dashboard.")
    log("=" * 60)


if __name__ == "__main__":
    run()
