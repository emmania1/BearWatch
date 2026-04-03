#!/usr/bin/env python3
"""
BEARWATCH — Live Data Patcher (no API credentials needed)
Uses Reddit's public JSON endpoints to pull subscriber counts and
weekly post volume for r/buildabear, r/squishmallows, r/stuffedanimals.

Writes data/reddit_latest.json and data/dashboard_data.json,
then patches bearwatch.html signal cards with real numbers.

Usage:
  python3 update_data.py

Based on the Warhammer demand dashboard update_data.py pattern.
"""

import csv
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Config ───────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
HTML_FILE   = Path("/Users/emmania/Desktop/bearwatch_MASTER.html")
DATA_DIR    = BASE_DIR / "data"
LATEST_JSON = DATA_DIR / "reddit_latest.json"
DASH_JSON   = DATA_DIR / "dashboard_data.json"
LOG_FILE    = BASE_DIR / "logs" / "update_data.log"

USER_AGENT  = "bearwatch-demand-tracker/1.0"

SUBREDDITS = ["buildabear", "squishmallows", "stuffedanimals", "bluey", "TheMandalorianTV", "plushies", "jellycat"]

KEYWORDS = ["walmart", "sold out", "grogu", "bluey", "waitlist"]
KEYWORD_MAX_SCALE = 15  # posts/week — bar is 100% at this value


# ── Logging ──────────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Reddit public JSON fetchers ──────────────────────────────────────────────
def fetch_subscribers(sub_name):
    """Fetch subscriber count via public /about.json endpoint."""
    try:
        r = requests.get(
            f"https://www.reddit.com/r/{sub_name}/about.json",
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        r.raise_for_status()
        n = r.json()["data"]["subscribers"]
        log(f"  r/{sub_name}: {n:,} subscribers")
        return n
    except Exception as e:
        log(f"  r/{sub_name} subscriber ERROR: {e}")
        return None


def fetch_recent_posts(sub_name, limit=100):
    """Fetch recent posts via public /new.json endpoint, filter to last 7 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    posts = []
    try:
        r = requests.get(
            f"https://www.reddit.com/r/{sub_name}/new.json?limit={limit}",
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        r.raise_for_status()
        children = r.json().get("data", {}).get("children", [])
        for child in children:
            p = child["data"]
            created = datetime.fromtimestamp(p["created_utc"], tz=timezone.utc)
            if created < cutoff:
                break
            posts.append({
                "id":           p["id"],
                "title":        p["title"],
                "score":        p["score"],
                "upvote_ratio": p.get("upvote_ratio", 0),
                "num_comments": p["num_comments"],
                "created_utc":  created.isoformat(),
            })
        log(f"  r/{sub_name}: {len(posts)} posts in last 7 days")
    except Exception as e:
        log(f"  r/{sub_name} posts ERROR: {e}")
    return posts


def aggregate_posts(posts):
    """Summarise posts into weekly metrics."""
    if not posts:
        return {
            "post_count": 0, "total_score": 0, "avg_score": 0,
            "total_comments": 0, "avg_comments": 0, "avg_upvote_ratio": 0,
        }
    scores   = [p["score"] for p in posts]
    comments = [p["num_comments"] for p in posts]
    ratios   = [p["upvote_ratio"] for p in posts]
    return {
        "post_count":       len(posts),
        "total_score":      sum(scores),
        "avg_score":        round(sum(scores) / len(scores), 1),
        "total_comments":   sum(comments),
        "avg_comments":     round(sum(comments) / len(comments), 1),
        "avg_upvote_ratio": round(sum(ratios) / len(ratios), 3),
    }


# ── Keyword signal tracker ───────────────────────────────────────────────────
def fetch_keyword_signals(sub="buildabear"):
    """Count posts mentioning each keyword in r/{sub} in the past 7 days."""
    results = {}
    for kw in KEYWORDS:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/search.json",
                params={"q": kw, "restrict_sr": 1, "t": "week",
                        "limit": 100, "sort": "new"},
                headers={"User-Agent": USER_AGENT},
                timeout=15,
            )
            r.raise_for_status()
            posts = [p for p in r.json().get("data", {}).get("children", [])
                     if p["data"].get("subreddit", "").lower() == sub.lower()]
            key = kw.replace(" ", "_")
            results[key] = len(posts)
            log(f"  r/{sub} '{kw}': {len(posts)} posts this week")
            time.sleep(0.8)
        except Exception as e:
            log(f"  keyword '{kw}' ERROR: {e}")
            results[kw.replace(" ", "_")] = 0
    return results


def read_walmart_tracker():
    """Read walmart_tracker.csv rows for HTML patching."""
    csv_path = DATA_DIR / "walmart_tracker.csv"
    if not csv_path.exists():
        return []
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))


# ── SimilarWeb CSV reader ────────────────────────────────────────────────────
def read_web_traffic():
    """Read the most recent row from web_traffic_manual.csv."""
    csv_path = DATA_DIR / "web_traffic_manual.csv"
    if not csv_path.exists():
        log(f"  web_traffic_manual.csv not found — skipping SimilarWeb patch")
        return None
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    latest = rows[-1]
    prev   = rows[-2] if len(rows) >= 2 else None

    visits = int(float(latest["monthly_visits"]))
    pages  = float(latest["pages_per_visit"])
    secs   = int(float(latest["avg_duration_sec"]))
    bounce = float(latest["bounce_rate"])
    dt     = datetime.strptime(latest["date"], "%Y-%m-%d")
    month_label = dt.strftime("%b %Y")

    visits_str = f"{visits/1_000_000:.1f}M" if visits >= 1_000_000 else (
                 f"{visits//1000}K" if visits >= 1000 else str(visits))
    m, s   = divmod(secs, 60)
    dur_str    = f"{m}:{s:02d}"
    bounce_str = f"{bounce*100:.1f}%"
    pages_str  = str(round(pages, 2))

    mom_str = ""
    if prev:
        prev_v = int(float(prev["monthly_visits"]))
        pct    = round((visits - prev_v) / prev_v * 100, 1)
        sign   = "+" if pct >= 0 else ""
        mom_str = f"{sign}{pct}% MoM"

    log(f"  SimilarWeb ({month_label}): {visits_str} visits · {pages_str} pages · {dur_str} · {bounce_str} bounce{' · ' + mom_str if mom_str else ''}")
    return {
        "visits_str": visits_str,
        "pages_str":  pages_str,
        "dur_str":    dur_str,
        "bounce_str": bounce_str,
        "month_label": month_label,
        "mom_str":    mom_str,
    }


def _sw_sub(html, tag, value):
    """Replace content between <!-- SW:tag --> and <!-- /SW:tag --> anchors."""
    return re.sub(
        rf'(<!-- SW:{tag} -->).*?(<!-- /SW:{tag} -->)',
        rf'\g<1>{value}\g<2>',
        html,
        flags=re.DOTALL,
    )


def _anchor_sub(html, tag, value):
    """Generic anchor substitution for <!-- TAG -->value<!-- /TAG --> patterns."""
    return re.sub(
        rf'(<!-- {re.escape(tag)} -->).*?(<!-- /{re.escape(tag)} -->)',
        rf'\g<1>{value}\g<2>',
        html, flags=re.DOTALL,
    )


def patch_community_table(html, latest):
    """Patch community comparison table with current subs + weekly posts."""
    sub_map = {
        "buildabear":      ("buildabear",      100),
        "squishmallows":   ("squishmallows",   None),
        "plushies":        ("plushies",        100),
        "bluey":           ("bluey",           100),
        "TheMandalorianTV":("TheMandalorianTV", None),
    }
    for tag_key, (sub_name, _) in sub_map.items():
        d = latest.get(sub_name, {})
        subs = d.get("subscribers")
        posts = d.get("post_count", 0)
        if subs:
            html = _anchor_sub(html, f"R:subs:{tag_key}", f"{subs:,}")
        p7d_str = f"{posts:,}" if posts < 100 else "100+"
        html = _anchor_sub(html, f"R:p7d:{tag_key}", p7d_str)
    return html


def patch_mando_pulse(html, mando_data, week_ending):
    """Patch Grogu fandom pulse card."""
    posts = mando_data.get("post_count", 0)
    # Percentage of 60-post scale (threshold is at 30 = 50%)
    pct = min(int(posts / 60 * 100), 100)
    html = _anchor_sub(html, "MANDO:posts", str(posts))
    html = _anchor_sub(html, "MANDO:pct",   f"{pct}%")
    html = _anchor_sub(html, "MANDO:week",  week_ending)
    return html


def patch_keyword_signals(html, keywords, week_ending):
    """Patch keyword signal bars."""
    for kw_key, count in keywords.items():
        pct = min(int(count / KEYWORD_MAX_SCALE * 100), 100)
        html = _anchor_sub(html, f"KW:{kw_key}", str(count))
        html = _anchor_sub(html, f"KW:pct:{kw_key}", f"{pct}%")
    html = _anchor_sub(html, "KW:week", week_ending)
    return html


def patch_walmart_table(html, rows):
    """Patch Walmart tracker table from CSV rows."""
    if not rows:
        return html  # keep placeholder

    tag_html = f"<span class='tag tag-live'>Live — {len(rows)} check{'s' if len(rows)>1 else ''}</span>"
    html = _anchor_sub(html, "WALMART:tag", f"Live — {len(rows)} check{'s' if len(rows)>1 else ''}")

    tr_rows = []
    for i, row in enumerate(rows):
        prev_reviews = int(rows[i-1].get("review_count", 0)) if i > 0 else 0
        curr_reviews = int(row.get("review_count", 0)) if row.get("review_count","").isdigit() else 0
        delta = curr_reviews - prev_reviews if i > 0 else 0
        delta_str = f"+{delta}" if delta > 0 else (str(delta) if delta < 0 else "—")
        delta_color = "val-green" if delta > 0 else ("val-red" if delta < 0 else "val-muted")
        in_stock = row.get("in_stock", "?")
        stock_color = "val-green" if in_stock.strip().upper() in ("Y","YES","TRUE","1") else "val-red"
        tr_rows.append(
            f'<tr>'
            f'<td class="td-mono">{row.get("date","")}</td>'
            f'<td class="td-mono">{row.get("review_count","—")}</td>'
            f'<td class="td-mono {delta_color}">{delta_str}</td>'
            f'<td class="td-mono">{row.get("avg_rating","—")}</td>'
            f'<td class="td-mono {stock_color}">{in_stock}</td>'
            f'<td class="td-mono">{row.get("stores_pickup","—")}</td>'
            f'<td style="font-size:11px;color:var(--muted)">{row.get("notes","")}</td>'
            f'</tr>'
        )
    rows_html = "\n".join(tr_rows)
    html = re.sub(
        r'<!-- WALMART:rows -->.*?<!-- /WALMART:rows -->',
        f'<!-- WALMART:rows -->\n{rows_html}\n<!-- /WALMART:rows -->',
        html, flags=re.DOTALL,
    )
    return html


# ── HTML patching ────────────────────────────────────────────────────────────
def patch_html(bbw_data, sqm_data, week_ending):
    """Patch bearwatch_MASTER.html signal cards with real Reddit data."""
    if not HTML_FILE.exists():
        log(f"  ERROR: {HTML_FILE} not found — skipping patch")
        return

    html = HTML_FILE.read_text(encoding="utf-8")
    subs         = bbw_data.get("subscribers") or 0
    posts_week   = bbw_data.get("post_count") or 0
    avg_score    = bbw_data.get("avg_score") or 0
    total_comments = bbw_data.get("total_comments") or 0
    sqm_posts    = sqm_data.get("post_count") or 1

    # --- Section label: update week-ending date ---
    html = re.sub(
        r'Live Signals — week ending \d{4}-\d{2}-\d{2}',
        f'Live Signals — week ending {week_ending}',
        html,
    )

    # --- Signal card 1: r/buildabear this week ---
    # Update post count (sig-value)
    html = re.sub(
        r'(<div class="sig-label">r/buildabear — this week</div>\s*<div class="sig-value">)\d+(</div>)',
        rf'\g<1>{posts_week}\g<2>',
        html,
    )
    # Update sub line: "posts · N members · N comments"
    html = re.sub(
        r'(<div class="sig-sub">)posts · [\d,]+ members · [\d,]+ comments(</div>)',
        rf'\g<1>posts · {subs:,} members · {total_comments:,} comments\g<2>',
        html,
    )

    # --- Signal card 2: BBW vs SQM ratio ---
    bbw_mo = bbw_data.get("post_count") or 1  # weekly as proxy; chart uses monthly constants
    ratio = round(bbw_mo / max(sqm_posts, 1), 1)
    html = re.sub(
        r'(<div class="sig-label">BBW vs Squishmallows \(posts/mo\)</div>\s*<div class="sig-value up">)[\d.]+×(</div>)',
        rf'\g<1>{ratio}×\g<2>',
        html,
    )

    # --- Signal card 3: avg upvote score ---
    html = re.sub(
        r'(<div class="sig-label">Avg upvote score — engagement quality</div>\s*<div class="sig-value gold">)[\d.]+(</div>)',
        rf'\g<1>{avg_score}\g<2>',
        html,
    )

    # --- Q1 answer value ---
    subs_k = f"{subs // 1000}K" if subs >= 1000 else str(subs)
    html = re.sub(
        r'(<div class="q-answer-value">YES — community engagement at all-time highs\. Avg score )[\d.]+ \(was 28 in 2022\)\. [\w,]+ subs, \d+ posts/wk\.',
        rf'\g<1>{int(avg_score)} (was 28 in 2022). {subs_k} subs, {posts_week} posts/wk.',
        html,
    )

    # --- Weekly brief modal: update week-ending line ---
    html = re.sub(
        r'(Summary \(week ending )\d{4}-\d{2}-\d{2}(\):)',
        rf'\g<1>{week_ending}\g<2>',
        html,
    )

    # --- SimilarWeb values from CSV ---
    sw = read_web_traffic()
    if sw:
        html = _sw_sub(html, "visits_val",  sw["visits_str"])
        html = _sw_sub(html, "visits_date", sw["month_label"])
        html = _sw_sub(html, "kv_date",     sw["month_label"])
        html = _sw_sub(html, "kv_date2",    sw["month_label"])
        html = _sw_sub(html, "kv_date3",    sw["month_label"])
        html = _sw_sub(html, "kv_date4",    sw["month_label"])
        html = _sw_sub(html, "kv_visits",   sw["visits_str"])
        html = _sw_sub(html, "kv_pages",    sw["pages_str"])
        html = _sw_sub(html, "kv_duration", sw["dur_str"])
        html = _sw_sub(html, "kv_bounce",   sw["bounce_str"])
        if sw["mom_str"]:
            html = _sw_sub(html, "kv_mom", sw["mom_str"])

    HTML_FILE.write_text(html, encoding="utf-8")
    log(f"bearwatch_MASTER.html patched ({HTML_FILE.stat().st_size // 1024}KB)")


def patch_live_signals(html, latest, keywords, week_ending):
    """Patch community table, mando pulse, keywords, walmart."""
    html = patch_community_table(html, latest)
    mando = latest.get("TheMandalorianTV", {})
    html = patch_mando_pulse(html, mando, week_ending)
    html = patch_keyword_signals(html, keywords, week_ending)
    walmart_rows = read_walmart_tracker()
    html = patch_walmart_table(html, walmart_rows)
    return html


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    log("=" * 60)
    log("BEARWATCH update_data.py starting (public JSON, no credentials)")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    week_ending = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    latest = {}

    for sub_name in SUBREDDITS:
        log(f"Scraping r/{sub_name} ...")
        subscribers = fetch_subscribers(sub_name)
        time.sleep(1.0)  # polite rate limiting
        posts = fetch_recent_posts(sub_name)
        time.sleep(1.0)
        metrics = aggregate_posts(posts)

        latest[sub_name] = {
            "week_ending": week_ending,
            "subreddit":   sub_name,
            "subscribers": subscribers,
            **metrics,
        }

    # Write reddit_latest.json
    reddit_snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_ending":  week_ending,
        "subreddits":   latest,
    }
    with open(LATEST_JSON, "w") as f:
        json.dump(reddit_snapshot, f, indent=2)
    log(f"Saved → {LATEST_JSON}")

    # Build dashboard_data.json
    bbw = latest.get("buildabear", {})
    sqm = latest.get("squishmallows", {})

    dashboard = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_ending":  week_ending,
        "q1_consumer_demand": {
            "status":  "live",
            "sources": ["reddit"],
            "metrics": {
                "bbw_reddit_subscribers": bbw.get("subscribers"),
                "bbw_reddit_posts_week":  bbw.get("post_count"),
                "bbw_reddit_comments":    bbw.get("total_comments"),
                "bbw_reddit_avg_score":   bbw.get("avg_score"),
                "sqm_reddit_subscribers": sqm.get("subscribers"),
                "sqm_reddit_posts_week":  sqm.get("post_count"),
            },
        },
        "q2_ip_spikes": {
            "status": "monitoring",
            "active_alerts": [],
            "alert_count": 0,
            "releases_in_window": [],
            "competitor_sqm_posts": sqm.get("post_count"),
        },
        "q3_commercial": {
            "status": "manual",
            "commercial_locations": 178,
            "walmart_trial_stores": 1500,
            "uk_locations": 35,
            "germany_locations": 4,
        },
        "q4_macro": {
            "status": "deferred",
            "store_traffic": None,
            "note": "Oleg: misleading near-term. Revisit Q3 2026.",
        },
        "reddit_raw": reddit_snapshot,
        "alerts": {"trend_spikes": [], "ir_high_signal": []},
    }

    with open(DASH_JSON, "w") as f:
        json.dump(dashboard, f, indent=2)
    log(f"Saved → {DASH_JSON}")

    # Keyword signals
    log("Fetching keyword signals for r/buildabear ...")
    keywords = fetch_keyword_signals("buildabear")

    # Patch HTML
    log("Patching bearwatch_MASTER.html with live data ...")
    sqm = latest.get("squishmallows", {})
    patch_html(bbw, sqm, week_ending)

    # Patch live signals (community, mando, keywords, walmart)
    log("Patching live signals ...")
    html = HTML_FILE.read_text(encoding="utf-8")
    html = patch_live_signals(html, latest, keywords, week_ending)
    HTML_FILE.write_text(html, encoding="utf-8")
    log(f"Live signals patched.")

    # Print summary
    log("")
    log("── SUMMARY ─────────────────────────────────────────")
    for sub_name, d in latest.items():
        s = d.get("subscribers")
        p = d.get("post_count", 0)
        c = d.get("total_comments", 0)
        s_str = f"{s:,}" if s else "?"
        log(f"  r/{sub_name}: {s_str} subs · {p} posts · {c} comments (7d)")
    log("─────────────────────────────────────────────────────")
    log("Done.")
    log("=" * 60)


if __name__ == "__main__":
    main()
