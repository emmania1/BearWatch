"""
BEARWATCH — Google Trends Scraper (Background / IP Spike Alerts Only)
NOT used as a general demand indicator (per Oleg).
ONLY fires when within 14 days of a release calendar event,
or when a spike threshold is crossed relative to baseline.

Oleg use case: "check online statistics around the time of releases"
Baby Yoda benchmark: ~4-5x vs 90-day baseline over a 2-week window.

Setup:
  pip install pytrends pandas

Run:
  python scrapers/trends_scraper.py

NOTE: pytrends can be rate-limited by Google. Add retry logic if needed.
If you get 429 errors, increase SLEEP_BETWEEN_REQUESTS.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
from pytrends.request import TrendReq

# ── CONFIG ────────────────────────────────────────────────────
SLEEP_BETWEEN_REQUESTS = 5   # seconds between pytrends calls (avoid 429)
SPIKE_THRESHOLD        = 1.5  # flag if current week > 1.5x 90-day avg
RELEASE_WINDOW_DAYS    = 21   # start watching 21 days before a release date

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
TRENDS_CSV  = os.path.join(DATA_DIR, "trends_history.csv")
ALERTS_JSON = os.path.join(DATA_DIR, "trends_alerts.json")
LATEST_JSON = os.path.join(DATA_DIR, "trends_latest.json")
LOG_FILE    = os.path.join(DATA_DIR, "..", "logs", "trends_scraper.log")

# ── IP RELEASE CALENDAR ───────────────────────────────────────
# Add new releases here. Format: "YYYY-MM-DD"
# Research upcoming ones from BBW.com, press releases, film calendars.
IP_RELEASES = [
    {"name": "Pokemon Collaboration",   "date": "2026-05-01",  "keywords": ["Build-A-Bear Pokemon"],         "heat": "high"},
    {"name": "Star Wars Tie-in",        "date": "2026-06-15",  "keywords": ["Build-A-Bear Star Wars"],       "heat": "high"},
    {"name": "Animated Film TBD",       "date": "2026-09-01",  "keywords": ["Build-A-Bear"],                 "heat": "medium"},
    # Historical reference (do not delete — used as benchmark)
    {"name": "Baby Yoda (benchmark)",   "date": "2020-11-15",  "keywords": ["Build-A-Bear Baby Yoda", "Baby Yoda plush"], "heat": "benchmark"},
]

# General keywords tracked for baseline (background, not displayed prominently)
BASELINE_KEYWORDS = [
    "Build-A-Bear",
    "Build-A-Bear Workshop",
]
# ─────────────────────────────────────────────────────────────


def log(msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def releases_in_window(window_days=RELEASE_WINDOW_DAYS):
    """Return releases whose date falls within the next window_days."""
    today    = datetime.now(timezone.utc).date()
    upcoming = []
    for r in IP_RELEASES:
        if r["heat"] == "benchmark":
            continue
        release_date = datetime.strptime(r["date"], "%Y-%m-%d").date()
        days_out     = (release_date - today).days
        if -7 <= days_out <= window_days:   # -7 = already happened this week
            upcoming.append({**r, "days_out": days_out})
    return upcoming


def fetch_trends(keywords, timeframe="today 3-m"):
    """Fetch Google Trends interest over time for given keywords."""
    pytrends = TrendReq(hl="en-US", tz=360)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    try:
        pytrends.build_payload(keywords, timeframe=timeframe)
        df = pytrends.interest_over_time()
        if df.empty:
            log(f"  No data returned for {keywords}")
            return None
        if "isPartial" in df.columns:
            df = df[~df["isPartial"]]
        return df
    except Exception as e:
        log(f"  ERROR fetching trends for {keywords}: {e}")
        return None


def compute_spike(series, window=90):
    """
    Compare most recent value to rolling mean of prior `window` days.
    Returns multiplier (e.g. 4.2 = 4.2x baseline).
    """
    if series is None or len(series) < 2:
        return None
    baseline = series[:-1].mean()
    current  = series.iloc[-1]
    if baseline == 0:
        return None
    return round(current / baseline, 2)


def run_baseline_pull():
    """Pull 3 months of baseline data for general BBW keywords."""
    log("Pulling 90-day baseline for general BBW keywords ...")
    df = fetch_trends(BASELINE_KEYWORDS[:1], timeframe="today 3-m")  # 1 keyword at a time
    if df is None:
        return None

    col = BASELINE_KEYWORDS[0]
    if col not in df.columns:
        return None

    # Save to history CSV
    history_rows = []
    for idx, row in df.iterrows():
        history_rows.append({
            "date":    idx.strftime("%Y-%m-%d"),
            "keyword": col,
            "value":   int(row[col]),
        })

    new_df = pd.DataFrame(history_rows)

    if os.path.exists(TRENDS_CSV):
        existing = pd.read_csv(TRENDS_CSV)
        # Remove overlap
        existing = existing[~(
            (existing["keyword"] == col) &
            (existing["date"].isin(new_df["date"]))
        )]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(["keyword", "date"])
    combined.to_csv(TRENDS_CSV, index=False)
    log(f"  Saved {len(history_rows)} rows to {TRENDS_CSV}")

    spike = compute_spike(df[col])
    log(f"  Spike ratio vs 90-day baseline: {spike}x")
    return spike


def run_release_monitoring(upcoming_releases):
    """For each upcoming release, pull keyword-specific trends and check for spike."""
    alerts = []
    for release in upcoming_releases:
        log(f"Monitoring release: {release['name']} ({release['days_out']} days out)")
        kw = release["keywords"][0]
        df = fetch_trends([kw], timeframe="today 3-m")
        if df is None or kw not in df.columns:
            continue

        spike = compute_spike(df[kw])
        log(f"  '{kw}' spike ratio: {spike}x")

        if spike and spike >= SPIKE_THRESHOLD:
            alert = {
                "release":    release["name"],
                "keyword":    kw,
                "spike_ratio": spike,
                "days_out":   release["days_out"],
                "flagged_at": datetime.now(timezone.utc).isoformat(),
                "severity":   "HIGH" if spike >= 3.0 else "WATCH",
            }
            alerts.append(alert)
            log(f"  *** SPIKE ALERT: {spike}x for '{kw}' ***")

    return alerts


def run():
    log("=" * 60)
    log("BEARWATCH Trends scraper starting (background / IP alerts)")

    # Check which releases are in the monitoring window
    upcoming = releases_in_window()
    log(f"Releases in monitoring window: {[r['name'] for r in upcoming]}")

    # Always pull the general baseline
    baseline_spike = run_baseline_pull()

    # Monitor upcoming releases
    alerts = []
    if upcoming:
        alerts = run_release_monitoring(upcoming)
    else:
        log("No releases in window — skipping release monitoring")

    # Save latest snapshot
    latest = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "baseline_spike": baseline_spike,
        "releases_in_window": [r["name"] for r in upcoming],
        "active_alerts":  alerts,
        "alert_count":    len(alerts),
        "note":           "Trends used for IP spike detection only, not general demand signal (per Oleg)"
    }

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ALERTS_JSON, "w") as f:
        json.dump(alerts, f, indent=2)
    with open(LATEST_JSON, "w") as f:
        json.dump(latest, f, indent=2)

    log(f"Alerts saved → {ALERTS_JSON}")
    log(f"Active spike alerts: {len(alerts)}")
    log("Trends scraper complete.")
    log("=" * 60)


if __name__ == "__main__":
    run()
