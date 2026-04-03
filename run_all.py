"""
BEARWATCH — Master Runner
Runs all scrapers in sequence and writes a single data/dashboard_data.json
that the HTML dashboard reads on load.

Run weekly (or manually):
  python run_all.py

Cron schedule (every Sunday at 8am):
  0 8 * * 0 cd /path/to/bearwatch && python run_all.py >> logs/master.log 2>&1
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

DATA_DIR        = os.path.join(os.path.dirname(__file__), "data")
DASHBOARD_JSON  = os.path.join(DATA_DIR, "dashboard_data.json")
LOG_FILE        = os.path.join(os.path.dirname(__file__), "logs", "master.log")

SCRAPERS = [
    "scrapers/reddit_scraper.py",
    "scrapers/trends_scraper.py",
    "scrapers/ir_scraper.py",
]


def log(msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def run_scraper(script_path):
    log(f"Running {script_path} ...")
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True, text=True,
        cwd=os.path.dirname(__file__)
    )
    if result.returncode != 0:
        log(f"  ERROR in {script_path}:\n{result.stderr[:500]}")
        return False
    log(f"  {script_path} completed OK")
    return True


def load_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def build_dashboard_data():
    """
    Merge all scraper outputs into a single JSON the dashboard reads.
    """
    reddit  = load_json(os.path.join(DATA_DIR, "reddit_latest.json"))
    trends  = load_json(os.path.join(DATA_DIR, "trends_latest.json"))
    ir      = load_json(os.path.join(DATA_DIR, "ir_latest.json"))
    alerts  = load_json(os.path.join(DATA_DIR, "trends_alerts.json"))

    # Pull key metrics from reddit
    bbw_reddit  = reddit.get("subreddits", {}).get("buildabear", {})
    sqm_reddit  = reddit.get("subreddits", {}).get("squishmallows", {})

    dashboard = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_ending":  reddit.get("week_ending", "—"),

        # ── Q1: Consumer demand ──
        "q1_consumer_demand": {
            "status":   "data_pending",  # update to "live" once scrapers run
            "sources":  ["reddit", "trends_baseline"],
            "metrics": {
                "bbw_reddit_subscribers":  bbw_reddit.get("subscribers"),
                "bbw_reddit_posts_week":   bbw_reddit.get("post_count"),
                "bbw_reddit_comments":     bbw_reddit.get("total_comments"),
                "bbw_reddit_avg_score":    bbw_reddit.get("avg_score"),
                "trends_baseline_spike":   trends.get("baseline_spike"),
            }
        },

        # ── Q2: IP Spikes ──
        "q2_ip_spikes": {
            "status":          "monitoring",
            "active_alerts":   alerts if isinstance(alerts, list) else [],
            "alert_count":     len(alerts) if isinstance(alerts, list) else 0,
            "releases_in_window": trends.get("releases_in_window", []),
            "competitor_sqm_posts": sqm_reddit.get("post_count"),  # spike in competitor = category heat
        },

        # ── Q3: Commercial scaling ──
        "q3_commercial": {
            "status": "manual",  # updated after earnings / press releases
            "commercial_locations": 178,
            "walmart_trial_stores": 1500,
            "uk_locations":         35,
            "germany_locations":    4,
            "new_high_signal_releases": ir.get("high_signal_new", []),
        },

        # ── Q4: Macro resilience ──
        "q4_macro": {
            "status":        "deferred",
            "store_traffic": None,  # Placer.ai — deferred per Oleg
            "note":          "Oleg: misleading near-term. Revisit Q3 2026.",
        },

        # ── Raw feeds for sparklines ──
        "reddit_raw":  reddit,
        "trends_raw":  trends,
        "ir_raw":      ir,

        # ── Alerts summary ──
        "alerts": {
            "trend_spikes":    alerts if isinstance(alerts, list) else [],
            "ir_high_signal":  ir.get("high_signal_new", []),
        }
    }

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DASHBOARD_JSON, "w") as f:
        json.dump(dashboard, f, indent=2)
    log(f"Dashboard data written → {DASHBOARD_JSON}")
    return dashboard


def run():
    log("=" * 60)
    log("BEARWATCH master runner starting")

    results = {}
    for scraper in SCRAPERS:
        results[scraper] = run_scraper(scraper)

    log("Building combined dashboard_data.json ...")
    data = build_dashboard_data()

    # Print summary
    log("")
    log("── RUN SUMMARY ─────────────────────────────────────")
    for scraper, ok in results.items():
        status = "OK" if ok else "FAILED"
        log(f"  {scraper}: {status}")

    alerts = data.get("alerts", {})
    spike_alerts = alerts.get("trend_spikes", [])
    ir_alerts    = alerts.get("ir_high_signal", [])
    log(f"  Trend spike alerts:   {len(spike_alerts)}")
    log(f"  IR high-signal new:   {len(ir_alerts)}")
    log("─────────────────────────────────────────────────────")
    log("Master runner complete.")
    log("=" * 60)


if __name__ == "__main__":
    run()
