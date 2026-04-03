"""
BEARWATCH — Wayback Machine Traffic Proxy
Uses the Wayback Machine CDX API to pull monthly crawl/snapshot counts
for buildabear.com going back to January 2019.

Crawl frequency correlates with site activity and content changes —
more crawls = more pages changing = more commercial activity.

Saves to: data/wayback_monthly.csv and data/wayback_latest.json
Detects spikes above 1.4x rolling average.

CDX API docs: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server

Run:
    python3 scripts/fetch_wayback_traffic.py

No credentials needed — completely free API.
"""

import csv
import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict

import requests

# ── CONFIG ────────────────────────────────────────────────────
CDX_API = "https://web.archive.org/cdx/search/cdx"

TARGETS = [
    {"url": "buildabear.com/*", "label": "buildabear.com"},
    {"url": "shop.buildabear.com/*", "label": "shop.buildabear.com"},
]

START_YEAR = 2019
START_MONTH = 1

SPIKE_THRESHOLD = 1.4
SPIKE_WINDOW = 3  # months for rolling average

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
MONTHLY_CSV = os.path.join(DATA_DIR, "wayback_monthly.csv")
LATEST_JSON = os.path.join(DATA_DIR, "wayback_latest.json")
LOG_FILE    = os.path.join(DATA_DIR, "..", "logs", "wayback_scraper.log")

USER_AGENT = "bearwatch-demand-tracker/1.0 (research)"
# ─────────────────────────────────────────────────────────────


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def generate_months(start_year, start_month):
    """Yield (year, month) tuples from start to now."""
    now = datetime.now(timezone.utc)
    year, month = start_year, start_month
    while (year, month) <= (now.year, now.month):
        yield year, month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def fetch_cdx_month_count(url_pattern, year, month):
    """
    Use CDX API with showNumPages to get the count of captures
    for a given URL pattern in a specific month.

    Falls back to a paginated count if showNumPages is unreliable.
    """
    # Build the timestamp range for this month
    from_ts = f"{year}{month:02d}01000000"
    if month == 12:
        to_ts = f"{year + 1}0101000000"
    else:
        to_ts = f"{year}{month + 1:02d}01000000"

    # Use matchType=prefix with collapse=timestamp:6 to get unique URL+month combos
    # and output=json with limit=-1 counting approach
    params = {
        "url": url_pattern,
        "from": from_ts,
        "to": to_ts,
        "output": "json",
        "fl": "timestamp,statuscode",
        "collapse": "urlkey",  # deduplicate by unique URL
        "limit": 10000,
        "showResumeKey": "false",
    }

    try:
        resp = requests.get(
            CDX_API,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=60,
        )

        if resp.status_code == 429:
            log(f"  Rate limited — waiting 30s")
            time.sleep(30)
            resp = requests.get(
                CDX_API,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=60,
            )

        resp.raise_for_status()
        data = resp.json()

        # First row is headers, rest are data
        if len(data) <= 1:
            return 0, 0

        rows = data[1:]  # skip header row
        total = len(rows)
        ok_count = sum(1 for r in rows if len(r) >= 2 and str(r[1]).startswith("2"))
        return total, ok_count

    except Exception as e:
        log(f"  CDX error for {url_pattern} {year}-{month:02d}: {e}")
        return 0, 0


def detect_spikes(monthly_rows, window=SPIKE_WINDOW, threshold=SPIKE_THRESHOLD):
    """Detect months where total_snapshots > threshold * rolling avg."""
    sorted_rows = sorted(monthly_rows, key=lambda r: r["month"])
    spikes = []

    for i, row in enumerate(sorted_rows):
        if i < window:
            continue
        prior = sorted_rows[i - window:i]
        avg = sum(p["total_snapshots"] for p in prior) / window
        if avg == 0:
            continue
        ratio = row["total_snapshots"] / avg
        if ratio >= threshold:
            spikes.append({
                "month": row["month"],
                "total_snapshots": row["total_snapshots"],
                "rolling_avg": round(avg, 1),
                "spike_ratio": round(ratio, 2),
            })

    return spikes


def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    log("=" * 60)
    log("BEARWATCH Wayback Machine Traffic Proxy")
    log(f"Targets: {[t['label'] for t in TARGETS]}")
    log(f"Period: {START_YEAR}-{START_MONTH:02d} → now")
    log("=" * 60)

    months = list(generate_months(START_YEAR, START_MONTH))
    all_rows = []

    for target in TARGETS:
        url_pattern = target["url"]
        label = target["label"]
        log(f"\nFetching {label} ({url_pattern}) ...")

        for idx, (year, month) in enumerate(months):
            month_key = f"{year}-{month:02d}"
            log(f"  [{idx + 1}/{len(months)}] {label} {month_key} ...")

            total, ok_count = fetch_cdx_month_count(url_pattern, year, month)

            row = {
                "month": month_key,
                "target": label,
                "total_snapshots": total,
                "ok_snapshots": ok_count,
            }
            all_rows.append(row)

            if total > 0:
                log(f"    snapshots={total:,}  (2xx={ok_count:,})")
            else:
                log(f"    no data")

            time.sleep(1.5)  # be polite to the API

    # ── Save CSV ──────────────────────────────────────────────
    fieldnames = ["month", "target", "total_snapshots", "ok_snapshots"]
    with open(MONTHLY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(all_rows, key=lambda r: (r["target"], r["month"])))
    log(f"\nCSV saved → {MONTHLY_CSV} ({len(all_rows)} rows)")

    # ── Detect spikes (on main domain) ────────────────────────
    main_rows = [r for r in all_rows if r["target"] == "buildabear.com"]
    spikes = detect_spikes(main_rows)

    # ── Compute summary stats ─────────────────────────────────
    main_sorted = sorted(main_rows, key=lambda r: r["month"])
    peak_row = max(main_rows, key=lambda r: r["total_snapshots"]) if main_rows else {}
    recent_6 = main_sorted[-6:] if len(main_sorted) >= 6 else main_sorted
    prior_6 = main_sorted[-12:-6] if len(main_sorted) >= 12 else main_sorted[:6]

    recent_avg = sum(r["total_snapshots"] for r in recent_6) / max(len(recent_6), 1)
    prior_avg = sum(r["total_snapshots"] for r in prior_6) / max(len(prior_6), 1)
    trend_pct = round(((recent_avg - prior_avg) / prior_avg) * 100, 1) if prior_avg > 0 else 0

    # Latest month
    latest_month = main_sorted[-1] if main_sorted else {}
    prev_month = main_sorted[-2] if len(main_sorted) >= 2 else {}
    mom_change = None
    if latest_month and prev_month and prev_month["total_snapshots"] > 0:
        mom_change = round(
            ((latest_month["total_snapshots"] - prev_month["total_snapshots"])
             / prev_month["total_snapshots"]) * 100, 1
        )

    # ── Save JSON ─────────────────────────────────────────────
    latest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": {
            "start": f"{START_YEAR}-{START_MONTH:02d}",
            "end": main_sorted[-1]["month"] if main_sorted else "—",
        },
        "latest_month": latest_month.get("month", "—"),
        "latest_snapshots": latest_month.get("total_snapshots", 0),
        "mom_change_pct": mom_change,
        "6mo_avg": round(recent_avg, 1),
        "trend_vs_prior_6mo_pct": trend_pct,
        "peak_month": peak_row.get("month", "—"),
        "peak_snapshots": peak_row.get("total_snapshots", 0),
        "spikes": spikes,
        "spike_count": len(spikes),
        "monthly_series": [
            {"month": r["month"], "snapshots": r["total_snapshots"]}
            for r in main_sorted
        ],
    }

    with open(LATEST_JSON, "w") as f:
        json.dump(latest, f, indent=2)
    log(f"JSON saved → {LATEST_JSON}")

    # ── Summary ───────────────────────────────────────────────
    log("\n" + "=" * 60)
    log("SUMMARY — buildabear.com Wayback crawl activity")
    log("=" * 60)
    log(f"  Period:              {START_YEAR}-{START_MONTH:02d} → {latest_month.get('month', '—')}")
    log(f"  Total months:        {len(main_rows)}")
    log(f"  Peak month:          {peak_row.get('month', '—')} ({peak_row.get('total_snapshots', 0):,} snapshots)")
    log(f"  Latest month:        {latest_month.get('month', '—')} ({latest_month.get('total_snapshots', 0):,} snapshots)")
    if mom_change is not None:
        log(f"  MoM change:          {'+' if mom_change >= 0 else ''}{mom_change}%")
    log(f"  6-month avg:         {recent_avg:,.1f}")
    log(f"  Trend (6mo vs prior):{'+' if trend_pct >= 0 else ''}{trend_pct}%")
    log(f"  Spikes detected:     {len(spikes)}")
    if spikes:
        for s in spikes:
            log(f"    {s['month']}: {s['spike_ratio']}x rolling avg ({s['total_snapshots']:,} snapshots)")
    log("=" * 60)


if __name__ == "__main__":
    run()
