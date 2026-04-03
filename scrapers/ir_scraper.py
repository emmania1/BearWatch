"""
BEARWATCH — Press Release / IR Scraper
Monitors Build-A-Bear's investor relations page and PR Newswire
for new partner announcements, earnings releases, and commercial location news.

Writes to data/press_releases.json and data/ir_latest.json.

Setup:
  pip install requests beautifulsoup4 pandas

Run:
  python scrapers/ir_scraper.py
"""

import json
import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

DATA_DIR        = os.path.join(os.path.dirname(__file__), "..", "data")
RELEASES_JSON   = os.path.join(DATA_DIR, "press_releases.json")
IR_LATEST_JSON  = os.path.join(DATA_DIR, "ir_latest.json")
LOG_FILE        = os.path.join(DATA_DIR, "..", "logs", "ir_scraper.log")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Keywords that indicate a high-signal press release
SIGNAL_KEYWORDS = [
    "partner",
    "commercial",
    "location",
    "store",
    "expansion",
    "walmart",
    "international",
    "license",
    "pokemon",
    "star wars",
    "agreement",
    "collaboration",
    "repurchase",
    "dividend",
    "earnings",
    "revenue",
    "guidance",
]

# BBW IR press release feed
BBW_IR_URL = "https://ir.buildabear.com/news-releases/news-release-details"
BBW_IR_BASE = "https://ir.buildabear.com"

# PR Newswire search (backup)
PRNEWSWIRE_URL = "https://www.prnewswire.com/rss/news-releases-list.rss"


def log(msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def score_release(title, body=""):
    """Score a press release 0-10 based on keyword relevance."""
    text  = (title + " " + body).lower()
    score = sum(1 for kw in SIGNAL_KEYWORDS if kw in text)
    return score


def scrape_bbw_ir():
    """Scrape BBW's investor relations news page."""
    releases = []
    try:
        resp = requests.get(BBW_IR_BASE + "/news-releases", headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # BBW IR uses a standard Q4 CMS — find news links
        news_links = soup.find_all("a", href=re.compile(r"/news-releases/news-release-details"))
        log(f"  BBW IR: found {len(news_links)} news links")

        for link in news_links[:20]:  # latest 20
            title = link.get_text(strip=True)
            url   = BBW_IR_BASE + link.get("href", "")
            sig   = score_release(title)
            releases.append({
                "source":    "BBW IR",
                "title":     title,
                "url":       url,
                "signal_score": sig,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            log(f"    [{sig}/10] {title[:80]}")
            time.sleep(0.5)

    except Exception as e:
        log(f"  ERROR scraping BBW IR: {e}")

    return releases


def scrape_prnewswire_bbw():
    """Scrape PR Newswire RSS for BBW mentions as a backup feed."""
    releases = []
    try:
        # Search PR Newswire for Build-A-Bear
        search_url = "https://www.prnewswire.com/rss/news-releases-list.rss?company=build-a-bear"
        resp = requests.get(search_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "xml")

        items = soup.find_all("item")
        log(f"  PR Newswire RSS: found {len(items)} items")

        for item in items[:15]:
            title    = item.find("title")
            link_tag = item.find("link")
            pub_date = item.find("pubDate")

            title_text = title.get_text(strip=True) if title else ""
            url_text   = link_tag.get_text(strip=True) if link_tag else ""
            date_text  = pub_date.get_text(strip=True) if pub_date else ""

            if "build-a-bear" not in title_text.lower() and "bbw" not in title_text.lower():
                continue

            sig = score_release(title_text)
            releases.append({
                "source":       "PR Newswire",
                "title":        title_text,
                "url":          url_text,
                "date":         date_text,
                "signal_score": sig,
                "scraped_at":   datetime.now(timezone.utc).isoformat(),
            })
            log(f"    [{sig}/10] {title_text[:80]}")

    except Exception as e:
        log(f"  ERROR scraping PR Newswire: {e}")

    return releases


def load_existing_releases():
    if os.path.exists(RELEASES_JSON):
        with open(RELEASES_JSON) as f:
            return json.load(f)
    return []


def deduplicate(existing, new_releases):
    existing_titles = {r["title"] for r in existing}
    added = []
    for r in new_releases:
        if r["title"] not in existing_titles:
            added.append(r)
            existing_titles.add(r["title"])
    return added


def run():
    log("=" * 60)
    log("BEARWATCH IR / Press Release scraper starting")

    existing = load_existing_releases()
    log(f"Existing releases in database: {len(existing)}")

    # Scrape both sources
    bbw_releases = scrape_bbw_ir()
    time.sleep(2)
    pr_releases  = scrape_prnewswire_bbw()

    all_new = bbw_releases + pr_releases
    added   = deduplicate(existing, all_new)
    log(f"New releases found: {len(added)}")

    # Flag high-signal new releases
    high_signal = [r for r in added if r["signal_score"] >= 3]
    if high_signal:
        log(f"*** HIGH-SIGNAL NEW RELEASES: {len(high_signal)} ***")
        for r in high_signal:
            log(f"  [{r['signal_score']}] {r['title']}")

    # Save updated releases list
    all_releases = existing + added
    # Sort by signal score desc
    all_releases.sort(key=lambda x: x.get("signal_score", 0), reverse=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(RELEASES_JSON, "w") as f:
        json.dump(all_releases, f, indent=2)
    log(f"Releases saved → {RELEASES_JSON} ({len(all_releases)} total)")

    # Latest snapshot for dashboard
    ir_latest = {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "total_releases":    len(all_releases),
        "new_this_run":      len(added),
        "high_signal_new":   high_signal,
        "top_recent":        sorted(all_releases, key=lambda x: x.get("scraped_at",""), reverse=True)[:5],
    }
    with open(IR_LATEST_JSON, "w") as f:
        json.dump(ir_latest, f, indent=2)
    log(f"IR latest snapshot saved → {IR_LATEST_JSON}")
    log("IR scraper complete.")
    log("=" * 60)


if __name__ == "__main__":
    run()
