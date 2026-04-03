"""
BEARWATCH — Reddit Scraper
Pulls weekly post/comment/member data from r/buildabear via PRAW.
Writes to data/reddit_history.csv and data/reddit_latest.json.

Setup:
  pip install praw pandas
  Create a Reddit app at https://www.reddit.com/prefs/apps (script type)
  Fill in your credentials in the CREDENTIALS section below.

Run:
  python scrapers/reddit_scraper.py

Schedule (Mac/Linux cron, weekly Sunday midnight):
  0 0 * * 0 cd /path/to/bearwatch && python scrapers/reddit_scraper.py
"""

import praw
import pandas as pd
import json
import os
import time
from datetime import datetime, timezone, timedelta

# ── CREDENTIALS ──────────────────────────────────────────────
# Go to https://www.reddit.com/prefs/apps → create app → script
REDDIT_CLIENT_ID     = "YOUR_CLIENT_ID"
REDDIT_CLIENT_SECRET = "YOUR_CLIENT_SECRET"
REDDIT_USER_AGENT    = "bearwatch-scraper/1.0 (by u/YOUR_REDDIT_USERNAME)"
# ─────────────────────────────────────────────────────────────

SUBREDDITS = [
    "buildabear",
    "squishmallows",   # competitor signal
    "stuffedanimals",  # category signal
]

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
HISTORY_CSV  = os.path.join(DATA_DIR, "reddit_history.csv")
LATEST_JSON  = os.path.join(DATA_DIR, "reddit_latest.json")
LOG_FILE     = os.path.join(DATA_DIR, "..", "logs", "reddit_scraper.log")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def get_reddit():
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        read_only=True,
    )


def scrape_subreddit_week(reddit, subreddit_name):
    """Pull the last 7 days of posts from a subreddit."""
    sub = reddit.subreddit(subreddit_name)
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    posts = []
    try:
        for post in sub.new(limit=500):
            created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
            if created < cutoff:
                break
            posts.append({
                "id":           post.id,
                "title":        post.title,
                "score":        post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_utc":  created.isoformat(),
                "url":          post.url,
                "flair":        post.link_flair_text,
            })
        log(f"  {subreddit_name}: {len(posts)} posts in last 7 days")
    except Exception as e:
        log(f"  ERROR scraping r/{subreddit_name}: {e}")

    return posts


def get_subscriber_count(reddit, subreddit_name):
    try:
        count = reddit.subreddit(subreddit_name).subscribers
        log(f"  {subreddit_name}: {count:,} subscribers")
        return count
    except Exception as e:
        log(f"  ERROR getting subscribers for r/{subreddit_name}: {e}")
        return None


def aggregate_week(posts):
    """Summarise a list of posts into weekly metrics."""
    if not posts:
        return {
            "post_count":      0,
            "total_score":     0,
            "avg_score":       0,
            "total_comments":  0,
            "avg_comments":    0,
            "avg_upvote_ratio": 0,
        }
    scores   = [p["score"]        for p in posts]
    comments = [p["num_comments"] for p in posts]
    ratios   = [p["upvote_ratio"] for p in posts]
    return {
        "post_count":       len(posts),
        "total_score":      sum(scores),
        "avg_score":        round(sum(scores) / len(scores), 1),
        "total_comments":   sum(comments),
        "avg_comments":     round(sum(comments) / len(comments), 1),
        "avg_upvote_ratio": round(sum(ratios)   / len(ratios),   3),
    }


def load_history():
    if os.path.exists(HISTORY_CSV):
        return pd.read_csv(HISTORY_CSV)
    return pd.DataFrame()


def save_history(df):
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(HISTORY_CSV, index=False)
    log(f"History saved → {HISTORY_CSV} ({len(df)} rows)")


def run():
    log("=" * 60)
    log("BEARWATCH Reddit scraper starting")
    reddit = get_reddit()

    week_ending = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_rows    = []
    latest      = {}

    for sub_name in SUBREDDITS:
        log(f"Scraping r/{sub_name} ...")
        subscribers = get_subscriber_count(reddit, sub_name)
        posts       = scrape_subreddit_week(reddit, sub_name)
        metrics     = aggregate_week(posts)
        time.sleep(1)  # polite rate limiting

        row = {
            "week_ending": week_ending,
            "subreddit":   sub_name,
            "subscribers": subscribers,
            **metrics,
        }
        new_rows.append(row)
        latest[sub_name] = row

    # Append to history CSV
    history = load_history()
    new_df  = pd.DataFrame(new_rows)

    # Avoid duplicate weeks
    if not history.empty:
        history = history[
            ~((history["week_ending"] == week_ending) &
              (history["subreddit"].isin(SUBREDDITS)))
        ]

    history = pd.concat([history, new_df], ignore_index=True)
    history = history.sort_values(["subreddit", "week_ending"])
    save_history(history)

    # Write latest snapshot JSON (dashboard reads this)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LATEST_JSON, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "week_ending":  week_ending,
            "subreddits":   latest,
        }, f, indent=2)
    log(f"Latest snapshot saved → {LATEST_JSON}")
    log("Reddit scraper complete.")
    log("=" * 60)


if __name__ == "__main__":
    run()
