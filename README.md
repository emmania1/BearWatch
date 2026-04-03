# BEARWATCH — Setup Guide

## What this is
A demand intelligence system for Build-A-Bear Workshop (BBW) built for Pillsbury Lake Capital.
Scrapers collect data weekly → write to JSON files → the dashboard HTML reads them on load.

---

## Folder Structure
```
bearwatch/
├── bearwatch.html          ← the dashboard (open in browser or GitHub Pages)
├── run_all.py              ← run this weekly to update everything
├── data/
│   ├── dashboard_data.json ← dashboard reads this
│   ├── reddit_history.csv  ← full Reddit history over time
│   ├── reddit_latest.json  ← most recent Reddit snapshot
│   ├── trends_history.csv  ← Google Trends baseline history
│   ├── trends_latest.json  ← current trends snapshot + spike alerts
│   ├── trends_alerts.json  ← active IP release spike alerts
│   ├── press_releases.json ← all scraped press releases
│   └── ir_latest.json      ← latest IR snapshot
├── scrapers/
│   ├── reddit_scraper.py
│   ├── trends_scraper.py
│   └── ir_scraper.py
└── logs/
    ├── master.log
    ├── reddit_scraper.log
    ├── trends_scraper.log
    └── ir_scraper.log
```

---

## Step 1 — Install Python dependencies

```bash
pip install praw pytrends requests beautifulsoup4 pandas
```

---

## Step 2 — Set up Reddit API credentials

1. Go to https://www.reddit.com/prefs/apps
2. Click "create another app" → choose **script**
3. Name it "bearwatch", redirect URI = `http://localhost:8080`
4. Copy your **client_id** (under the app name) and **client_secret**
5. Open `scrapers/reddit_scraper.py` and fill in:
   ```python
   REDDIT_CLIENT_ID     = "your_client_id_here"
   REDDIT_CLIENT_SECRET = "your_client_secret_here"
   REDDIT_USER_AGENT    = "bearwatch-scraper/1.0 (by u/your_reddit_username)"
   ```

No payment or approval needed — Reddit's free API allows read-only access.

---

## Step 3 — Run the scrapers

```bash
cd bearwatch
python run_all.py
```

This runs all three scrapers in sequence and writes `data/dashboard_data.json`.
First run takes ~2 minutes. Subsequent runs are faster (incremental).

---

## Step 4 — Open the dashboard

Open `bearwatch.html` in your browser. It will automatically read `data/dashboard_data.json`
from the same folder and populate the signal cards with real data.

To refresh the data: run `python run_all.py` again, then refresh the browser.

---

## Step 5 — Schedule weekly runs (Mac)

Open Terminal:
```bash
crontab -e
```

Add this line (runs every Sunday at 8am):
```
0 8 * * 0 cd /full/path/to/bearwatch && python run_all.py >> logs/master.log 2>&1
```

---

## IP Release Calendar — Manual Updates

When BBW announces a new collab or a film tie-in is confirmed, add it to
`scrapers/trends_scraper.py` in the `IP_RELEASES` list:

```python
{"name": "New Collab Name", "date": "2026-08-01", "keywords": ["Build-A-Bear NewCollab"], "heat": "high"},
```

The trends scraper will automatically start monitoring 21 days before that date.

---

## Web Traffic — Manual Process (SimilarWeb Free)

Until a paid API is available:
1. Go to https://www.similarweb.com/website/buildabear.com/
2. Note monthly visits, engagement rate, pages per visit
3. Paste into `data/web_traffic_manual.csv`:
   ```
   date,monthly_visits,pages_per_visit,avg_duration_sec,bounce_rate
   2026-03-30,450000,4.2,185,0.42
   ```
4. The dashboard will read this CSV and display the trend

---

## GitHub Pages (optional)

1. Push the entire `bearwatch/` folder to a GitHub repo
2. Go to repo Settings → Pages → set source to `main` branch root
3. Access your dashboard at `https://yourusername.github.io/bearwatch/bearwatch.html`

Note: for GitHub Pages to serve live data, you need to commit the updated
`data/dashboard_data.json` after each weekly scraper run (or use GitHub Actions to automate).

---

## Troubleshooting

**pytrends 429 error**: Google rate limited you. Increase `SLEEP_BETWEEN_REQUESTS` in `trends_scraper.py` to 10-15 seconds and try again.

**Reddit empty results**: Check your credentials. Make sure the app type is "script" not "web app".

**IR scraper returns nothing**: BBW may have updated their website structure. Check the URL in `ir_scraper.py` matches what's live at ir.buildabear.com.
