# WhatsApp Link Organizer

Parse, enrich, and categorize a WhatsApp chat export containing saved links (Facebook/Instagram reels, YouTube videos, etc.) into a browsable, organized format.

## Quick Start

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env   # then fill in ANTHROPIC_API_KEY
python -m src.main
```

Place your WhatsApp export at `data/raw/whatsapp_export.txt` before running.

## Output Files

| File | Description |
|------|-------------|
| `data/processed/extracted_links.csv` | All links with timestamp, sender, URL, platform, message context |
| `data/processed/enriched_links.csv` | Above + title, description, source name |
| `data/processed/categorized_links.csv` | Above + category, confidence, reason, method |
| `data/processed/report.html` | Browsable HTML report grouped by category |
| `data/processed/review.xlsx` | Editable Excel workbook — re-categorize and annotate links |

## Pipeline

```
data/raw/whatsapp_export.txt
        │
        ▼  parser.py
data/processed/extracted_links.csv
        │
        ▼  metadata.py  (YouTube via yt-dlp · Meta via API → yt-dlp cookies fallback)
data/processed/enriched_links.csv
        │
        ▼  categorizer.py  (keyword rules → Claude API fallback)
data/processed/categorized_links.csv
        │
        ▼  exporters.py
    report.html  +  review.xlsx
```

## Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `ANTHROPIC_API_KEY` | Yes | — | Claude API key for categorization fallback |
| `ANTHROPIC_MODEL` | No | `claude-haiku-4-5-20251001` | Model used for LLM fallback |
| `CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD` | No | `0.62` | Confidence below which LLM fallback triggers |
| `CATEGORIZATION_ENABLE_LLM_FALLBACK` | No | `1` | Set to `0` for rules-only (no API calls) |
| `META_ACCESS_TOKEN` | No | — | Meta oEmbed API token (requires Meta app review) |
| `META_ENABLE_PLAYWRIGHT` | No | `0` | Set to `1` to enable Playwright browser fallback for Meta |

Copy `.env.example` to `.env` for a pre-filled template.

## Meta Enrichment

Facebook/Instagram metadata uses a layered approach:

1. **Meta oEmbed API** — if `META_ACCESS_TOKEN` is set and the app has been approved for `oembed_read`
2. **yt-dlp + browser cookies** — fallback using `data/raw/facebook_cookies.txt` / `data/raw/instagram_cookies.txt` (export with [Get cookies.txt LOCALLY](https://chrome.google.com/webstore/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc))
3. **Empty metadata** — links are still categorized by Claude using URL + message context

## Categorization

Two-tier classification per link:

- **Tier 1 — Keyword rules** (fast, free): weighted matching across title, description, context, message text. 162 keywords across 8 categories.
- **Tier 2 — Claude API** (triggered when rules are uncertain): passes all available text fields to Claude Haiku.

### Categories

1. SMC / Trading Concepts
2. General Trading / Investing
3. Daily Affirmations / Mindset
4. Psychology / Self Development
5. Healthy Cooking / Recipes
6. Nutrition / Health
7. Fitness / Training
8. Business / Entrepreneurship
9. Other / Review ← manual review queue

## Exporting Your WhatsApp Chat

- **Android**: WhatsApp → Chat → ⋮ → More → Export chat → Without media
- **iPhone**: WhatsApp → Chat → name at top → Export Chat → Without Media

## Known Limitations

- Facebook reels have no accessible metadata without a Meta API token that has passed app review. Categorization falls back to Claude using message context only.
- WhatsApp timestamp format varies between Android and iPhone — malformed lines are skipped silently.
- For large exports (1000+ links) with LLM fallback enabled, runtime will be several minutes.
