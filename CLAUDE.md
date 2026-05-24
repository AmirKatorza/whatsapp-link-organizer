# WhatsApp Link Organizer

Parse, enrich, and categorize a WhatsApp chat export containing saved links (Facebook/Instagram reels, YouTube videos, etc.) into a browsable, organized format.

## Data Flow

```
data/raw/whatsapp_export.txt
        │
        ▼ parser.py — extract URLs + timestamps
data/processed/extracted_links.csv
        │
        ▼ metadata.py — enrich with title/description (YouTube via yt-dlp; Meta skipped without token)
data/processed/enriched_links.csv
        │
        ▼ categorizer.py — keyword rules → Claude API fallback for uncertain rows
data/processed/categorized_links.csv
        │
        ▼ exporters.py — HTML report
data/processed/report.html
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
copy .env.example .env          # then fill in values
```

## Running

```bash
python -m src.main
```

Place your WhatsApp export at `data/raw/whatsapp_export.txt` before running.
On Android: WhatsApp → Chat → ⋮ → More → Export chat → Without media.
On iPhone:  WhatsApp → Chat → name at top → Export Chat → Without Media.

## Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `ANTHROPIC_API_KEY` | Yes (for LLM fallback) | — | Claude API key for categorization of uncertain links |
| `ANTHROPIC_MODEL` | No | `claude-haiku-4-5-20251001` | Claude model for LLM fallback |
| `CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD` | No | `0.62` | Confidence below which LLM fallback is triggered |
| `CATEGORIZATION_ENABLE_LLM_FALLBACK` | No | `1` | Set to `0` to disable LLM and keep rule-only results |
| `META_ACCESS_TOKEN` | No | — | Meta oEmbed API token (not needed for backlog clearance) |
| `META_ENABLE_PLAYWRIGHT` | No | `0` | Set to `1` to enable Playwright browser fallback for Meta links |

## Output Files

| File | Description |
|------|-------------|
| `data/processed/extracted_links.csv` | All links with timestamp, sender, URL, platform, message context |
| `data/processed/enriched_links.csv` | Above + title, description, source_name (YouTube only without Meta token) |
| `data/processed/categorized_links.csv` | Above + category, confidence, reason, method |
| `data/processed/report.html` | Browsable HTML report grouped by category — open in any browser |
| `data/processed/review.xlsx` | Editable Excel workbook — review, re-categorize, and annotate all links |

## Categories

1. SMC / Trading Concepts
2. General Trading / Investing
3. Daily Affirmations / Mindset
4. Psychology / Self Development
5. Healthy Cooking / Recipes
6. Nutrition / Health
7. Fitness / Training
8. Business / Entrepreneurship
9. Other / Review ← manual review queue

## Categorization Logic

**Tier 1 — Keyword rules** (fast, free): Weighted keyword matching across title, description, context, message text, and source name. 162 keywords across 8 categories.

**Tier 2 — Claude API fallback** (triggered when rules are uncertain): Passes all available text fields plus the rule's best guess to Claude. Requires `ANTHROPIC_API_KEY`.

Rows that cannot be classified confidently end up in **Other / Review** for manual tagging.

## Known Limitations

- Facebook/Instagram reels provide no metadata without a Meta API token — categorization relies on the message context the user typed when sending the link.
- WhatsApp export timestamp format varies between Android and iPhone. The parser handles both common formats, but malformed lines are skipped silently.
- The pipeline processes all links sequentially. For very large exports (1000+ links) with LLM fallback enabled, runtime will be several minutes due to API calls.
