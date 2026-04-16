# WhatsApp Link Organizer

Extract links from a WhatsApp export, classify them by platform, and enrich them into CSV output.

## Usage

Run the pipeline:

```powershell
python -m src.main
```

It writes:

- `data/processed/extracted_links.csv`
- `data/processed/enriched_links.csv`
- `data/processed/categorized_links.csv`

## Meta Enrichment

Facebook and Instagram links use a layered enrichment flow:

1. normalize Meta share URLs and remove tracking parameters
2. try official Meta oEmbed-style API requests using `META_ACCESS_TOKEN`
3. fall back to Playwright for pages that still need browser rendering
4. record `canonical_url`, `enrichment_status`, and `enrichment_reason`

The pipeline does not use personal Facebook or Instagram usernames/passwords.

### Environment

Copy `.env.example` values into your environment before running the script:

- `META_ACCESS_TOKEN`: Meta app access token used for official API requests
- `META_GRAPH_BASE_URL`: Graph API base URL, defaults to `https://graph.facebook.com/v22.0`
- `META_ENABLE_PLAYWRIGHT`: `1` to allow browser fallback, `0` to disable it
- `META_PLAYWRIGHT_TIMEOUT_MS`: timeout for Playwright navigation
- `OPENAI_API_KEY`: OpenAI API key used for categorization fallback
- `OPENAI_MODEL`: OpenAI model name for categorization fallback, for example `gpt-4o-mini`
- `CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD`: rules confidence threshold before LLM fallback, defaults to `0.62`
- `CATEGORIZATION_ENABLE_LLM_FALLBACK`: `1` to enable OpenAI fallback, `0` to keep uncertain rows as review

## Categorization

`src.categorizer.categorize_links()` reads the enriched CSV, scores preferred categories with deterministic keyword rules, and only calls OpenAI for rows that remain ambiguous.

The categorization output appends:

- `category`
- `category_confidence`
- `categorization_reason`
- `categorization_method`

`categorization_method` is one of:

- `rules`
- `llm`
- `review`

### Optional Playwright Install

Playwright is optional. If you want the browser fallback:

```powershell
pip install playwright
playwright install chromium
```

If Playwright is not installed, Meta rows still complete with structured status fields instead of crashing.
