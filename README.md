# GEO Engine

Automation for Generative Engine Optimization audits across seven WordPress/content sites. The repo runs capped test audits and resumable full audits with GitHub Actions.

## Target Sites

| Slug | Sitemap |
| --- | --- |
| `businessabc` | `https://businessabc.net/sitemap.xml` |
| `citiesabc` | `https://citiesabc.com/sitemap.xml` |
| `intelligenthq` | `https://www.intelligenthq.com/sitemap_index.xml` |
| `fashionabc` | `https://www.fashionabc.org/sitemap_index.xml` |
| `freedomx` | `https://freedomx.com/sitemap_index.xml` |
| `hedgethink` | `http://www.hedgethink.com/sitemap_index.xml` |
| `tradersdna` | `http://www.tradersdna.com/sitemap_index.xml` |

## Local Usage

Install dependencies:

```bash
python -m pip install -r requirements.txt -c constraints.txt
python -m pip uninstall -y sentence-transformers || true
```

Run a 30 URL sanity check:

```bash
python geo_audit.py test --site intelligenthq --max-urls 30
```

Run or resume a full audit for one site:

```bash
python geo_audit.py resume --site intelligenthq --time-budget-minutes 330
```

The legacy single-sitemap mode is still available:

```bash
python geo_audit.py https://www.intelligenthq.com/sitemap_index.xml --max-urls 20
```

## GitHub Actions

`GEO Audit Test` is manually triggered and runs all seven sites in parallel with `--max-urls 30`. Reports are uploaded as artifacts and written to:

- `reports/test/<site-slug>.json`
- `reports/test/<site-slug>.csv`

`GEO Audit Full` supports manual and scheduled runs. It keeps one checkpoint per site:

- `progress/<site-slug>.json`

Each full job runs for a 5 hour 30 minute soft budget. It finishes the current URL, writes the checkpoint, commits progress, and dispatches the same workflow for the same site if URLs remain. A completed site writes:

- `reports/<site-slug>/final.json`
- `reports/<site-slug>/final.csv`
- `reports/<site-slug>/state.json`

The workflow uses `GITHUB_TOKEN` with `contents: write` and `actions: write`; no personal tokens or hardcoded secrets are required in CI.

## Output Fields

Every page result is retained, including failures and timeouts. Important fields:

- `score`, `band`, `score_breakdown`: GEO score from `geo-optimizer-skill`.
- `http_status`, `error`: status and failure reason. Failures are not discarded.
- `duration_seconds`, `attempts`: timing and retry visibility.
- `structured_data`: extracted JSON-LD, microdata, RDFa, and OpenGraph data from `extruct`.
- `sitemap_metrics`: site-level status code, response time, and content length summaries.
- `failure_patterns`: grouped counts of success and error strings.

`constraints.txt` intentionally blocks `sentence-transformers` because that package destabilizes the embedding check under concurrent CI audits.
