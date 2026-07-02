# GEO Engine

Production-grade Generative Engine Optimization audit automation for seven WordPress/content properties.

## Sites

| Site name | Sitemap |
| --- | --- |
| `businessabc` | `https://businessabc.net/sitemap.xml` |
| `citiesabc` | `https://citiesabc.com/sitemap.xml` |
| `intelligenthq` | `https://www.intelligenthq.com/sitemap_index.xml` |
| `fashionabc` | `https://www.fashionabc.org/sitemap_index.xml` |
| `freedomx` | `https://freedomx.com/sitemap_index.xml` |
| `hedgethink` | `http://www.hedgethink.com/sitemap_index.xml` |
| `tradersdna` | `http://www.tradersdna.com/sitemap_index.xml` |

## Local Setup

```bash
python -m pip install -r requirements.txt
python -m pip uninstall -y sentence-transformers || true
```

`sentence-transformers` is intentionally removed because it can trigger unstable per-page model loads in the underlying audit library.

## Run A Local Audit

```bash
python scripts/geo_audit.py "https://www.intelligenthq.com/sitemap_index.xml" \
  --site-name intelligenthq \
  --max-urls 30 \
  --concurrency 3 \
  --time-budget-minutes 25 \
  --checkpoint-file progress/intelligenthq.json \
  --out reports/test
```

The root `geo_audit.py` file is a compatibility wrapper around `scripts/geo_audit.py`.

## Checkpoint And Resume

Each production run writes a checkpoint file such as `progress/intelligenthq.json`.

The checkpoint contains:

- the discovered sitemap URL list
- completed URL list
- per-page GEO results, including failures and timeouts
- `crawl_stats` from sitemap/page sampling
- `status`: `in_progress` or `complete`

Every checkpoint JSON write is atomic: the script writes a temporary file and renames it into place. If a job stops at the 5.5 hour soft budget, it finishes the current page, saves `status: in_progress`, and exits cleanly. The full GitHub Actions workflow commits that checkpoint and dispatches the same workflow for the same site so it resumes instead of restarting.

When all URLs are complete, the script writes:

- `results/<site-name>/final.json`
- `results/<site-name>/final.csv`

## GitHub Actions

`GEO Audit Test` is manually triggered. It runs all seven sites in parallel with:

- `--max-urls 30`
- `--time-budget-minutes 25`
- artifacts uploaded from `reports/test/`
- no checkpoint commits and no retriggering

`GEO Audit Full` can be manually triggered for one site or all sites. It also runs weekly. Each site has its own concurrency group, checkpoint, and resume chain. Commits use `[skip ci]` to avoid workflow loops.

## Consolidated Report

After completed site reports exist under `results/<site-name>/final.json`, run:

```bash
python scripts/generate_report.py --results-dir results --out-json summary.json --out-md REPORT.md
```

`summary.json` contains per-site and cross-site aggregates, category averages against the expected maximums, timeout rates, and best/worst pages. `REPORT.md` is a one-page operational summary with headline site scores, consistent findings, and prioritized fixes.

## Tests

```bash
python -m pytest
```

Tests mock GEO audit and sitemap calls, so they do not hit live sites.
