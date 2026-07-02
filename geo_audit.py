#!/usr/bin/env python3
"""
GEO audit automation for WordPress/content sites.

The module exposes reusable audit functions and a CLI used by GitHub Actions:

    python geo_audit.py test --site intelligenthq --max-urls 30
    python geo_audit.py resume --site intelligenthq --time-budget-minutes 330
    python geo_audit.py https://www.intelligenthq.com/sitemap_index.xml --max-urls 20
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import json
import math
import statistics
import subprocess
import sys
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from geo_optimizer.core.batch_audit import _aggregate_batch_result, _audit_urls, _select_urls, _summarize_audit_result
from geo_optimizer.core.llms_generator import fetch_sitemap
from geo_optimizer.models.config import AUDIT_TIMEOUT_SECONDS
from geo_optimizer.models.results import AuditResult, BatchAuditPageResult
from tenacity import Retrying, stop_after_attempt, wait_exponential


DEFAULT_CONCURRENCY = 3
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_TIME_BUDGET_MINUTES = 330
CHECKPOINT_COMMIT_PAGES = 10
CHECKPOINT_COMMIT_SECONDS = 600
REQUEST_TIMEOUT_SECONDS = 15

SITES: dict[str, str] = {
    "businessabc": "https://businessabc.net/sitemap.xml",
    "citiesabc": "https://citiesabc.com/sitemap.xml",
    "intelligenthq": "https://www.intelligenthq.com/sitemap_index.xml",
    "fashionabc": "https://www.fashionabc.org/sitemap_index.xml",
    "freedomx": "https://freedomx.com/sitemap_index.xml",
    "hedgethink": "http://www.hedgethink.com/sitemap_index.xml",
    "tradersdna": "http://www.tradersdna.com/sitemap_index.xml",
}


class RetryableAuditError(Exception):
    def __init__(self, page: BatchAuditPageResult):
        super().__init__(page.error or "Audit failed")
        self.page = page


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_site(site_or_url: str) -> tuple[str, str]:
    if site_or_url in SITES:
        return site_or_url, SITES[site_or_url]
    parsed = urlparse(site_or_url)
    if parsed.scheme and parsed.netloc:
        host = parsed.netloc.lower().removeprefix("www.")
        slug = host.split(".")[0].replace("-", "_")
        return slug, site_or_url
    raise ValueError(f"Unknown site '{site_or_url}'. Use one of: {', '.join(SITES)}")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        f.write("\n")
    tmp_path.replace(path)


def append_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "url",
        "score",
        "band",
        "http_status",
        "error",
        "duration_seconds",
        "attempts",
        "recommendations_count",
        "json_ld_count",
        "microdata_count",
        "rdfa_count",
        "opengraph_count",
        "content_length",
    ]
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def discover_urls(sitemap_url: str, max_urls: int | None = None) -> list[str]:
    entries = fetch_sitemap(sitemap_url)
    if not entries:
        raise ValueError(f"No URLs found in sitemap: {sitemap_url}")
    cap = max_urls if max_urls and max_urls > 0 else len(entries)
    return _select_urls(entries, max_urls=cap)


def sitemap_metrics(sitemap_url: str, urls: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "sitemap_url": sitemap_url,
        "url_count": len(urls),
        "status_code_counts": {},
        "response_time_seconds": {},
        "content_length_bytes": {},
        "content_length_outliers": [],
        "errors": [],
    }
    try:
        import advertools as adv  # type: ignore

        df = adv.sitemap_to_df(sitemap_url)
        if "loc" in df.columns:
            metrics["url_count_from_advertools"] = int(len(df))
        for col in ("lastmod", "sitemap", "etag"):
            if col in df.columns:
                metrics[f"{col}_available"] = True
    except Exception as exc:
        metrics["errors"].append(f"advertools sitemap parse failed: {type(exc).__name__}: {exc}")

    sampled = urls[: min(len(urls), 50)]
    timings: list[float] = []
    lengths: list[int] = []
    status_counts: Counter[int] = Counter()
    for url in sampled:
        started = time.monotonic()
        try:
            response = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in {403, 405}:
                response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS)
            elapsed = round(time.monotonic() - started, 3)
            status_counts[response.status_code] += 1
            timings.append(elapsed)
            content_length = response.headers.get("content-length")
            if content_length and content_length.isdigit():
                lengths.append(int(content_length))
        except Exception as exc:
            metrics["errors"].append(f"{url}: {type(exc).__name__}: {exc}")

    metrics["status_code_counts"] = {str(k): v for k, v in sorted(status_counts.items())}
    metrics["response_time_seconds"] = summarize_numbers(timings)
    metrics["content_length_bytes"] = summarize_numbers(lengths)
    if lengths:
        threshold = statistics.median(lengths) + (3 * statistics.pstdev(lengths) if len(lengths) > 1 else 0)
        metrics["content_length_outliers"] = [length for length in lengths if length > threshold]
    return metrics


def summarize_numbers(values: list[float | int]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(float(value) for value in values)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 3),
        "median": round(statistics.median(ordered), 3),
        "p95": round(percentile(ordered, 95), 3),
        "max": round(ordered[-1], 3),
        "average": round(sum(ordered) / len(ordered), 3),
    }


def percentile(sorted_values: list[float], pct: int) -> float:
    if not sorted_values:
        return 0.0
    index = (len(sorted_values) - 1) * pct / 100
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def structured_data_for_url(url: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "json-ld": [],
        "microdata": [],
        "rdfa": [],
        "opengraph": [],
        "counts": {"json-ld": 0, "microdata": 0, "rdfa": 0, "opengraph": 0},
        "error": None,
    }
    try:
        import extruct  # type: ignore
        from w3lib.html import get_base_url  # type: ignore

        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "GEO-Audit-Bot/1.0 (+https://github.com/SujalSokandeDev/GEO-Engine)"},
        )
        response.raise_for_status()
        base_url = get_base_url(response.text, response.url)
        extracted = extruct.extract(
            response.text,
            base_url=base_url,
            syntaxes=["json-ld", "microdata", "rdfa", "opengraph"],
            uniform=True,
        )
        for key in payload["counts"]:
            payload[key] = extracted.get(key, [])
            payload["counts"][key] = len(payload[key])
        payload["content_length"] = len(response.content)
    except Exception as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"
    return payload


def audit_page(url: str, *, use_cache: bool, attempts: int = DEFAULT_RETRY_ATTEMPTS) -> dict[str, Any]:
    started = time.monotonic()
    page: BatchAuditPageResult | None = None
    attempt_count = 0
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=2, min=2, max=20),
            reraise=True,
        ):
            with attempt:
                attempt_count = attempt.retry_state.attempt_number
                page = asyncio.run(_audit_urls([url], use_cache=use_cache, project_config=None, concurrency=1))[0]
                if page.error:
                    raise RetryableAuditError(page)
    except RetryableAuditError as exc:
        page = exc.page
    except Exception as exc:
        result = AuditResult(url=url, error=f"{type(exc).__name__}: {exc}", band="critical")
        page = _summarize_audit_result(result)

    duration = round(time.monotonic() - started, 3)
    structured = structured_data_for_url(url)
    page_dict = dataclasses.asdict(page)
    page_dict.update(
        {
            "duration_seconds": duration,
            "attempts": attempt_count,
            "structured_data": structured,
            "content_length": structured.get("content_length", 0),
            "audited_at": utc_now(),
        }
    )
    if page_dict.get("error"):
        print(f"FAIL {url} [{duration}s] {page_dict['error']}")
    else:
        print(f"OK   {url} [{duration}s] score={page_dict['score']} band={page_dict['band']}")
    return page_dict


def page_dict_to_result(page: dict[str, Any]) -> BatchAuditPageResult:
    return BatchAuditPageResult(
        url=page["url"],
        score=int(page.get("score") or 0),
        band=page.get("band") or "critical",
        http_status=int(page.get("http_status") or 0),
        error=page.get("error"),
        score_breakdown=page.get("score_breakdown") or {},
        recommendations_count=int(page.get("recommendations_count") or 0),
    )


def build_report(sitemap_url: str, urls: list[str], pages: list[dict[str, Any]], started_at: str) -> dict[str, Any]:
    batch = _aggregate_batch_result(
        sitemap_url=sitemap_url,
        discovered_urls=len(urls),
        page_results=[page_dict_to_result(page) for page in pages],
    )
    report = dataclasses.asdict(batch)
    report["timestamp"] = started_at
    report["completed_at"] = utc_now()
    report["all_pages"] = pages
    report["pages"] = pages
    report["failure_patterns"] = dict(Counter((page.get("error") or "success") for page in pages))
    durations = [float(page.get("duration_seconds") or 0) for page in pages]
    report["timing"] = {
        "total_seconds": round(sum(durations), 3),
        "average_seconds_per_page": round(sum(durations) / len(durations), 3) if durations else 0,
        "duration_distribution_seconds": summarize_numbers(durations),
    }
    return report


def write_report_json_csv(report: dict[str, Any], json_path: Path, csv_path: Path) -> None:
    write_json(json_path, report)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        rows = report.get("pages", [])
        fieldnames = [
            "url",
            "score",
            "band",
            "http_status",
            "error",
            "duration_seconds",
            "attempts",
            "recommendations_count",
            "json_ld_count",
            "microdata_count",
            "rdfa_count",
            "opengraph_count",
            "content_length",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for page in rows:
            counts = (page.get("structured_data") or {}).get("counts") or {}
            writer.writerow(
                {
                    "url": page.get("url"),
                    "score": page.get("score"),
                    "band": page.get("band"),
                    "http_status": page.get("http_status"),
                    "error": page.get("error") or "",
                    "duration_seconds": page.get("duration_seconds"),
                    "attempts": page.get("attempts"),
                    "recommendations_count": page.get("recommendations_count"),
                    "json_ld_count": counts.get("json-ld", 0),
                    "microdata_count": counts.get("microdata", 0),
                    "rdfa_count": counts.get("rdfa", 0),
                    "opengraph_count": counts.get("opengraph", 0),
                    "content_length": page.get("content_length", 0),
                }
            )


def audit_pages(urls: list[str], *, use_cache: bool, concurrency: int) -> list[dict[str, Any]]:
    if concurrency <= 1:
        return [audit_page(url, use_cache=use_cache) for url in urls]
    pages_by_url: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(audit_page, url, use_cache=use_cache): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                pages_by_url[url] = future.result()
            except Exception as exc:
                result = AuditResult(url=url, error=f"{type(exc).__name__}: {exc}", band="critical")
                pages_by_url[url] = dataclasses.asdict(_summarize_audit_result(result))
    return [pages_by_url[url] for url in urls]


def run_test_audit(site: str, *, max_urls: int, concurrency: int, use_cache: bool, out_dir: Path) -> dict[str, Any]:
    slug, sitemap_url = resolve_site(site)
    started_at = utc_now()
    urls = discover_urls(sitemap_url, max_urls=max_urls)
    print(f"Running test audit for {slug}: {len(urls)} URLs with concurrency={concurrency}")
    pages = audit_pages(urls, use_cache=use_cache, concurrency=concurrency)
    report = build_report(sitemap_url, urls, pages, started_at)
    report["site_slug"] = slug
    report["mode"] = "test"
    report["sitemap_metrics"] = sitemap_metrics(sitemap_url, urls)
    write_report_json_csv(report, out_dir / f"{slug}.json", out_dir / f"{slug}.csv")
    print_console_summary(slug, report)
    return report


def checkpoint_path(progress_dir: Path, slug: str) -> Path:
    return progress_dir / f"{slug}.json"


def load_or_create_checkpoint(slug: str, sitemap_url: str, progress_dir: Path) -> dict[str, Any]:
    path = checkpoint_path(progress_dir, slug)
    checkpoint = load_json(path, None)
    if checkpoint:
        return checkpoint
    urls = discover_urls(sitemap_url)
    checkpoint = {
        "site_slug": slug,
        "sitemap_url": sitemap_url,
        "created_at": utc_now(),
        "last_update": utc_now(),
        "complete": False,
        "urls": urls,
        "results": [],
        "sitemap_metrics": sitemap_metrics(sitemap_url, urls),
    }
    write_json(path, checkpoint)
    return checkpoint


def save_checkpoint(progress_dir: Path, checkpoint: dict[str, Any]) -> None:
    checkpoint["last_update"] = utc_now()
    write_json(checkpoint_path(progress_dir, checkpoint["site_slug"]), checkpoint)


def commit_checkpoint_files(slug: str, progress_dir: Path, reports_dir: Path) -> None:
    paths = [str(checkpoint_path(progress_dir, slug)), str(reports_dir / slug)]
    try:
        subprocess.run(["git", "add", *paths], check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            return
        subprocess.run(["git", "commit", "-m", f"Update GEO audit checkpoint for {slug}"], check=True)
        for attempt in range(1, 4):
            pull = subprocess.run(["git", "pull", "--rebase"])
            push = subprocess.run(["git", "push"]) if pull.returncode == 0 else pull
            if push.returncode == 0:
                return
            print(f"Checkpoint push attempt {attempt} failed for {slug}; retrying.")
            time.sleep(5 * attempt)
        print(f"Checkpoint commit created for {slug}, but push did not complete.")
    except Exception as exc:
        print(f"Checkpoint git commit skipped for {slug}: {type(exc).__name__}: {exc}")


def run_resumable_audit(
    site: str,
    *,
    progress_dir: Path,
    reports_dir: Path,
    time_budget_minutes: int,
    use_cache: bool,
    force_restart: bool,
    commit_checkpoints: bool,
) -> dict[str, Any]:
    slug, sitemap_url = resolve_site(site)
    path = checkpoint_path(progress_dir, slug)
    if force_restart and path.exists():
        path.unlink()
    checkpoint = load_or_create_checkpoint(slug, sitemap_url, progress_dir)
    if checkpoint.get("complete"):
        print(f"{slug} checkpoint is already complete; skipping.")
        return {"complete": True, "audited": len(checkpoint.get("results", [])), "remaining": 0}

    urls = checkpoint["urls"]
    audited = {row["url"] for row in checkpoint.get("results", [])}
    started = time.monotonic()
    budget_seconds = time_budget_minutes * 60
    last_flush = time.monotonic()
    pages_since_flush = 0

    print(f"Resuming {slug}: {len(audited)}/{len(urls)} URLs audited")
    for url in urls:
        if url in audited:
            continue
        if time.monotonic() - started >= budget_seconds:
            print(f"Soft time budget reached before starting next URL for {slug}.")
            break
        page = audit_page(url, use_cache=use_cache)
        checkpoint["results"].append(page)
        audited.add(url)
        pages_since_flush += 1
        now = time.monotonic()
        if pages_since_flush >= CHECKPOINT_COMMIT_PAGES or now - last_flush >= CHECKPOINT_COMMIT_SECONDS:
            save_checkpoint(progress_dir, checkpoint)
            append_csv(reports_dir / slug / "incremental.csv", [page])
            print(f"Checkpoint saved for {slug}: {len(audited)}/{len(urls)}")
            if commit_checkpoints:
                commit_checkpoint_files(slug, progress_dir, reports_dir)
            pages_since_flush = 0
            last_flush = now

    save_checkpoint(progress_dir, checkpoint)
    remaining = len(urls) - len(audited)
    if remaining == 0:
        checkpoint["complete"] = True
        checkpoint["completed_at"] = utc_now()
        report = build_report(sitemap_url, urls, checkpoint["results"], checkpoint["created_at"])
        report["site_slug"] = slug
        report["mode"] = "full"
        report["sitemap_metrics"] = checkpoint.get("sitemap_metrics", {})
        write_report_json_csv(report, reports_dir / slug / "final.json", reports_dir / slug / "final.csv")
        save_checkpoint(progress_dir, checkpoint)
        print_console_summary(slug, report)
        if commit_checkpoints:
            commit_checkpoint_files(slug, progress_dir, reports_dir)

    state = {"complete": remaining == 0, "audited": len(audited), "remaining": remaining}
    write_json(reports_dir / slug / "state.json", state)
    print(f"STATE complete={str(state['complete']).lower()} audited={state['audited']} remaining={state['remaining']}")
    return state


def run_legacy_audit(sitemap_url: str, max_urls: int, concurrency: int, use_cache: bool, out_dir: Path) -> dict[str, Any]:
    slug, _ = resolve_site(sitemap_url)
    urls = discover_urls(sitemap_url, max_urls=max_urls)
    pages = audit_pages(urls, use_cache=use_cache, concurrency=concurrency)
    report = build_report(sitemap_url, urls, pages, utc_now())
    report["site_slug"] = slug
    report["mode"] = "single"
    write_report_json_csv(report, out_dir / f"geo_audit_{slug}.json", out_dir / f"geo_audit_{slug}.csv")
    print_console_summary(slug, report)
    return report


def print_console_summary(slug: str, report: dict[str, Any]) -> None:
    timing = report.get("timing", {})
    print("=" * 72)
    print(f"GEO AUDIT SUMMARY: {slug}")
    print("=" * 72)
    print(f"Audited pages     : {report.get('audited_urls')}")
    print(f"Successful        : {report.get('successful_urls')}")
    print(f"Failed/timeouts   : {report.get('failed_urls')}")
    print(f"Average score     : {report.get('average_score')} ({report.get('average_band')})")
    print(f"Total audit time  : {timing.get('total_seconds', 0)}s")
    print(f"Avg per page      : {timing.get('average_seconds_per_page', 0)}s")
    print(f"Failure patterns  : {report.get('failure_patterns', {})}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run GEO audits with optional checkpoint/resume support.")
    subparsers = parser.add_subparsers(dest="command")

    test_parser = subparsers.add_parser("test", help="Run a capped sanity audit for one site.")
    test_parser.add_argument("--site", required=True, choices=sorted(SITES))
    test_parser.add_argument("--max-urls", type=int, default=30)
    test_parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    test_parser.add_argument("--out", default="reports/test")
    test_parser.add_argument("--no-cache", action="store_true")

    resume_parser = subparsers.add_parser("resume", help="Run or resume a checkpointed full audit.")
    resume_parser.add_argument("--site", required=True, choices=sorted(SITES))
    resume_parser.add_argument("--progress-dir", default="progress")
    resume_parser.add_argument("--reports-dir", default="reports")
    resume_parser.add_argument("--time-budget-minutes", type=int, default=DEFAULT_TIME_BUDGET_MINUTES)
    resume_parser.add_argument("--force-restart", action="store_true")
    resume_parser.add_argument("--commit-checkpoints", action="store_true")
    resume_parser.add_argument("--no-cache", action="store_true")

    parser.add_argument("sitemap_url", nargs="?", help="Legacy one-shot mode: sitemap URL to audit.")
    parser.add_argument("--max-urls", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--out", default="geo_reports")
    parser.add_argument("--no-cache", action="store_true")

    args = parser.parse_args()
    try:
        if args.command == "test":
            run_test_audit(
                args.site,
                max_urls=args.max_urls,
                concurrency=args.concurrency,
                use_cache=not args.no_cache,
                out_dir=Path(args.out),
            )
        elif args.command == "resume":
            run_resumable_audit(
                args.site,
                progress_dir=Path(args.progress_dir),
                reports_dir=Path(args.reports_dir),
                time_budget_minutes=args.time_budget_minutes,
                use_cache=not args.no_cache,
                force_restart=args.force_restart,
                commit_checkpoints=args.commit_checkpoints,
            )
        elif args.sitemap_url:
            run_legacy_audit(
                args.sitemap_url,
                max_urls=args.max_urls,
                concurrency=args.concurrency,
                use_cache=not args.no_cache,
                out_dir=Path(args.out),
            )
        else:
            parser.print_help()
            sys.exit(2)
    except Exception as exc:
        print(f"Audit failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
