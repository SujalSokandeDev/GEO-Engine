#!/usr/bin/env python3
"""Production GEO audit runner with resumable checkpoints."""

from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import gzip
import json
import math
import statistics
import sys
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests
from geo_optimizer.core.batch_audit import _aggregate_batch_result, _audit_urls, _select_urls, _summarize_audit_result
from geo_optimizer.core.llms_generator import fetch_sitemap
from geo_optimizer.models.results import AuditResult, BatchAuditPageResult
from tenacity import Retrying, stop_after_attempt, wait_exponential


SITES: dict[str, str] = {
    "businessabc": "https://businessabc.net/sitemap.xml",
    "citiesabc": "https://citiesabc.com/sitemap.xml",
    "intelligenthq": "https://www.intelligenthq.com/sitemap_index.xml",
    "fashionabc": "https://www.fashionabc.org/sitemap_index.xml",
    "freedomx": "https://freedomx.com/sitemap_index.xml",
    "hedgethink": "http://www.hedgethink.com/sitemap_index.xml",
    "tradersdna": "http://www.tradersdna.com/sitemap_index.xml",
}

DEFAULT_CONCURRENCY = 3
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_TIME_BUDGET_MINUTES = 330.0
DEFAULT_CHECKPOINT_BATCH_SIZE = 20
REQUEST_TIMEOUT_SECONDS = 15
SITEMAP_TIMEOUT_SECONDS = 60
USER_AGENT = "GEO-Audit-Bot/1.0 (+https://github.com/SujalSokandeDev/GEO-Engine)"


class RetryableAuditError(Exception):
    def __init__(self, page: BatchAuditPageResult):
        super().__init__(page.error or "Audit failed")
        self.page = page


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_exception(message: str) -> None:
    print(message, file=sys.stderr)
    traceback.print_exc()


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{int(time.time() * 1000)}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        f.write("\n")
    tmp_path.replace(path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{int(time.time() * 1000)}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    return host.split(".")[0].replace("-", "_") if host else "site"


def resolve_site_name(site_name: str | None, sitemap_url: str) -> str:
    if site_name:
        return site_name
    for slug, known_url in SITES.items():
        if known_url == sitemap_url:
            return slug
    return slug_from_url(sitemap_url)


def select_urls(entries: list[Any], max_urls: int | None) -> list[str]:
    if not entries:
        return []
    cap = max_urls if max_urls and max_urls > 0 else len(entries)
    return _select_urls(entries, max_urls=cap)


def discover_urls(sitemap_url: str, max_urls: int | None = None) -> list[str]:
    try:
        entries = fetch_sitemap(sitemap_url)
        urls = select_urls(entries, max_urls)
        if urls:
            return urls
    except Exception:
        log_exception(f"Primary sitemap fetch failed for {sitemap_url}")

    urls = fallback_fetch_sitemap_urls(sitemap_url, limit=max_urls or 10000)
    if urls:
        return urls[:max_urls] if max_urls else urls
    raise ValueError(f"No URLs found in sitemap: {sitemap_url}")


def fallback_fetch_sitemap_urls(sitemap_url: str, *, limit: int, seen: set[str] | None = None) -> list[str]:
    seen = seen or set()
    if sitemap_url in seen or limit <= 0:
        return []
    seen.add(sitemap_url)
    try:
        response = requests.get(
            sitemap_url,
            timeout=SITEMAP_TIMEOUT_SECONDS,
            headers={"User-Agent": USER_AGENT, "Accept": "application/xml,text/xml,*/*"},
        )
        response.raise_for_status()
        content = response.content
        if content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)
        root = ElementTree.fromstring(content)
    except Exception:
        log_exception(f"Fallback sitemap fetch failed for {sitemap_url}")
        return []

    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"
    locs = [node.text.strip() for node in root.findall(f".//{namespace}loc") if node.text and node.text.strip()]
    if root.tag.endswith("sitemapindex"):
        urls: list[str] = []
        for child_sitemap in locs:
            urls.extend(fallback_fetch_sitemap_urls(child_sitemap, limit=limit - len(urls), seen=seen))
            if len(urls) >= limit:
                break
        return dedupe_urls(urls)[:limit]
    return dedupe_urls(locs)[:limit]


def dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


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


def crawl_stats(sitemap_url: str, urls: list[str], sample_limit: int = 50) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "sitemap_url": sitemap_url,
        "url_count": len(urls),
        "status_code_breakdown": {},
        "response_time_distribution_seconds": {},
        "content_length_distribution_bytes": {},
        "content_length_outliers": [],
        "errors": [],
    }
    try:
        import advertools as adv  # type: ignore

        df = adv.sitemap_to_df(sitemap_url)
        stats["advertools_url_count"] = int(len(df))
        stats["advertools_columns"] = sorted(str(col) for col in df.columns)
    except Exception:
        stats["errors"].append(traceback.format_exc())

    timings: list[float] = []
    lengths: list[int] = []
    status_counts: Counter[int] = Counter()
    for url in urls[:sample_limit]:
        started = time.monotonic()
        try:
            response = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
            if response.status_code in {403, 405}:
                response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
            timings.append(round(time.monotonic() - started, 3))
            status_counts[response.status_code] += 1
            content_length = response.headers.get("content-length")
            if content_length and content_length.isdigit():
                lengths.append(int(content_length))
        except Exception:
            stats["errors"].append(traceback.format_exc())

    stats["status_code_breakdown"] = {str(k): v for k, v in sorted(status_counts.items())}
    stats["response_time_distribution_seconds"] = summarize_numbers(timings)
    stats["content_length_distribution_bytes"] = summarize_numbers(lengths)
    if len(lengths) >= 2:
        median = statistics.median(lengths)
        threshold = median + (3 * statistics.pstdev(lengths))
        stats["content_length_outliers"] = [length for length in lengths if length > threshold]
    return stats


def structured_data_extra(url: str) -> dict[str, Any]:
    result: dict[str, Any] = {
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

        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        base_url = get_base_url(response.text, response.url)
        extracted = extruct.extract(
            response.text,
            base_url=base_url,
            syntaxes=["json-ld", "microdata", "rdfa", "opengraph"],
            uniform=True,
        )
        for key in result["counts"]:
            result[key] = extracted.get(key, [])
            result["counts"][key] = len(result[key])
        result["content_length"] = len(response.content)
    except Exception:
        result["error"] = traceback.format_exc()
    return result


def audit_one_url(url: str, *, use_cache: bool, attempts: int = DEFAULT_RETRY_ATTEMPTS) -> dict[str, Any]:
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
    except Exception:
        log_exception(f"Audit raised an exception for {url}")
        result = AuditResult(url=url, error=traceback.format_exc(), band="critical")
        page = _summarize_audit_result(result)

    if page is None:
        result = AuditResult(url=url, error="Unknown audit failure", band="critical")
        page = _summarize_audit_result(result)

    duration = round(time.monotonic() - started, 3)
    page_dict = dataclasses.asdict(page)
    extra = structured_data_extra(url)
    page_dict.update(
        {
            "duration_seconds": duration,
            "attempts": attempt_count,
            "structured_data_extra": extra,
            "content_length": extra.get("content_length", 0),
            "audited_at": utc_now(),
        }
    )
    status = "FAIL" if page_dict.get("error") else "OK"
    print(f"{status} {url} [{duration}s] score={page_dict.get('score')} error={page_dict.get('error') or ''}")
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


def init_checkpoint(site_name: str, sitemap_url: str, urls: list[str], stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "site_name": site_name,
        "sitemap_url": sitemap_url,
        "created_at": utc_now(),
        "last_update": utc_now(),
        "status": "in_progress",
        "urls": urls,
        "completed_urls": [],
        "results": [],
        "crawl_stats": stats,
    }


def load_or_create_checkpoint(
    checkpoint_file: Path,
    *,
    site_name: str,
    sitemap_url: str,
    max_urls: int | None,
) -> dict[str, Any]:
    checkpoint = load_json(checkpoint_file, None)
    if checkpoint:
        checkpoint.setdefault("status", "complete" if checkpoint.get("complete") else "in_progress")
        checkpoint.setdefault("completed_urls", [row["url"] for row in checkpoint.get("results", [])])
        checkpoint.setdefault("results", [])
        checkpoint.setdefault("urls", [])
        return checkpoint

    urls = discover_urls(sitemap_url, max_urls=max_urls)
    stats = crawl_stats(sitemap_url, urls)
    checkpoint = init_checkpoint(site_name, sitemap_url, urls, stats)
    save_checkpoint(checkpoint_file, checkpoint)
    return checkpoint


def save_checkpoint(checkpoint_file: Path, checkpoint: dict[str, Any]) -> None:
    checkpoint["last_update"] = utc_now()
    atomic_write_json(checkpoint_file, checkpoint)


def audit_urls_concurrent(urls: list[str], *, concurrency: int, use_cache: bool) -> list[dict[str, Any]]:
    if concurrency <= 1:
        return [audit_one_url(url, use_cache=use_cache) for url in urls]
    results_by_url: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(audit_one_url, url, use_cache=use_cache): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                results_by_url[url] = future.result()
            except Exception:
                log_exception(f"Worker failed for {url}")
                result = AuditResult(url=url, error=traceback.format_exc(), band="critical")
                results_by_url[url] = dataclasses.asdict(_summarize_audit_result(result))
    return [results_by_url[url] for url in urls]


def run_audit(
    *,
    sitemap_url: str,
    site_name: str,
    max_urls: int | None,
    concurrency: int,
    out_dir: Path,
    time_budget_minutes: float,
    checkpoint_file: Path,
    checkpoint_batch_size: int,
    use_cache: bool,
) -> dict[str, Any]:
    checkpoint = load_or_create_checkpoint(
        checkpoint_file,
        site_name=site_name,
        sitemap_url=sitemap_url,
        max_urls=max_urls,
    )
    urls = checkpoint["urls"]
    if max_urls and len(urls) > max_urls:
        urls = urls[:max_urls]
        checkpoint["urls"] = urls
    completed = {url for url in checkpoint.get("completed_urls", [])}
    started = time.monotonic()
    budget_seconds = max(0.0, time_budget_minutes * 60)
    pending_batch: list[dict[str, Any]] = []

    pending_urls = [url for url in urls if url not in completed]
    print(f"Starting {site_name}: {len(completed)}/{len(urls)} URLs complete, budget={time_budget_minutes}m")
    cursor = 0
    chunk_size = max(1, concurrency)
    while cursor < len(pending_urls):
        if time.monotonic() - started >= budget_seconds:
            print(f"Time budget reached before starting next page for {site_name}.")
            break
        chunk = pending_urls[cursor : cursor + chunk_size]
        for page in audit_urls_concurrent(chunk, concurrency=concurrency, use_cache=use_cache):
            url = page["url"]
            checkpoint["results"].append(page)
            checkpoint["completed_urls"].append(url)
            completed.add(url)
            pending_batch.append(page)
            if len(pending_batch) >= checkpoint_batch_size:
                checkpoint["status"] = "in_progress"
                save_checkpoint(checkpoint_file, checkpoint)
                pending_batch = []
        cursor += len(chunk)

    checkpoint["status"] = "complete" if len(completed) >= len(urls) else "in_progress"
    save_checkpoint(checkpoint_file, checkpoint)

    report = build_report(checkpoint)
    write_outputs(report, out_dir=out_dir, site_name=site_name)
    print_summary(site_name, report)
    return report


def build_report(checkpoint: dict[str, Any]) -> dict[str, Any]:
    pages = checkpoint.get("results", [])
    batch = _aggregate_batch_result(
        sitemap_url=checkpoint["sitemap_url"],
        discovered_urls=len(checkpoint.get("urls", [])),
        page_results=[page_dict_to_result(page) for page in pages],
    )
    report = dataclasses.asdict(batch)
    report["site_name"] = checkpoint["site_name"]
    report["status"] = checkpoint["status"]
    report["timestamp"] = checkpoint.get("created_at")
    report["completed_at"] = utc_now()
    report["pages"] = pages
    report["crawl_stats"] = checkpoint.get("crawl_stats", {})
    report["failure_patterns"] = dict(Counter((page.get("error") or "success") for page in pages))
    durations = [float(page.get("duration_seconds") or 0) for page in pages]
    report["timing"] = {
        "total_seconds": round(sum(durations), 3),
        "average_seconds_per_page": round(sum(durations) / len(durations), 3) if durations else 0,
        "duration_distribution_seconds": summarize_numbers(durations),
    }
    return report


def write_outputs(report: dict[str, Any], *, out_dir: Path, site_name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{site_name}.json"
    csv_path = out_dir / f"{site_name}.csv"
    atomic_write_json(json_path, report)
    write_csv(csv_path, report.get("pages", []))
    if report.get("status") == "complete":
        site_dir = Path("results") / site_name
        atomic_write_json(site_dir / "final.json", report)
        write_csv(site_dir / "final.csv", report.get("pages", []))
    atomic_write_json(out_dir / f"{site_name}.state.json", {"status": report.get("status"), "site_name": site_name})


def write_csv(path: Path, pages: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{int(time.time() * 1000)}.tmp")
    fields = [
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
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for page in pages:
            counts = (page.get("structured_data_extra") or {}).get("counts") or {}
            writer.writerow(
                {
                    "url": page.get("url", ""),
                    "score": page.get("score", 0),
                    "band": page.get("band", ""),
                    "http_status": page.get("http_status", 0),
                    "error": page.get("error") or "",
                    "duration_seconds": page.get("duration_seconds", 0),
                    "attempts": page.get("attempts", 0),
                    "recommendations_count": page.get("recommendations_count", 0),
                    "json_ld_count": counts.get("json-ld", 0),
                    "microdata_count": counts.get("microdata", 0),
                    "rdfa_count": counts.get("rdfa", 0),
                    "opengraph_count": counts.get("opengraph", 0),
                    "content_length": page.get("content_length", 0),
                }
            )
    tmp_path.replace(path)


def print_summary(site_name: str, report: dict[str, Any]) -> None:
    print("=" * 72)
    print(f"GEO AUDIT SUMMARY: {site_name}")
    print("=" * 72)
    print(f"Status            : {report.get('status')}")
    print(f"Audited pages     : {report.get('audited_urls')}")
    print(f"Successful        : {report.get('successful_urls')}")
    print(f"Failed/timeouts   : {report.get('failed_urls')}")
    print(f"Average score     : {report.get('average_score')} ({report.get('average_band')})")
    print(f"Avg seconds/page  : {report.get('timing', {}).get('average_seconds_per_page', 0)}")
    print(f"Failure patterns  : {report.get('failure_patterns', {})}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a resumable GEO audit.")
    parser.add_argument("sitemap_url", help="URL of the sitemap or sitemap index.")
    parser.add_argument("--max-urls", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--out", default="reports/run")
    parser.add_argument("--site-name", default=None)
    parser.add_argument("--time-budget-minutes", type=float, default=DEFAULT_TIME_BUDGET_MINUTES)
    parser.add_argument("--checkpoint-file", default=None)
    parser.add_argument("--checkpoint-batch-size", type=int, default=DEFAULT_CHECKPOINT_BATCH_SIZE)
    parser.add_argument("--no-cache", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    site_name = resolve_site_name(args.site_name, args.sitemap_url)
    checkpoint_file = Path(args.checkpoint_file or f"progress/{site_name}.json")
    try:
        run_audit(
            sitemap_url=args.sitemap_url,
            site_name=site_name,
            max_urls=args.max_urls,
            concurrency=args.concurrency,
            out_dir=Path(args.out),
            time_budget_minutes=args.time_budget_minutes,
            checkpoint_file=checkpoint_file,
            checkpoint_batch_size=args.checkpoint_batch_size,
            use_cache=not args.no_cache,
        )
    except Exception as exc:
        print(f"Audit failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
