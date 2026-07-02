#!/usr/bin/env python3
"""Generate consolidated GEO summary outputs."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.geo_audit import atomic_write_json, atomic_write_text


CATEGORY_MAX = {
    "robots": 18,
    "llms": 18,
    "schema": 16,
    "meta": 14,
    "content": 12,
    "brand_entity": 10,
    "signals": 6,
    "ai_discovery": 6,
}


def load_reports(results_dir: Path) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for path in sorted(results_dir.glob("*/final.json")):
        with path.open("r", encoding="utf-8") as f:
            report = json.load(f)
        if report.get("status") == "complete":
            reports.append(report)
    return reports


def aggregate_site(report: dict[str, Any]) -> dict[str, Any]:
    pages = report.get("pages", [])
    successes = [page for page in pages if not page.get("error")]
    failures = [page for page in pages if page.get("error")]
    category_totals: dict[str, list[float]] = defaultdict(list)
    for page in successes:
        for category, score in (page.get("score_breakdown") or {}).items():
            if category in CATEGORY_MAX:
                category_totals[category].append(float(score))
    category_breakdown = {
        category: {
            "average": round(statistics.mean(values), 2) if values else 0,
            "max_possible": CATEGORY_MAX[category],
            "percent_of_max": round((statistics.mean(values) / CATEGORY_MAX[category]) * 100, 1) if values else 0,
        }
        for category, values in category_totals.items()
    }
    ranked = sorted(successes, key=lambda page: page.get("score", 0), reverse=True)
    return {
        "site_name": report.get("site_name"),
        "average_score": report.get("average_score", 0),
        "average_band": report.get("average_band", "critical"),
        "audited_urls": report.get("audited_urls", len(pages)),
        "successful_urls": len(successes),
        "failed_urls": len(failures),
        "timeout_rate": round(len(failures) / len(pages), 4) if pages else 0,
        "category_breakdown": category_breakdown,
        "best_pages": ranked[:5],
        "worst_pages": list(reversed(ranked[-5:])),
    }


def build_summary(reports: list[dict[str, Any]]) -> dict[str, Any]:
    sites = [aggregate_site(report) for report in reports]
    all_pages = [page for report in reports for page in report.get("pages", [])]
    category_values: dict[str, list[float]] = defaultdict(list)
    for page in all_pages:
        if page.get("error"):
            continue
        for category, score in (page.get("score_breakdown") or {}).items():
            if category in CATEGORY_MAX:
                category_values[category].append(float(score))
    cross_site_category = {
        category: {
            "average": round(statistics.mean(values), 2) if values else 0,
            "max_possible": CATEGORY_MAX[category],
            "percent_of_max": round((statistics.mean(values) / CATEGORY_MAX[category]) * 100, 1) if values else 0,
        }
        for category, values in category_values.items()
    }
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "site_count": len(sites),
        "total_pages": len(all_pages),
        "cross_site_average_score": round(statistics.mean([site["average_score"] for site in sites]), 2) if sites else 0,
        "cross_site_timeout_rate": round(sum(site["failed_urls"] for site in sites) / len(all_pages), 4) if all_pages else 0,
        "cross_site_category_breakdown": cross_site_category,
        "sites": sites,
    }


def common_findings(summary: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    low_categories = [
        (category, data)
        for category, data in summary.get("cross_site_category_breakdown", {}).items()
        if data.get("percent_of_max", 100) < 50
    ]
    for category, data in sorted(low_categories, key=lambda item: item[1].get("percent_of_max", 0))[:3]:
        findings.append(f"{category} averages {data['average']} / {data['max_possible']} across completed properties.")
    if summary.get("cross_site_timeout_rate", 0) > 0.2:
        findings.append(f"Timeout/failure rate is {summary['cross_site_timeout_rate']:.1%} across audited pages.")
    return findings[:3] or ["No uniform cross-site finding is available yet; complete more site reports first."]


def build_markdown(summary: dict[str, Any]) -> str:
    lines = ["# GEO Audit Report", ""]
    lines.append("## Headline Scores")
    for site in summary.get("sites", []):
        lines.append(f"- {site['site_name']}: {site['average_score']} ({site['average_band']}), timeout rate {site['timeout_rate']:.1%}")
    lines.append("")
    lines.append("## Consistent Findings")
    for finding in common_findings(summary):
        lines.append(f"- {finding}")
    lines.append("")
    lines.append("## Prioritized Fix List")
    lines.append("1. Add or repair AI crawler instruction files (`llms.txt` and `ai.txt`) where missing.")
    lines.append("2. Improve schema coverage on templates with weak structured data.")
    lines.append("3. Fix repeated timeout/dead URL patterns surfaced in each site's failure list.")
    lines.append("4. Rewrite or expand low-scoring content only after structural fixes are complete.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate consolidated GEO reports.")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--out-json", default="summary.json")
    parser.add_argument("--out-md", default="REPORT.md")
    args = parser.parse_args()

    summary = build_summary(load_reports(Path(args.results_dir)))
    atomic_write_json(Path(args.out_json), summary)
    atomic_write_text(Path(args.out_md), build_markdown(summary))


if __name__ == "__main__":
    main()
