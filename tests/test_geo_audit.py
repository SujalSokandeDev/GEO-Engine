from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import geo_audit


def make_page(url: str, score: int = 75) -> dict:
    return {
        "url": url,
        "score": score,
        "band": "good",
        "http_status": 200,
        "error": None,
        "score_breakdown": {
            "robots": 15,
            "llms": 10,
            "schema": 12,
            "meta": 14,
            "content": 10,
            "signals": 6,
            "ai_discovery": 0,
            "brand_entity": 5,
        },
        "recommendations_count": 1,
        "duration_seconds": 0.01,
        "attempts": 1,
        "structured_data_extra": {
            "counts": {"json-ld": 1, "microdata": 0, "rdfa": 0, "opengraph": 2}
        },
        "content_length": 1234,
        "audited_at": geo_audit.utc_now(),
    }


def test_checkpoint_save_load_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkpoint = tmp_path / "progress" / "site.json"
    monkeypatch.setattr(geo_audit, "discover_urls", lambda sitemap_url, max_urls=None: ["https://e.test/1", "https://e.test/2"])
    monkeypatch.setattr(geo_audit, "crawl_stats", lambda sitemap_url, urls: {"url_count": len(urls)})

    created = geo_audit.load_or_create_checkpoint(
        checkpoint,
        site_name="site",
        sitemap_url="https://e.test/sitemap.xml",
        max_urls=None,
    )
    created["results"].append(make_page("https://e.test/1"))
    created["completed_urls"].append("https://e.test/1")
    geo_audit.save_checkpoint(checkpoint, created)

    loaded = geo_audit.load_or_create_checkpoint(
        checkpoint,
        site_name="site",
        sitemap_url="https://e.test/sitemap.xml",
        max_urls=None,
    )

    assert loaded["completed_urls"] == ["https://e.test/1"]
    assert loaded["results"][0]["url"] == "https://e.test/1"
    assert not list(checkpoint.parent.glob("*.tmp"))


def test_low_time_budget_stops_cleanly_in_progress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkpoint = tmp_path / "progress" / "site.json"
    out_dir = tmp_path / "reports"
    urls = [f"https://e.test/{index}" for index in range(30)]
    calls = {"count": 0}

    monkeypatch.setattr(geo_audit, "discover_urls", lambda sitemap_url, max_urls=None: urls)
    monkeypatch.setattr(geo_audit, "crawl_stats", lambda sitemap_url, discovered: {"url_count": len(discovered)})

    def fake_audit(url: str, *, use_cache: bool, attempts: int = 3) -> dict:
        calls["count"] += 1
        return make_page(url)

    times = iter([0.0, 0.0, 999.0, 999.0])
    monkeypatch.setattr(geo_audit.time, "monotonic", lambda: next(times, 999.0))
    monkeypatch.setattr(geo_audit, "audit_one_url", fake_audit)

    report = geo_audit.run_audit(
        sitemap_url="https://e.test/sitemap.xml",
        site_name="site",
        max_urls=None,
        concurrency=1,
        out_dir=out_dir,
        time_budget_minutes=0.001,
        checkpoint_file=checkpoint,
        checkpoint_batch_size=1,
        commit_checkpoints=False,
        use_cache=False,
    )

    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert calls["count"] == 1
    assert saved["status"] == "in_progress"
    assert report["status"] == "in_progress"
    assert saved["completed_urls"] == ["https://e.test/0"]


def test_completed_checkpoint_writes_final_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkpoint = tmp_path / "progress" / "site.json"
    out_dir = tmp_path / "reports"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(geo_audit, "discover_urls", lambda sitemap_url, max_urls=None: ["https://e.test/1"])
    monkeypatch.setattr(geo_audit, "crawl_stats", lambda sitemap_url, urls: {"url_count": len(urls)})
    monkeypatch.setattr(geo_audit, "audit_one_url", lambda url, *, use_cache, attempts=3: make_page(url))

    report = geo_audit.run_audit(
        sitemap_url="https://e.test/sitemap.xml",
        site_name="site",
        max_urls=None,
        concurrency=1,
        out_dir=out_dir,
        time_budget_minutes=1,
        checkpoint_file=checkpoint,
        checkpoint_batch_size=20,
        commit_checkpoints=False,
        use_cache=False,
    )

    assert report["status"] == "complete"
    assert (tmp_path / "results" / "site" / "final.json").exists()
    assert (tmp_path / "results" / "site" / "final.csv").exists()
