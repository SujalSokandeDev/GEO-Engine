from __future__ import annotations

import json
from pathlib import Path

from scripts import generate_report


def test_generate_report_outputs_summary_and_markdown(tmp_path: Path) -> None:
    result_dir = tmp_path / "results" / "site"
    result_dir.mkdir(parents=True)
    (result_dir / "final.json").write_text(
        json.dumps(
            {
                "status": "complete",
                "site_name": "site",
                "average_score": 70,
                "average_band": "good",
                "audited_urls": 1,
                "pages": [
                    {
                        "url": "https://e.test/1",
                        "score": 70,
                        "error": None,
                        "score_breakdown": {"ai_discovery": 0, "robots": 15},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = generate_report.build_summary(generate_report.load_reports(tmp_path / "results"))
    markdown = generate_report.build_markdown(summary)

    assert summary["site_count"] == 1
    assert summary["sites"][0]["site_name"] == "site"
    assert "site: 70" in markdown
