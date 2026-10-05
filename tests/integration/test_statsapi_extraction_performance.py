"""Manual measurements of the serial extractor, without Airflow."""

import os
from pathlib import Path

import pytest

from .extraction_profile import run_supervised

pytestmark = pytest.mark.manual
LEAGUES = ["american_league", "national_league"]


@pytest.mark.parametrize("league_name", LEAGUES)
def test_extraction_profile(league_name: str, tmp_path: Path) -> None:
    root = Path(os.environ.get("STATSAPI_PROFILE_OUTPUT_DIR", str(tmp_path)))
    directory, summary = run_supervised(league_name, True, root)
    assert summary["outcome"] == "success", f"See {directory / 'summary.json'}"


@pytest.mark.parametrize("league_name", LEAGUES)
def test_extraction_benchmark(league_name: str, tmp_path: Path) -> None:
    root = Path(os.environ.get("STATSAPI_PROFILE_OUTPUT_DIR", str(tmp_path)))
    directory, summary = run_supervised(league_name, False, root)
    assert summary["outcome"] == "success", f"See {directory / 'summary.json'}"
