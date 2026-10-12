"""Database-only charts, notebooks, and report comparisons."""

import json
from collections.abc import Iterator
from pathlib import Path
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from mlb_airflow_data_pipeline import db_utils, statsapi_parameters_script as parameters
from mlb_airflow_data_pipeline import statsapi_treatment_script as treatment
from mlb_airflow_data_pipeline.report_data import read_report_batters
from mlb_airflow_data_pipeline.statsapi_analysis_script import create_analysis_plots
from mlb_airflow_data_pipeline.statsapi_time_series_creation_analysis_script import (
    PlotGenerator,
    batter_input_parameters,
    league_input_parameters,
)

from .database_helpers import DATE, LEAGUE, extract


def test_database_only_charts(report_database: Path, tmp_path: Path) -> None:
    output = tmp_path / "output"
    create_analysis_plots(str(report_database), LEAGUE, DATE, str(output))
    for config in (league_input_parameters, batter_input_parameters):
        generator = PlotGenerator(
            config, str(report_database), LEAGUE, DATE, str(output)
        )
        generator.get_time_series_stats_plots()
    frame = generator.get_time_series()["homeRuns"]
    assert frame.shape == (2, 2)
    assert frame.index.tolist() == ["2026-10-01", DATE]
    assert len(list(output.glob("*.png"))) == 15
    assert not list(tmp_path.rglob("*.csv"))


@pytest.fixture
def notebook_environment(
    report_database: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    pytest.importorskip("seaborn")

    monkeypatch.setattr(parameters, "OUTPUT_FILE_LOCATION", str(tmp_path / "output"))
    monkeypatch.setattr(db_utils, "DATA_FILE_LOCATION", str(report_database.parent))
    monkeypatch.setenv("MLB_REPORT_DATE", DATE)
    monkeypatch.setenv("MLB_REPORT_LEAGUE", LEAGUE)
    output = str(tmp_path / "output")
    create_analysis_plots(str(report_database), LEAGUE, DATE, output)
    for config in (league_input_parameters, batter_input_parameters):
        PlotGenerator(
            config, str(report_database), LEAGUE, DATE, output
        ).get_time_series_stats_plots()
    yield
    plt.close("all")


@pytest.mark.parametrize(
    "name", ["statsapi_reporting_notebook", "statsapi_team_stats_notebook"]
)
def test_database_only_notebook(name: str, notebook_environment: None) -> None:
    path = Path(__file__).parents[2] / "mlb_airflow_data_pipeline" / f"{name}.ipynb"
    namespace: dict = {}
    for cell in json.loads(path.read_text())["cells"]:
        if cell["cell_type"] == "code":
            exec("".join(cell["source"]), namespace)
    if name == "statsapi_team_stats_notebook":
        assert "player_id" not in namespace["team_averages_data"][LEAGUE]


def test_report_comparison_does_not_cross_calendar_years(
    database: Path, players: pd.DataFrame
) -> None:
    for date in ("2025-12-26", "2026-01-05"):
        extract(database, players, date=date)
        treatment.treat_player_stats(str(database), LEAGUE, date)
    with sqlite3.connect(database) as conn:
        current, previous = read_report_batters(conn, LEAGUE, "2026-01-05")
        assert len(current) == 2
        assert previous is None
