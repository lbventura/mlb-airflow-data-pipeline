from pathlib import Path

from mlb_airflow_data_pipeline.statsapi_analysis_script import create_analysis_plots
from mlb_airflow_data_pipeline.statsapi_time_series_creation_analysis_script import (
    PlotGenerator,
    batter_input_parameters,
    league_input_parameters,
)

from .database_helpers import DATE, LEAGUE


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
