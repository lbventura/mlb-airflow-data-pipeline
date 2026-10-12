"""Ranking and rendering behavior for time-series charts."""

from pathlib import Path

from matplotlib.figure import Figure
import pandas as pd
import pytest

from mlb_airflow_data_pipeline.statsapi_time_series_creation_analysis_script import (
    PlotGenerator,
    PlotterInputRepresentation,
    sort_dataframe_by_largest_values,
)


def test_series_rank_by_last_day_and_place_missing_values_last() -> None:
    frame = pd.DataFrame(
        {"Earlier leader": [100, 2], "Latest leader": [1, 10], "Missing": [5, None]}
    )
    original = frame.copy()
    ranked = sort_dataframe_by_largest_values(frame)
    assert ranked.columns.tolist() == ["Latest leader", "Earlier leader", "Missing"]
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("n_best", [1, -1], ids=["best-only", "all-series"])
def test_charts_forward_fill_missing_days_and_select_best_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, n_best: int
) -> None:
    config = PlotterInputRepresentation(
        dataset_name="batter_stats",
        time_series_stats_is_int={"homeRuns": True},
        n_best=n_best,
    )
    frame = pd.DataFrame(
        {"Leader": [10, None], "Second": [3, 8]}, index=["2026-10-01", "2026-10-11"]
    )
    monkeypatch.setattr(
        PlotGenerator, "get_time_series", lambda self: {"homeRuns": frame.copy()}
    )
    savefig = Figure.savefig

    def check_chart(figure, filename, **kwargs):
        lines = figure.axes[0].lines
        assert lines[0].get_label() == "Leader"
        assert lines[0].get_ydata().tolist() == [10, 10]
        assert len(lines) == (1 if n_best == 1 else 2)
        savefig(figure, filename, **kwargs)

    monkeypatch.setattr(Figure, "savefig", check_chart)
    PlotGenerator(
        config, tmp_path / "unused.db", "american_league", "2026-10-11", str(tmp_path)
    ).get_time_series_stats_plots()
    assert (
        tmp_path / "american_league_2026-10-11_time_series_batter_stats_homeRuns.png"
    ).stat().st_size > 0
    assert pd.isna(frame.loc["2026-10-11", "Leader"])


def test_unsupported_dataset_is_rejected(tmp_path: Path) -> None:
    config = PlotterInputRepresentation(
        dataset_name="defender_stats", time_series_stats_is_int={}, n_best=1
    )
    with pytest.raises(ValueError, match="Unsupported time series"):
        PlotGenerator(config, tmp_path / "unused.db", "american_league", "2026-10-11")
