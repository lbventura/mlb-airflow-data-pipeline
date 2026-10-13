"""Report calculations and snapshot selection, independent of Jupyter and SQLite."""

from datetime import date
from unittest.mock import Mock, call

import pandas as pd
import pytest

from mlb_airflow_data_pipeline import report_data
from mlb_airflow_data_pipeline.report_data import (
    batter_difference,
    read_report_batters,
    report_scope,
    team_metric_averages,
)
from mlb_airflow_data_pipeline.treated_storage import SnapshotUnavailable


def batters(rows: list[tuple], layout: str = "grouped") -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["player_id", "team_id", "hits"]).assign(
        playername="Same Name", layout=layout
    )


@pytest.mark.parametrize("layout", ["grouped", "legacy_unprefixed"])
def test_difference_matches_ids_and_teams_and_keeps_current_name(layout: str) -> None:
    current = batters([(101, 147, 10), (102, 147, 30), (103, 111, 40)], layout)
    previous = batters([(102, 147, 20), (101, 147, 5), (103, 147, 8)], layout)
    previous["playername"] = "Former Name"
    result = batter_difference(current, previous, "hits")
    assert result.set_index("player_id")["hitsdifference"].to_dict() == {
        101: 5,
        102: 10,
    }
    assert result.playername.tolist() == ["Same Name", "Same Name"]


@pytest.mark.parametrize(
    "previous",
    [None, batters([]), batters([(101, 147, 3)], "legacy_unprefixed")],
    ids=["missing", "empty", "different-layout"],
)
def test_unavailable_comparison_has_empty_difference(
    previous: pd.DataFrame | None,
) -> None:
    result = batter_difference(batters([(101, 147, 5)]), previous, "hits")
    assert result.empty
    assert "hitsdifference" in result


def test_empty_current_snapshot_has_empty_difference() -> None:
    assert batter_difference(batters([]), batters([(101, 147, 3)]), "hits").empty


@pytest.mark.parametrize("duplicate_side", ["current", "previous"])
def test_ambiguous_identity_raises(duplicate_side: str) -> None:
    frame = batters([(101, 147, 5)])
    snapshots = {"current": frame, "previous": frame}
    snapshots[duplicate_side] = pd.concat([frame, frame])
    with pytest.raises(pd.errors.MergeError):
        batter_difference(**snapshots, metric="hits")


def test_missing_metric_values_remain_missing() -> None:
    result = batter_difference(
        batters([(101, 147, None)]), batters([(101, 147, 5)]), "hits"
    )
    assert result.hitsdifference.isna().all()


def test_team_averages_use_only_metrics_and_skip_missing_values() -> None:
    frame = batters([(101, 147, 4), (102, 147, 6), (103, 111, 9), (104, 111, None)])
    frame["hitsperplateAppearance"] = [0.1, 0.3, 0.5, None]
    result = team_metric_averages(frame)
    assert result.columns.tolist() == ["hits", "hitsperplateAppearance"]
    assert result.hits.to_dict() == {147: 5, 111: 9}
    assert result.hitsperplateAppearance.to_dict() == {147: 0.2, 111: 0.5}


def test_empty_team_averages_preserve_metric_columns() -> None:
    result = team_metric_averages(batters([]))
    assert result.empty
    assert result.columns.tolist() == ["hits"]


def test_report_scope_uses_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLB_REPORT_LEAGUE", "national_league")
    monkeypatch.setenv("MLB_REPORT_DATE", "2026-10-11")
    assert report_scope() == ("national_league", "2026-10-11")


def test_report_scope_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MLB_REPORT_LEAGUE", raising=False)
    monkeypatch.delenv("MLB_REPORT_DATE", raising=False)
    assert report_scope() == (report_data.LEAGUE_NAME, date.today().isoformat())


@pytest.mark.parametrize(
    ("league", "day"), [("unknown", "2026-10-11"), ("american_league", "20261011")]
)
def test_report_scope_rejects_invalid_parameters(monkeypatch, league, day) -> None:
    monkeypatch.setenv("MLB_REPORT_LEAGUE", league)
    monkeypatch.setenv("MLB_REPORT_DATE", day)
    with pytest.raises(ValueError):
        report_scope()


def test_comparison_reads_exactly_ten_days_earlier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current, previous = batters([(101, 147, 10)]), batters([(101, 147, 5)])
    reader = Mock(side_effect=[current, previous])
    monkeypatch.setattr(report_data, "read_treated_stats", reader)
    conn = Mock()
    actual_current, actual_previous = read_report_batters(
        conn, "american_league", "2026-10-05"
    )
    assert actual_current is current
    assert actual_previous is previous
    assert reader.call_args_list == [
        call(conn, "batter", "american_league", "2026-10-05"),
        call(conn, "batter", "american_league", "2026-09-25"),
    ]


def test_comparison_does_not_cross_calendar_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = batters([(101, 147, 10)])
    reader = Mock(return_value=current)
    monkeypatch.setattr(report_data, "read_treated_stats", reader)
    conn = Mock()
    actual_current, actual_previous = read_report_batters(
        conn, "american_league", "2026-01-05"
    )
    assert actual_current is current
    assert actual_previous is None
    reader.assert_called_once_with(conn, "batter", "american_league", "2026-01-05")


def test_missing_previous_snapshot_is_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    current = batters([(101, 147, 10)])
    reader = Mock(side_effect=[current, SnapshotUnavailable("not treated")])
    monkeypatch.setattr(report_data, "read_treated_stats", reader)
    actual_current, actual_previous = read_report_batters(
        Mock(), "american_league", "2026-10-11"
    )
    assert actual_current is current
    assert actual_previous is None


@pytest.mark.parametrize("failed_read", ["current", "previous"])
def test_unexpected_read_errors_propagate(monkeypatch, failed_read) -> None:
    error = ValueError("row count mismatch")
    results = [error] if failed_read == "current" else [batters([]), error]
    monkeypatch.setattr(report_data, "read_treated_stats", Mock(side_effect=results))
    with pytest.raises(ValueError, match="row count mismatch") as raised:
        read_report_batters(Mock(), "american_league", "2026-10-11")
    assert raised.value is error


def test_missing_current_snapshot_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        report_data,
        "read_treated_stats",
        Mock(side_effect=SnapshotUnavailable("missing")),
    )
    with pytest.raises(SnapshotUnavailable):
        read_report_batters(Mock(), "american_league", "2026-10-11")
