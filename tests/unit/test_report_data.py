import pandas as pd
import pytest

from mlb_airflow_data_pipeline.report_data import (
    batter_difference,
    team_metric_averages,
)


def batters(rows: list[tuple], layout: str = "grouped") -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["player_id", "team_id", "hits"]).assign(
        playername="Same Name", layout=layout, source_row=range(len(rows))
    )


def test_difference_joins_ids_and_teams_independent_of_row_index() -> None:
    current = batters([(101, 147, 10), (102, 147, 30), (103, 111, 40)])
    previous = batters([(102, 147, 20), (101, 147, 5), (103, 147, 8)])
    result = batter_difference(current, previous, "hits")
    assert result is not None
    assert result.set_index("player_id")["hitsdifference"].to_dict() == {
        101: 5,
        102: 10,
    }


@pytest.mark.parametrize(
    "previous", [None, pd.DataFrame(), batters([(None, 147, 3)], "legacy_unprefixed")]
)
def test_missing_or_incompatible_comparison_is_unavailable(
    previous: pd.DataFrame | None,
) -> None:
    assert batter_difference(batters([(101, 147, 5)]), previous, "hits") is None


def test_legacy_comparison_requires_unique_names_per_team() -> None:
    current = batters([(None, 147, 5)], "legacy_unprefixed")
    previous = batters([(None, 147, 3)], "legacy_unprefixed")
    result = batter_difference(current, previous, "hits")
    assert result is not None
    assert result.hitsdifference.tolist() == [2]
    with pytest.raises(ValueError, match="Ambiguous"):
        batter_difference(pd.concat([current, current]), previous, "hits")


def test_team_averages_exclude_identity_and_provenance() -> None:
    frame = batters([(101, 147, 4), (102, 147, 6)])
    result = team_metric_averages(frame)
    assert result.columns.tolist() == ["hits"]
    assert result.loc[147, "hits"] == 5
