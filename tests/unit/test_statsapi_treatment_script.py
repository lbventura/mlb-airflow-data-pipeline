from pathlib import Path

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.statsapi_treatment_script import (
    DataTreater,
    DataTreaterInputRepresentation,
    TreatmentDataPaths,
    _convert_numeric_columns,
    _fielding_subset,
    _grouped_subset,
    defender_input_data_repr,
    filter_data,
)


def test_treatment_filters_before_applying_configured_transformations(
    tmp_path: Path,
) -> None:
    source = pd.DataFrame({"hits": [9, 10, 20], "unused": [1, 2, 3]})
    original = source.copy()

    def double_hits(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.assign(doubled_hits=frame.hits * 2)

    def normalize(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        return frame.assign(
            **{
                f"{column}_share": frame[column] / frame[column].sum()
                for column in columns
            }
        )

    config = DataTreaterInputRepresentation(
        subset_columns=["hits"],
        filter_conditions_dict={"hits": 10},
        transformation_dict={double_hits: None, normalize: ["doubled_hits"]},
        stats_group="hitting",
    )
    paths = TreatmentDataPaths(
        database_path=tmp_path / "unused.db",
        league_name="american_league",
        execution_date="2026-10-14",
    )

    result = DataTreater(paths, config, source).get_output_data()

    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "hits": [10, 20],
                "doubled_hits": [20, 40],
                "doubled_hits_share": [1 / 3, 2 / 3],
            },
            index=[1, 2],
        ),
    )
    pd.testing.assert_frame_equal(source, original)


def test_convert_known_metrics_preserves_numbers_and_missing_rates() -> None:
    result = _convert_numeric_columns(
        pd.DataFrame(
            {
                "hits": ["12", "8", None],
                "hitting_avg": [".300", ".250", None],
                "fielding_C_rangeFactorPer9Inn": ["-.--", "2.50", None],
                "playername": ["123", "Player Two", "Player Three"],
                "position": ["C", "1B", "RF"],
            }
        )
    )

    assert result.loc[:1, "hits"].tolist() == [12, 8]
    assert result.loc[:1, "hitting_avg"].tolist() == [0.300, 0.250]
    assert result.loc[1, "fielding_C_rangeFactorPer9Inn"] == 2.5
    assert result.loc[[0, 2], "fielding_C_rangeFactorPer9Inn"].isna().all()
    assert result["playername"].tolist() == ["123", "Player Two", "Player Three"]
    assert result["position"].tolist() == ["C", "1B", "RF"]


def test_grouped_projection_rejects_missing_required_columns() -> None:
    data = pd.DataFrame(
        [{"playername": "Player", "team_id": 147, "hitting_atBats": 50}]
    )
    with pytest.raises(KeyError, match="hitting_hits"):
        _grouped_subset(data, ["playername", "team_id", "hits"], "hitting")


def test_absent_group_has_an_empty_subset() -> None:
    data = pd.DataFrame([{"playername": "Player", "team_id": 147, "hitting_hits": 4}])
    result = _grouped_subset(data, ["playername", "team_id", "gamesPlayed"], "pitching")
    assert result.empty
    assert result.columns.tolist() == ["playername", "team_id", "gamesPlayed"]


def test_filter_keeps_boundary_and_excludes_insufficient_or_missing_stats() -> None:
    data = pd.DataFrame(
        {"atBats": [50, 50, 49, 50], "plateAppearances": [100, 99, 100, None]},
        index=[101, 102, 103, 104],
    )
    result = filter_data(data, {"atBats": 50, "plateAppearances": 100})
    assert result.index.tolist() == [101]


def test_defenders_follow_fixed_position_order_and_exclude_pitcher_and_dh() -> None:
    positions = ("C", "1B", "2B", "3B", "SS", "LF", "CF", "RF")
    row = {
        "playername": "Player",
        "team_id": 147,
        "hitting_atBats": 50,
        "hitting_plateAppearances": 100,
        "fielding_C_catcherERA": 3.25,
        **{
            f"fielding_{position}_gamesPlayed": 1
            for position in reversed(positions + ("P", "DH"))
        },
    }
    result = _fielding_subset(
        pd.DataFrame([row]), defender_input_data_repr.subset_columns
    )
    assert result["position"].tolist() == list(positions)
    assert result["atBats"].tolist() == [50] * 8
    assert result.loc[0, "catcherERA"] == 3.25
    assert result.loc[1:, "catcherERA"].isna().all()


@pytest.mark.parametrize("position", ["P", "DH", None])
def test_no_defender_positions_produces_an_empty_filterable_subset(
    position: str | None,
) -> None:
    row = {
        "playername": "Player",
        "team_id": 147,
        "hitting_atBats": 50,
        "hitting_plateAppearances": 100,
    }
    if position:
        row[f"fielding_{position}_gamesPlayed"] = 10
    subset = _fielding_subset(
        pd.DataFrame([row]), defender_input_data_repr.subset_columns
    )
    result = filter_data(subset, defender_input_data_repr.filter_conditions_dict)
    assert result.empty
    assert result.columns.tolist() == defender_input_data_repr.subset_columns + [
        "position"
    ]
