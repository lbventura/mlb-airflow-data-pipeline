from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    expected_output_columns,
)


def test_league_mapping() -> None:
    expected_result: dict[str, int] = {"american_league": 103, "national_league": 104}
    assert LEAGUE_MAPPING == expected_result


def test_league_division_mapping() -> None:
    expected_result: dict[int, list[int]] = {103: [200, 201, 202], 104: [203, 204, 205]}
    assert LEAGUE_DIVISION_MAPPING == expected_result


def test_expected_output_columns_include_group_and_fielding_position() -> None:
    columns = expected_output_columns()

    assert {"playername", "team_id", "date"} <= set(columns)
    assert {
        "hitting_hits",
        "pitching_hits",
        "hitting_stolenBases",
        "fielding_C_stolenBases",
        "fielding_P_stolenBases",
    } <= set(columns)
    assert "hits" not in columns
    assert "pitching_innings" not in columns
    assert "fielding_1B_stolenBases" in columns
    assert "fielding_ZZ_gamesPlayed" not in columns
