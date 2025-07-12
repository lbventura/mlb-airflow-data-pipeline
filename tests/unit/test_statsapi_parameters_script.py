from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
)


def test_league_mapping() -> None:
    expected_result: dict[str, int] = {"american_league": 103, "national_league": 104}
    assert LEAGUE_MAPPING == expected_result


def test_league_division_mapping() -> None:
    expected_result: dict[int, list[int]] = {103: [200, 201, 202], 104: [203, 204, 205]}
    assert LEAGUE_DIVISION_MAPPING == expected_result
