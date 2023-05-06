from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    LEAGUE_MAPPING,
    LEAGUE_DIVISION_MAPPING,
)


def test_league_mapping():
    expected_result = {"american_league": 103, "national_league": 104}
    assert LEAGUE_MAPPING == expected_result


def test_league_division_mapping():
    expected_result = {103: [200, 201, 202], 104: [203, 204, 205]}
    assert LEAGUE_DIVISION_MAPPING == expected_result
