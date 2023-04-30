from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    _insert_col_in_first_position,
    _extract_player_name,
    _generate_player_stats,
)


def test__insert_col_in_first_position():
    # TODO: test _insert_col_in_first_position
    assert 1 == 1


def test__extract_player_name():
    input_string = "#99  CF  Aaron Judge"
    expected_result = "Aaron Judge"
    assert _extract_player_name(input_string) == expected_result


def test__generate_player_stats(player_stats, player_stats_list):

    generated_result = _generate_player_stats(player_stats)
    expected_result = player_stats_list
    assert sorted(generated_result.keys()) == expected_result
