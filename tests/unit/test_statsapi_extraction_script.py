import pandas as pd

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    _extract_player_name,
    _generate_player_stats,
    _insert_col_in_first_position,
)


def test__insert_col_in_first_position():
    col_2_data = ["a", "b", "c", "d"]
    test_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": col_2_data})

    first_column = "col_2"
    _insert_col_in_first_position(test_df, column_name=first_column)
    assert test_df.columns[0] == first_column
    assert test_df[first_column].to_list() == col_2_data


def test__extract_player_name():
    input_string = "#99  CF  Aaron Judge"
    expected_result = "Aaron Judge"
    assert _extract_player_name(input_string) == expected_result


def test__generate_player_stats(player_stats, player_stats_list):
    generated_result = _generate_player_stats(player_stats)
    expected_result = player_stats_list
    assert sorted(generated_result.keys()) == expected_result
