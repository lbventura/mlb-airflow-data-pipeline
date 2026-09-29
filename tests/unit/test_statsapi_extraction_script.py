from typing import Any
from unittest.mock import Mock

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction_script
from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    _get_team_roster_players,
    _generate_player_stats,
    _insert_col_in_first_position,
)


def test__insert_col_in_first_position() -> None:
    col_2_data: list[str] = ["a", "b", "c", "d"]
    test_df: pd.DataFrame = pd.DataFrame.from_dict(
        {"col_1": [3, 2, 1, 0], "col_2": col_2_data}
    )

    first_column: str = "col_2"
    _insert_col_in_first_position(test_df, column_name=first_column)
    assert test_df.columns[0] == first_column
    assert test_df[first_column].to_list() == col_2_data


def test__generate_player_stats(
    player_stats: Any, player_stats_list: list[str]
) -> None:
    generated_result: dict[str, Any] = _generate_player_stats(player_stats)
    expected_result: list[str] = player_stats_list
    assert sorted(generated_result.keys()) == expected_result


@pytest.mark.parametrize(
    ("response", "missing_key", "event"),
    [
        ({}, "roster", "malformed_team_roster"),
        (
            {"roster": [{"person": {"id": 101}}]},
            "fullName",
            "malformed_roster_entry",
        ),
    ],
)
def test_malformed_roster_logs_context_and_preserves_error(
    monkeypatch: pytest.MonkeyPatch,
    response: dict,
    missing_key: str,
    event: str,
) -> None:
    monkeypatch.setattr(statsapi, "get", Mock(return_value=response))
    logger = Mock()
    monkeypatch.setattr(extraction_script, "logger", logger)

    with pytest.raises(KeyError) as error:
        _get_team_roster_players(147)

    assert error.value.args == (missing_key,)
    logger.error.assert_called_once_with(
        event, team_id=147, error=str(error.value), exc_info=True
    )


def test_duplicate_roster_id_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        statsapi,
        "get",
        Mock(
            return_value={
                "roster": [
                    {"person": {"id": 101, "fullName": "Player One"}},
                    {"person": {"id": 101, "fullName": "Other Name"}},
                ]
            }
        ),
    )

    with pytest.raises(ValueError, match="Duplicate player ID 101 in team 147 roster"):
        _get_team_roster_players(147)
