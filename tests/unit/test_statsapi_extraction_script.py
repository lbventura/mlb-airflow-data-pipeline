from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import create_connection, save_extraction_run
from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    _extract_player_name,
    _generate_player_stats,
    _insert_col_in_first_position,
    run_extraction,
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


def test__extract_player_name() -> None:
    input_string: str = "#99  CF  Aaron Judge"
    expected_result: str = "Aaron Judge"
    assert _extract_player_name(input_string) == expected_result


def test__generate_player_stats(
    player_stats: Any, player_stats_list: list[str]
) -> None:
    generated_result: dict[str, Any] = _generate_player_stats(player_stats)
    expected_result: list[str] = player_stats_list
    assert sorted(generated_result.keys()) == expected_result


def test_failed_team_preserves_previous_snapshot(tmp_path: Path) -> None:
    database_path = str(tmp_path / "runs.db")
    with create_connection(database_path) as conn:
        standings = pd.DataFrame({"team_id": [1], "name": ["Original"]})
        players = pd.DataFrame(
            {"team_id": [1], "playername": ["Alex"]}, index=[10]
        )
        save_extraction_run(conn, "american_league", "2026-09-26", standings, players)

    with patch(
        "mlb_airflow_data_pipeline.statsapi_extraction_script.DataExtractor"
    ) as extractor_class:
        extractor = extractor_class.return_value
        extractor.league_standings = pd.DataFrame(
            {"team_id": [1], "name": ["Changed"]}
        )
        extractor.team_id_name_mapping = {1: "Changed"}
        extractor.get_player_stats_per_league.return_value = (
            pd.DataFrame({"team_id": [1], "playername": ["Alex"]}, index=[10]),
            {},
            ["Failed team"],
        )
        with pytest.raises(RuntimeError, match="Failed team"):
            run_extraction(database_path, "american_league", "2026-09-26")

    with create_connection(database_path) as conn:
        assert conn.execute("SELECT name FROM league_standings").fetchall() == [
            ("Original",)
        ]
        assert conn.execute("SELECT playername FROM player_stats").fetchall() == [
            ("Alex",)
        ]


def test_complete_run_saves_extracted_rows(tmp_path: Path) -> None:
    database_path = str(tmp_path / "runs.db")
    with patch(
        "mlb_airflow_data_pipeline.statsapi_extraction_script.DataExtractor"
    ) as extractor_class:
        extractor = extractor_class.return_value
        extractor.league_standings = pd.DataFrame(
            {"team_id": [1], "name": ["Team"]}
        )
        extractor.team_id_name_mapping = {1: "Team"}
        extractor.get_player_stats_per_league.return_value = (
            pd.DataFrame({"team_id": [1], "playername": ["Alex"]}, index=[10]),
            {},
            [],
        )
        run_extraction(database_path, "american_league", "2026-09-26")

    with create_connection(database_path) as conn:
        assert conn.execute(
            "SELECT league_name, date, team_id FROM league_standings"
        ).fetchall() == [("american_league", "2026-09-26", 1)]
        assert conn.execute(
            "SELECT league_name, date, team_id, player_id FROM player_stats"
        ).fetchall() == [("american_league", "2026-09-26", 1, 10)]
