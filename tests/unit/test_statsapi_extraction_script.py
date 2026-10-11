from itertools import permutations
from unittest.mock import Mock

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction_script
from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    _get_team_roster_players,
    _generate_player_stats,
    _insert_col_in_first_position,
    _validate_player_stats_columns,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    PITCHING_STATS,
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


@pytest.mark.parametrize("player_order", list(permutations((101, 202, 303))))
def test_mixed_roster_preserves_player_names_and_stats(
    monkeypatch: pytest.MonkeyPatch, player_order: tuple[int, ...]
) -> None:
    players = {101: "Hitter A", 202: "Pitcher B", 303: "Hitter C"}
    responses: dict[int, dict] = {}
    for player_id in players:
        group = "pitching" if player_id == 202 else "hitting"
        stat_names = PITCHING_STATS if group == "pitching" else BATTING_STATS
        responses[player_id] = {
            "people": [
                {
                    "stats": [
                        {
                            "group": {"displayName": group},
                            "splits": [{"stat": dict.fromkeys(stat_names, player_id)}],
                        }
                    ]
                }
            ]
        }
    responses[404] = {"people": [{"id": 404}]}
    monkeypatch.setattr(
        statsapi,
        "get",
        Mock(side_effect=lambda endpoint, params: responses[params["personId"]]),
    )
    roster = {player_id: players[player_id] for player_id in player_order}
    roster[404] = "Inactive"
    extractor = extraction_script.DataExtractor()
    extractor.league_team_roster_players = {147: roster}

    rows, inactive = extractor.get_player_stats_dataframe_per_team(147)

    assert inactive == {404: "Inactive"}
    assert rows["playername"].to_dict() == players
    assert rows.loc[101, "hitting_hits"] == "101"
    assert rows.loc[303, "hitting_hits"] == "303"
    assert rows.loc[202, "pitching_outs"] == "202"
    assert rows["team_id"].eq(147).all()


@pytest.mark.parametrize("reverse_sections", [False, True])
def test_same_named_stats_keep_their_group_and_position(
    reverse_sections: bool,
) -> None:
    sections = [
        {"group": "hitting", "stats": {"hits": 0, "stolenBases": 4}},
        {"group": "pitching", "stats": {"hits": 43, "stolenBases": 5}},
        {
            "group": "fielding",
            "stats": {
                "position": {"abbreviation": "C"},
                "stolenBases": 50,
                "gamesPlayed": 120,
            },
        },
        {
            "group": "fielding",
            "stats": {"position": {"abbreviation": "1B"}, "gamesPlayed": 2},
        },
    ]
    if reverse_sections:
        sections.reverse()

    result = _generate_player_stats(
        [
            {
                "group": {"displayName": section["group"]},
                "splits": [{"stat": section["stats"]}],
            }
            for section in sections
        ]
    )

    assert result == {
        "hitting_hits": "0",
        "pitching_hits": "43",
        "hitting_stolenBases": "4",
        "pitching_stolenBases": "5",
        "fielding_C_stolenBases": "50",
        "fielding_C_gamesPlayed": "120",
        "fielding_1B_gamesPlayed": "2",
    }


def test_duplicate_stat_across_sections_is_rejected() -> None:
    section = {"group": {"displayName": "hitting"}, "splits": [{"stat": {"hits": 1}}]}
    with pytest.raises(ValueError, match="Ambiguous player stats: hitting"):
        _generate_player_stats([section, section])


@pytest.mark.parametrize("reverse_splits", [False, True])
def test_traded_player_uses_combined_totals_and_retains_single_team_positions(
    reverse_splits: bool,
) -> None:
    hitting = [
        {"team": {"id": 119}, "stat": {"hits": 15, "avg": ".150"}},
        {"stat": {"hits": 48, "avg": ".240"}, "numTeams": 2},
        {"team": {"id": 139}, "stat": {"hits": 33, "avg": ".330"}},
    ]
    fielding = [
        {
            "team": {"id": 119},
            "stat": {"position": {"abbreviation": "C"}, "gamesPlayed": 31},
        },
        {"stat": {"position": {"abbreviation": "C"}, "gamesPlayed": 90}, "numTeams": 2},
        {
            "team": {"id": 139},
            "stat": {"position": {"abbreviation": "C"}, "gamesPlayed": 59},
        },
        {
            "team": {"id": 119},
            "stat": {"position": {"abbreviation": "1B"}, "gamesPlayed": 1},
        },
    ]
    if reverse_splits:
        hitting.reverse()
        fielding.reverse()
    assert _generate_player_stats(
        [
            {"group": {"displayName": "hitting"}, "splits": hitting},
            {"group": {"displayName": "fielding"}, "splits": fielding},
        ]
    ) == {
        "hitting_hits": "48",
        "hitting_avg": ".240",
        "fielding_C_gamesPlayed": "90",
        "fielding_1B_gamesPlayed": "1",
    }


def test_multiple_team_splits_without_a_total_are_rejected() -> None:
    with pytest.raises(ValueError, match="Ambiguous player stats: hitting"):
        _generate_player_stats(
            [
                {
                    "group": {"displayName": "hitting"},
                    "splits": [
                        {"team": {"id": 119}, "stat": {"hits": 15}},
                        {"team": {"id": 139}, "stat": {"hits": 33}},
                    ],
                }
            ]
        )


@pytest.mark.parametrize(
    ("removed", "added", "message"),
    [
        ({"hitting_hits"}, set(), "hitting_hits"),
        ({"fielding_C_gamesPlayed"}, set(), "fielding_C_gamesPlayed"),
        (set(), {"hitting_unrecognizedStat"}, "Unexpected player stats"),
        (set(), {"fielding_ZZ_gamesPlayed"}, "fielding_ZZ_gamesPlayed"),
    ],
)
def test_validator_rejects_missing_required_and_unknown_grouped_stats(
    removed: set[str], added: set[str], message: str
) -> None:
    columns = {
        "playername",
        "team_id",
        "date",
        "fielding_C_gamesPlayed",
        "fielding_C_stolenBases",
    }
    columns.update(f"hitting_{stat}" for stat in BATTING_STATS)
    columns.update(f"pitching_{stat}" for stat in PITCHING_STATS)
    _validate_player_stats_columns(columns)

    with pytest.raises(ValueError, match=message):
        _validate_player_stats_columns((columns - removed) | added)


def test_player_without_pitching_or_fielding_stats_is_valid() -> None:
    _validate_player_stats_columns(f"hitting_{stat}" for stat in BATTING_STATS)


def test_missing_player_stat_is_not_hidden_by_another_player(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    complete = dict.fromkeys(BATTING_STATS, 0)
    incomplete = {name: value for name, value in complete.items() if name != "hits"}
    monkeypatch.setattr(
        statsapi,
        "get",
        Mock(
            side_effect=[
                {
                    "people": [
                        {
                            "stats": [
                                {
                                    "group": {"displayName": "hitting"},
                                    "splits": [{"stat": stats}],
                                }
                            ]
                        }
                    ]
                }
                for stats in (complete, incomplete)
            ]
        ),
    )
    with pytest.raises(ValueError, match="hitting_hits"):
        extraction_script.TeamStats(
            {101: "Complete", 102: "Incomplete"}
        ).get_team_stats()


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
