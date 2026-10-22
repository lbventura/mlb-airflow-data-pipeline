from concurrent.futures import Future, ThreadPoolExecutor
from itertools import permutations
from threading import Event, Lock
from unittest.mock import ANY, Mock

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


@pytest.mark.parametrize("roster", [{}, {404: "Inactive"}], ids=["empty", "inactive"])
def test_team_stats_without_active_players(
    monkeypatch: pytest.MonkeyPatch, roster: dict[int, str]
) -> None:
    get = Mock(return_value={"people": [{"id": 404}]})
    monkeypatch.setattr(statsapi, "get", get)

    rows, active, inactive = extraction_script.get_team_stats(roster)

    assert rows.empty
    assert active == {}
    assert inactive == roster
    assert get.call_count == len(roster)


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
        extraction_script.get_team_stats({101: "Complete", 102: "Incomplete"})


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


def test_threaded_league_matches_serial_with_out_of_order_teams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = Mock()
    monkeypatch.setattr(extraction_script, "logger", logger)

    def response(endpoint: str, params: dict) -> dict:
        player_id = params["personId"]
        if player_id == 404:
            return {"people": [{}]}
        group = "pitching" if player_id == 202 else "hitting"
        names = PITCHING_STATS if group == "pitching" else BATTING_STATS
        return {
            "people": [
                {
                    "stats": [
                        {
                            "group": {"displayName": group},
                            "splits": [{"stat": dict.fromkeys(names, player_id)}],
                        }
                    ]
                }
            ]
        }

    monkeypatch.setattr(statsapi, "get", response)
    extractor = extraction_script.DataExtractor()
    extractor.league_team_roster_players = {
        147: {101: "Same Name", 202: "Pitcher", 404: "Inactive"},
        111: {303: "Same Name"},
        133: {},
        119: {404: "Inactive"},
    }
    extractor.team_id_name_mapping = dict.fromkeys(
        extractor.league_team_roster_players, "Team"
    )
    expected = pd.concat(
        extractor.get_player_stats_dataframe_per_team(team)[0]
        for team in extractor.league_team_roster_players
    )
    expected["date"] = extraction_script.DATE_TIME_EXECUTION

    second_finished = Event()
    get_team = extractor.get_player_stats_dataframe_per_team

    def reordered_team(team: int) -> tuple[pd.DataFrame, dict[int, str]]:
        if team == 147:
            assert second_finished.wait(10)
        result = get_team(team)
        if team == 111:
            second_finished.set()
        return result

    monkeypatch.setattr(
        extractor, "get_player_stats_dataframe_per_team", reordered_team
    )
    actual, actual_inactive = extractor.get_player_stats_per_league()

    pd.testing.assert_frame_equal(actual, expected)
    assert actual_inactive == {147: {404: "Inactive"}, 119: {404: "Inactive"}}
    assert list(actual_inactive) == [147, 119]
    logger.error.assert_not_called()


def test_team_collection_overlaps_at_most_four_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extractor = extraction_script.DataExtractor()
    extractor.league_team_roster_players = {team: {} for team in range(8)}
    extractor.team_id_name_mapping = dict.fromkeys(range(8), "Team")
    lock, release, four_started = Lock(), Event(), Event()
    active = maximum = 0

    def collect(team: int) -> tuple[pd.DataFrame, dict[int, str]]:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
            if active == 4:
                four_started.set()
        try:
            assert release.wait(10)
            return pd.DataFrame({"team_id": [team]}), {}
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(extractor, "get_player_stats_dataframe_per_team", collect)
    with ThreadPoolExecutor(max_workers=1) as caller:
        result = caller.submit(extractor.get_player_stats_per_league)
        try:
            assert four_started.wait(10)
            with lock:
                assert active == maximum == 4
            assert not result.done()
        finally:
            release.set()
        rows, inactive = result.result(timeout=10)

    assert maximum == 4
    assert rows["team_id"].tolist() == list(range(8))
    assert inactive == {}


@pytest.mark.parametrize("late_failure", [False, True])
def test_worker_failures_are_logged_after_shutdown(
    monkeypatch: pytest.MonkeyPatch, late_failure: bool
) -> None:
    extractor = extraction_script.DataExtractor(league_name="american_league")
    extractor.team_id_name_mapping = {147: "Yankees", 111: "Red Sox", 119: "Dodgers"}
    extractor.league_team_roster_players = dict.fromkeys(
        extractor.team_id_name_mapping, {}
    )
    futures: list[Future] = [Future() for _ in range(3)]
    errors: dict[int, Exception] = {147: ConnectionError("Connection lost")}
    futures[0].set_exception(errors[147])
    futures[1].set_running_or_notify_cancel()

    def shutdown(*, wait: bool, cancel_futures: bool) -> None:
        assert wait and cancel_futures
        futures[2].cancel()
        if late_failure:
            errors[111] = ValueError("Missing expected player stats")
            futures[1].set_exception(errors[111])
        else:
            futures[1].set_result((pd.DataFrame(), {}))

    executor = Mock(
        submit=Mock(side_effect=futures), shutdown=Mock(side_effect=shutdown)
    )
    monkeypatch.setattr(
        extraction_script, "ThreadPoolExecutor", Mock(return_value=executor)
    )
    logger = Mock()
    monkeypatch.setattr(extraction_script, "logger", logger)

    with pytest.raises(ConnectionError) as raised:
        extractor.get_player_stats_per_league()

    assert raised.value is errors[147]
    assert logger.error.call_count == len(errors)
    assert logger.error.call_args_list[0].kwargs["exc_info"][2] is not None
    for logged, (team, error) in zip(logger.error.call_args_list, errors.items()):
        assert logged.args == ("team_extraction_failed",)
        assert logged.kwargs == {
            "league": "american_league",
            "team_name": extractor.team_id_name_mapping[team],
            "team_number": team,
            "error_type": type(error).__name__,
            "error": str(error),
            "exc_info": (type(error), error, ANY),
        }
