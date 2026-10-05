"""Fixed roster responses establish player identity and failure boundaries."""

import pandas as pd
import pytest
import statsapi
from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    DataExtractor,
    TeamStats,
    _generate_player_stats,
    _get_team_roster_players,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import DATE_TIME_EXECUTION


def test_roster_ids_preserve_full_and_duplicate_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entries = [
        {"person": {"id": 677651, "fullName": "Luis Garcia"}},
        {"person": {"id": 671277, "fullName": "Luis Garcia"}},
        {"person": {"id": 2, "fullName": "Tommy La Stella"}},
        {"person": {"id": 3, "fullName": "John Smith Jr."}},
    ]
    calls = []

    def api(endpoint: str, params: dict) -> dict:
        calls.append((endpoint, params))
        return {"roster": entries}

    monkeypatch.setattr(statsapi, "get", api)
    assert _get_team_roster_players(147) == {
        677651: "Luis Garcia",
        671277: "Luis Garcia",
        2: "Tommy La Stella",
        3: "John Smith Jr.",
    }
    assert calls == [
        ("team_roster", {"teamId": 147, "season": 2023, "rosterType": "active"})
    ]


def test_duplicate_roster_id_aborts_setup_with_team_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        statsapi,
        "get",
        lambda endpoint, params: {
            "roster": [
                {"person": {"id": 123, "fullName": "First Name"}},
                {"person": {"id": 123, "fullName": "Second Name"}},
            ]
        },
    )
    with pytest.raises(ValueError, match="Duplicate player ID 123 in team 147"):
        _get_team_roster_players(147)


def test_roster_missing_name_keeps_original_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        statsapi,
        "get",
        lambda endpoint, params: {"roster": [{"person": {"id": 1}}]},
    )
    with pytest.raises(KeyError, match="fullName") as caught:
        _get_team_roster_players(147)
    assert caught.value.__notes__ == ["Malformed roster entry for team 147"]


def test_roster_missing_entries_keeps_original_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(statsapi, "get", lambda endpoint, params: {})
    with pytest.raises(KeyError, match="roster") as caught:
        _get_team_roster_players(147)
    assert caught.value.__notes__ == ["Malformed roster for team 147"]


def test_raw_roster_has_no_formatted_blank_player(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def api(endpoint: str, params: dict) -> dict:
        calls.append(endpoint)
        assert endpoint == "team_roster"
        return {"roster": [{"person": {"id": 123, "fullName": "Sample Player"}}]}

    def player_stats(player_id: int, **kwargs: str) -> str:
        calls.append("person")
        assert player_id == 123
        return "age: 30"

    monkeypatch.setattr(statsapi, "get", api)
    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    frame, active, inactive = TeamStats(_get_team_roster_players(147)).get_team_stats()
    assert frame.index.tolist() == [123]
    assert active == {123: "Sample Player"}
    assert inactive == {}
    assert calls == ["team_roster", "person"]


def test_team_dataframe_uses_authoritative_ids_without_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def player_stats(player_id: int, **kwargs: str) -> str:
        calls.append((player_id, kwargs))
        if player_id == 3:
            raise TypeError("fixture has no stats")
        return f"age: {player_id}\nSeason Hitting\ngamesPlayed: 40"

    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    monkeypatch.setattr(
        statsapi,
        "lookup_player",
        lambda *args, **kwargs: pytest.fail("Name lookup must not run"),
    )
    extractor = DataExtractor()
    extractor.league_team_roster_players = {
        147: {1: "Luis Garcia", 2: "Luis Garcia", 3: "Luis Garcia"}
    }
    frame, inactive = extractor.get_player_stats_dataframe_per_team(147)
    expected = pd.DataFrame(
        {
            "playername": ["Luis Garcia", "Luis Garcia"],
            "age": ["1", "2"],
            "gamesPlayed": ["40", "40"],
            "team_id": [147, 147],
        },
        index=[1, 2],
    )
    pd.testing.assert_frame_equal(frame, expected)
    assert inactive == {3: "Luis Garcia"}
    assert calls == [
        (1, {"type": "season"}),
        (2, {"type": "season"}),
        (3, {"type": "season"}),
    ]


def test_non_type_error_remains_team_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def player_stats(player_id: int, **kwargs: str) -> str:
        if player_id == 1:
            raise ConnectionError("fixture network failure")
        return "age: 30"

    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    monkeypatch.setattr(
        extraction,
        "expected_output_columns",
        lambda: ["age", "date", "playername", "team_id"],
    )
    extractor = DataExtractor()
    extractor.league_team_roster_players = {
        147: {1: "Failed Player"},
        148: {2: "Active Player"},
    }
    extractor.team_id_name_mapping = {147: "Failed Team", 148: "Active Team"}
    frame, inactive, failed = extractor.get_player_stats_per_league()
    assert frame.index.tolist() == [2]
    assert frame["team_id"].tolist() == [148]
    assert inactive == {}
    assert failed == ["Failed Team"]


def test_repeated_stat_fields_use_last_value_in_response_order() -> None:
    lines = ["Season Hitting", "gamesPlayed: 40", "Season Fielding", "gamesPlayed: 35"]
    assert _generate_player_stats(lines) == {"gamesPlayed": "35"}
    assert _generate_player_stats(list(reversed(lines))) == {"gamesPlayed": "40"}


def test_standings_reuses_one_response_for_fifteen_teams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def standings_data(league_id: int, **kwargs: int) -> dict:
        calls.append((league_id, kwargs))
        return {
            division: {
                "teams": [
                    {
                        "team_id": division * 10 + index,
                        "name": f"Team {division}-{index}",
                    }
                    for index in range(5)
                ]
            }
            for division in (200, 201, 202)
        }

    monkeypatch.setattr(statsapi, "standings_data", standings_data)
    extractor = DataExtractor("american_league")
    extractor.set_league_division_standings()
    assert calls == [(103, {"season": 2023})]
    assert len(extractor.league_standings) == 15
    assert extractor.league_standings["date"].eq(DATE_TIME_EXECUTION).all()
    assert extractor.league_standings["team_id"].tolist() == [
        division * 10 + index for division in (200, 201, 202) for index in range(5)
    ]
