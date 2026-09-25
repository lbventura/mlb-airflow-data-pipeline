"""Characterize current identity handling with synthetic, fixed API responses."""

from typing import Any

import pytest
import statsapi

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    TeamStats,
    _extract_player_name,
    _generate_player_stats,
)


@pytest.mark.parametrize(
    "name,roster_id,first_match_id",
    [
        ("Luis Garcia", 677651, 671277),
        ("Will Smith", 519293, 669257),
        ("Diego Castillo", 650895, 660636),
        ("José Rodríguez", 642578, 679563),
        ("Carlos Pérez", 656024, 542208),
    ],
)
def test_same_name_lookup_uses_first_match(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    roster_id: int,
    first_match_id: int,
) -> None:
    def api(endpoint: str, params: dict) -> dict:
        assert endpoint == "sports_players"
        return {
            "people": [
                {"id": first_match_id, "fullName": name},
                {"id": roster_id, "fullName": name},
            ]
        }

    monkeypatch.setattr(statsapi, "get", api)
    team = TeamStats([name])
    team._set_player_name_ids()
    assert team.player_name_ids == {name: first_match_id}
    assert team.player_name_ids[name] != roster_id


def test_multipart_name_loses_disambiguating_first_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def api(endpoint: str, params: dict) -> dict:
        return {
            "people": [
                {"id": 1, "fullName": "Other La Stella"},
                {"id": 2, "fullName": "Tommy La Stella"},
            ]
        }

    monkeypatch.setattr(statsapi, "get", api)
    name = _extract_player_name("#18  2B  Tommy La Stella")
    assert name == "La Stella"
    team = TeamStats([name])
    team._set_player_name_ids()
    assert team.player_name_ids == {"La Stella": 1}
    assert statsapi.lookup_player("Tommy La Stella", season=2023)[0]["id"] == 2


def test_formatted_roster_blank_line_causes_an_extra_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookup_calls = []

    def api(endpoint: str, params: dict) -> dict:
        if endpoint == "team_roster":
            return {
                "roster": [
                    {
                        "jerseyNumber": "1",
                        "position": {"abbreviation": "P"},
                        "person": {"id": 2, "fullName": "Sample Player"},
                    }
                ]
            }
        assert endpoint == "sports_players"
        lookup_calls.append(params)
        return {
            "people": [
                {"id": 1, "fullName": "First Person"},
                {"id": 2, "fullName": "Sample Player"},
            ]
        }

    monkeypatch.setattr(statsapi, "get", api)
    names = [
        _extract_player_name(line)
        for line in statsapi.roster(147, season=2023).split("\n")
    ]
    assert names == ["Sample Player", ""]
    team = TeamStats(names)
    team._set_player_name_ids()
    assert len(lookup_calls) == 2
    assert team.player_name_ids == {"Sample Player": 2, "": 1}


def test_stats_type_error_is_classified_as_inactive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def lookup(name: str, **kwargs: Any) -> list[dict]:
        return [{"id": 1 if name == "Active Player" else 2}]

    def player_stats(player_id: int, **kwargs: Any) -> str:
        if player_id == 2:
            raise TypeError("fixture has no stats")
        return "age: 30"

    monkeypatch.setattr(statsapi, "lookup_player", lookup)
    monkeypatch.setattr(statsapi, "player_stats", player_stats)
    team = TeamStats(["Active Player", "Inactive Player"])
    frame, active, inactive = team.get_team_stats()
    assert frame.index.tolist() == [1]
    assert active == {"Active Player": 1}
    assert inactive == {"Inactive Player": 2}


def test_repeated_stat_fields_use_last_value_in_response_order() -> None:
    lines = ["Season Hitting", "gamesPlayed: 40", "Season Fielding", "gamesPlayed: 35"]
    assert _generate_player_stats(lines) == {"gamesPlayed": "35"}
    assert _generate_player_stats(list(reversed(lines))) == {"gamesPlayed": "40"}
