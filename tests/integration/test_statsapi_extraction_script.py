from typing import Any

import pytest
import statsapi

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    DataExtractor,
    TeamStats,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    SEASON_YEAR,
    american_league_team_id_name,
    national_league_team_id_name,
)


def test_player_stats(lookup_player_expected_result: list[dict[str, Any]]) -> None:
    example_id: int = lookup_player_expected_result[0]["id"]
    result: list[str] = statsapi.player_stats(example_id, type="season").split("\n")
    assert isinstance(result, list)
    assert isinstance(result[0], str)
    # check that the ":" separator is used
    assert [stat for stat in result if ":" in stat]


def test_lookup_player(lookup_player_expected_result: list[dict[str, Any]]) -> None:
    result: list[dict[str, Any]] = statsapi.lookup_player(
        "Aaron Judge", season=SEASON_YEAR
    )
    assert result == lookup_player_expected_result

    data: dict[str, Any] = result[0]
    expected_data: dict[str, Any] = lookup_player_expected_result[0]
    assert data.keys() == expected_data.keys()
    assert data["id"] == 592450


def test_team_roster() -> None:
    team_id: int = 147  # NYY
    result: list[str] = statsapi.roster(team_id, season=SEASON_YEAR).split("\n")
    assert isinstance(result, list)

    player_identifier: str = [player for player in result if "Rizzo" in player][0]

    assert isinstance(player_identifier, str)
    assert len(player_identifier.split(" ")) == 6
    # TODO: include regex which checks if player_identifier.split(" ")[-2:]
    # is of the form "{first_name} {second_name}"


def test_standings_data() -> None:
    american_league_id: int = LEAGUE_MAPPING["american_league"]
    first_division_american_league: int = min(
        LEAGUE_DIVISION_MAPPING[american_league_id]
    )

    result: list[dict[str, Any]] = statsapi.standings_data(
        american_league_id, season=SEASON_YEAR
    )[  # type: ignore
        first_division_american_league
    ]["teams"]

    assert sorted(result[0].keys()) == [
        "div_rank",
        "elim_num",
        "gb",
        "l",
        "league_rank",
        "name",
        "sport_rank",
        "team_id",
        "w",
        "wc_elim_num",
        "wc_gb",
        "wc_rank",
    ]


def test_team_stats_get_team_stats() -> None:
    # this test requires a manual input of players that are known to be
    # active
    active_mlb_players: dict[str, int] = {
        "Aaron Judge": 592450,
        "Aaron Hicks": 543305,
        "Gerrit Cole": 543037,
    }
    team_stats: TeamStats = TeamStats(
        player_names_per_team=list(active_mlb_players.keys())
    )
    (
        team_player_stats,
        active_player_name_ids,
        inactive_player_info,
    ) = team_stats.get_team_stats()

    assert sorted(list(team_player_stats.index)) == sorted(active_mlb_players.values())
    assert active_player_name_ids
    assert not inactive_player_info


@pytest.mark.parametrize(
    "league_name,expected_league_team_ids_name",
    [
        ("american_league", american_league_team_id_name()),
        ("national_league", national_league_team_id_name()),
    ],
)
def test_data_extractor_set_team_ids_and_names(
    league_name: str,
    expected_league_team_ids_name: dict[int, str],
) -> None:
    data_extractor: DataExtractor = DataExtractor(league_name=league_name)

    data_extractor.set_league_team_rosters_player_names()
    data_extractor.set_team_ids_and_names()

    comparing_elements: list[bool] = [
        value == expected_league_team_ids_name[key]
        for key, value in data_extractor.team_id_name_mapping.items()
    ]

    assert all(comparing_elements)
    assert len(comparing_elements) == 15  # there are 15 teams per league in the MLB
