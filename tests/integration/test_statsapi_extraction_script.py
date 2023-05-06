import statsapi
import pytest

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    SEASON_YEAR,
    LEAGUE_MAPPING,
    LEAGUE_DIVISION_MAPPING,
)

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    TeamStats,
    DataExtractor,
)


def test_player_stats(lookup_player_expected_result):

    example_id = lookup_player_expected_result[0]["id"]
    result = statsapi.player_stats(example_id, type="season").split("\n")
    assert type(result) == list
    assert type(result[0]) == str
    # check that the ":" separator is used
    assert [stat for stat in result if ":" in stat]


def test_lookup_player(lookup_player_expected_result):

    result = statsapi.lookup_player("Aaron Judge", season=SEASON_YEAR)
    assert result == lookup_player_expected_result

    data = result[0]
    expected_data = lookup_player_expected_result[0]
    assert data.keys() == expected_data.keys()
    assert data["id"] == 592450


def test_team_roster():

    team_id = 147  # NYY
    result = statsapi.roster(team_id, season=SEASON_YEAR).split("\n")
    assert type(result) == list

    player_identifier = result[0]
    assert type(player_identifier) == str
    assert len(player_identifier.split(" ")) == 6
    # TODO: include regex which checks if player_identifier.split(" ")[-2:]
    # is of the form "{first_name} {second_name}"


def test_standings_data():

    american_league_id = LEAGUE_MAPPING["american_league"]
    first_division_american_league = min(LEAGUE_DIVISION_MAPPING[american_league_id])

    result = statsapi.standings_data(american_league_id, season=SEASON_YEAR)[  # type: ignore
        first_division_american_league
    ][
        "teams"
    ]

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


def test_team_stats_get_team_stats():
    # this test requires a manual input of players that are known to be
    # active
    active_nyy_players = {
        "Aaron Judge": 592450,
        "Aaron Hicks": 543305,
        "Gerrit Cole": 543037,
    }
    team_stats = TeamStats(player_names_per_team=list(active_nyy_players.keys()))
    (
        team_player_stats,
        active_player_name_ids,
        inactive_player_info,
    ) = team_stats.get_team_stats()

    assert sorted(list(team_player_stats.index)) == sorted(active_nyy_players.values())
    assert active_player_name_ids
    assert not inactive_player_info


def american_league_team_ids() -> list[int]:
    return [108, 110, 111, 114, 116, 117, 118, 133, 136, 139, 140, 141, 142, 145, 147]


def national_league_team_ids() -> list[int]:
    return [109, 112, 113, 115, 119, 120, 121, 134, 135, 137, 138, 143, 144, 146, 158]


@pytest.mark.parametrize(
    "league_name,league_team_ids",
    [
        ("american_league", american_league_team_ids()),
        ("national_league", national_league_team_ids()),
    ],
)
def test_data_extractor_set_league_team_rosters_player_names(
    league_name, league_team_ids
):
    data_extractor = DataExtractor(league_name)

    data_extractor.set_league_team_rosters_player_names()

    result = sorted(list(data_extractor.league_team_rosters_player_names.keys()))  # type: ignore

    assert result == league_team_ids


def american_league_team_names() -> list[str]:
    return [
        "Los Angeles Angels",
        "Baltimore Orioles",
        "Boston Red Sox",
        "Cleveland Guardians",
        "Detroit Tigers",
        "Houston Astros",
        "Kansas City Royals",
        "Oakland Athletics",
        "Seattle Mariners",
        "Tampa Bay Rays",
        "Texas Rangers",
        "Toronto Blue Jays",
        "Minnesota Twins",
        "Chicago White Sox",
        "New York Yankees",
    ]


def test_data_extractor_set_team_ids_and_names():
    data_extractor = DataExtractor(league_name="american_league")

    data_extractor.set_league_team_rosters_player_names()
    data_extractor.set_team_ids_and_names()

    ordered_team_ids = sorted(data_extractor.team_id_name_mapping.keys())
    ordered_team_names = [
        data_extractor.team_id_name_mapping[key] for key in ordered_team_ids
    ]

    expected_team_ids = american_league_team_ids()
    expected_team_names = american_league_team_names()
    assert ordered_team_ids == expected_team_ids
    assert ordered_team_names == expected_team_names
