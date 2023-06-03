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


def american_league_team_id_name() -> dict:
    return {
        140: "Texas Rangers",
        117: "Houston Astros",
        136: "Seattle Mariners",
        108: "Los Angeles Angels",
        133: "Oakland Athletics",
        139: "Tampa Bay Rays",
        110: "Baltimore Orioles",
        147: "New York Yankees",
        141: "Toronto Blue Jays",
        111: "Boston Red Sox",
        142: "Minnesota Twins",
        116: "Detroit Tigers",
        114: "Cleveland Guardians",
        145: "Chicago White Sox",
        118: "Kansas City Royals",
    }


def national_league_team_id_name() -> dict:
    return {
        109: "Arizona Diamondbacks",
        119: "Los Angeles Dodgers",
        137: "San Francisco Giants",
        135: "San Diego Padres",
        115: "Colorado Rockies",
        144: "Atlanta Braves",
        146: "Miami Marlins",
        121: "New York Mets",
        143: "Philadelphia Phillies",
        120: "Washington Nationals",
        158: "Milwaukee Brewers",
        134: "Pittsburgh Pirates",
        113: "Cincinnati Reds",
        112: "Chicago Cubs",
        138: "St. Louis Cardinals",
    }


@pytest.mark.parametrize(
    "league_name,expected_league_team_ids_name",
    [
        ("american_league", american_league_team_id_name()),
        ("national_league", national_league_team_id_name()),
    ],
)
def test_data_extractor_set_team_ids_and_names(
    league_name,
    expected_league_team_ids_name,
):
    data_extractor = DataExtractor(league_name=league_name)

    data_extractor.set_league_team_rosters_player_names()
    data_extractor.set_team_ids_and_names()

    comparing_elements = [
        value == expected_league_team_ids_name[key]
        for key, value in data_extractor.team_id_name_mapping.items()
    ]

    assert all(comparing_elements)
    assert len(comparing_elements) == 15  # there are 15 teams per league in the MLB
