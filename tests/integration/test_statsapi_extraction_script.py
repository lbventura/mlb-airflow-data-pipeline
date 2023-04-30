import statsapi

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    SEASON_YEAR,
    LEAGUE_MAPPING,
    LEAGUE_DIVISION_MAPPING,
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
