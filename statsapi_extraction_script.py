import json
import pandas as pd
import statsapi
import logging

from typing import Any, Dict

from statsapi_parameters_script import (
    DATE_TIME_EXECUTION,
    DATA_FILE_LOCATION,
    LEAGUE_MAPPING,
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_NAME,
    PLAYER_DATA_FILE_NAME,
    LEAGUE_STANDINGS_FILE_NAME,
)

# creates a logger
logging.basicConfig(
    filename="mlb-airflow-debugger.log",
    format="%(asctime)s: %(levelname)s: %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    filemode="w",
)


def get_league_division_standings() -> tuple[dict, dict]:
    """
    Creates the league and division standings for one of the two leagues in MLB.

    Returns:
    dict: League standings
    dict: Division standings
    """
    league_standings_dict = {}
    division_standings_dict: Dict[Any, Any] = {}

    for league in LEAGUE_DIVISION_MAPPING.keys():

        league_list = []
        division_standings_dict[LEAGUE_MAPPING[league]] = {}

        for division in LEAGUE_DIVISION_MAPPING[league]:
            division_results: pd.DataFrame = pd.DataFrame(
                statsapi.standings_data(league)[division]["teams"]  # type: ignore
            )
            league_list.append(division_results)
            division_standings_dict[LEAGUE_MAPPING[league]][division] = division_results
        league_standings_dict[LEAGUE_MAPPING[league]] = pd.concat(league_list, axis=0)

    return league_standings_dict, division_standings_dict


def get_league_team_rosters_player_names(
    league_name: str = LEAGUE_NAME,
) -> tuple[dict, dict, dict]:
    """
    Uses get_league_division_standings this to generate the player names for each team roster.
    Args:
        team_number (int): MLB team number

    Returns:
        pd.DataFrame: pandas DataFrame containing containing the
        players names for each team roster
    """
    league_standings_dict, division_standings_dict = get_league_division_standings()

    league_standings = league_standings_dict[league_name]
    league_team_info = {team: statsapi.lookup_team(team) for team in league_standings}

    league_team_rosters = {
        team_id: statsapi.roster(team_id).split("\n")
        for team_id in league_standings["team_id"].values
    }

    league_team_rosters_player_names = {
        team_id: [
            " ".join(player.split(" ")[-2:]) for player in league_team_rosters[team_id]
        ]
        for team_id in league_standings["team_id"].values
    }

    return (
        league_standings_dict,
        division_standings_dict,
        league_team_rosters_player_names,
    )


(
    league_standings_dict,
    division_standings_dict,
    league_team_rosters_player_names,
) = get_league_team_rosters_player_names(LEAGUE_NAME)

league_standings_dict[LEAGUE_NAME].to_csv(
    DATA_FILE_LOCATION + LEAGUE_STANDINGS_FILE_NAME
)


def get_player_stats_dataframe_per_team(
    team_number: int,
) -> tuple[pd.DataFrame, dict]:
    """Takes as input a team number and returns a pandas DataFrame
    containing the stats of the active players,
    and a dictionary with inactive player information.

    This methodology has a big problem: if players change teams, their
    stats from the previous team will not be considered.

    Args:
        team_number (int): MLB team number

    Returns:
        pd.DataFrame: pandas DataFrame containing containing the
        stats of the active players for a particular team
        dict: Dictionary with inactive player information
    """

    team_name_ids = {
        player_name: statsapi.lookup_player(player_name)[0]["id"]
        for player_name in league_team_rosters_player_names[team_number]
    }

    team_stats_json = {}
    corrected_team_name_ids = {}
    inactive_player_dict = (
        {}
    )  # this is a dict of playername, playerid for inactive players

    for name, id in team_name_ids.items():
        try:
            team_stats_json[name] = statsapi.player_stats(id).split("\n")
            corrected_team_name_ids[name] = id
        except TypeError:
            inactive_player_dict[name] = id

    team_player_stats = pd.DataFrame(
        {
            team_name_ids[player]: {
                stat.split(": ")[0]: stat.split(": ")[1]
                for stat in team_stats_json[player]
                if ":" in stat
            }
            for player in team_stats_json.keys()
        }
    ).T

    if len(corrected_team_name_ids.keys()) != len(team_player_stats):
        team_player_stats["playername"] = list(corrected_team_name_ids.keys())[:-1]
    else:
        team_player_stats["playername"] = corrected_team_name_ids.keys()

    col = team_player_stats.pop("playername")
    team_player_stats.insert(0, col.name, col)

    return team_player_stats, inactive_player_dict


def get_player_stats_per_league() -> tuple[pd.DataFrame, dict, list]:
    """
    Returns player individual stats per league.

    Returns:
        pd.DataFrame: Containing stats for a given league
        dict: Keys are team names and values are inactive players
        list: List of teams for which we failed to get stats
    """
    league_player_team_stats_dict = {}
    inactive_players_per_team = {}
    failed_teams_list = []

    for team_number in league_team_rosters_player_names.keys():
        try:
            team_info = get_player_stats_dataframe_per_team(team_number)
            league_player_team_stats_dict[team_number] = team_info[0]
            if len(team_info[1]) != 0:
                inactive_players_per_team[int(team_number)] = team_info[1]
        except:
            failed_teams_list.append(team_number)
            logging.exception(
                f"Oops! Something went wrong for the team_number {team_number}"
            )
            continue

    return (
        pd.concat(league_player_team_stats_dict.values()),
        inactive_players_per_team,
        failed_teams_list,
    )


if __name__ == "__main__":

    logging.info("Data extraction started")

    (
        league_player_team_stats_df,
        inactive_players_per_team,
        failed_teams_list,
    ) = get_player_stats_per_league()

    # save full player stats
    league_player_team_stats_df.to_csv(DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME)

    # save inactive players and failed teams json
    if len(inactive_players_per_team) != 0:
        with open(
            DATA_FILE_LOCATION
            + f"{DATE_TIME_EXECUTION}_{LEAGUE_NAME}_inactive_players.json",
            "w",
        ) as fp:
            inactive_players_per_team_json = json.dumps(inactive_players_per_team)
            json.dump(inactive_players_per_team, fp)

    if len(failed_teams_list) != 0:
        with open(
            DATA_FILE_LOCATION
            + f"{DATE_TIME_EXECUTION}_{LEAGUE_NAME}_failed_teams.json",
            "w",
        ) as fp:
            failed_teams_list_json = json.dumps(failed_teams_list)
            json.dump(failed_teams_list_json, fp)

    logging.info("Data extraction finished")
