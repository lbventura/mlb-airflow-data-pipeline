import logging
import pandas as pd

import pandas as pd
import statsapi

from statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    IS_SEASON_STATS,
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    LEAGUE_NAME,
    LEAGUE_STANDINGS_FILE_NAME,
    PLAYER_DATA_FILE_NAME,
    SEASON_YEAR,
)

# creates a logger
logging.basicConfig(
    filename="mlb-airflow-debugger.log",
    format="%(asctime)s: %(levelname)s: %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    filemode="w",
)


def _insert_col_in_first_position(
    dataframe: pd.DataFrame, column_name: str = "playername"
) -> pd.DataFrame:
    col = dataframe.pop(column_name)
    dataframe.insert(0, col.name, col)
    return dataframe


class TeamStats:
    def __init__(self, player_names_per_team: list[str]):
        self.player_names_per_team = player_names_per_team

    def get_stats_type(self, is_season_stats: bool = IS_SEASON_STATS) -> str:
        if is_season_stats:
            stats_type = "season"
        else:
            stats_type = "career"
        return stats_type

    def _set_player_name_ids(self) -> None:
        player_name_ids = {
            player_name: statsapi.lookup_player(player_name, season=SEASON_YEAR)[0][
                "id"
            ]
            for player_name in self.player_names_per_team
        }
        self.player_name_ids = player_name_ids

    def get_team_stats(self) -> tuple[pd.DataFrame, dict, dict]:

        team_stats_json = {}
        active_player_name_ids = {}
        inactive_player_info = {}

        self._set_player_name_ids()

        for name, id in self.player_name_ids.items():
            try:
                team_stats_json[name] = statsapi.player_stats(
                    id, type=self.get_stats_type()
                ).split("\n")
                active_player_name_ids[name] = id
            except TypeError:
                inactive_player_info[name] = id

        team_player_stats = pd.DataFrame(
            data={
                self.player_name_ids[player]: {
                    stat.split(": ")[0]: stat.split(": ")[1]
                    for stat in team_stats_json[player]
                    if ":" in stat
                }
                for player in team_stats_json.keys()
            }
        ).T

        return team_player_stats, active_player_name_ids, inactive_player_info


class DataExtractor:
    def __init__(self, league_name: str = LEAGUE_NAME) -> None:
        self.league_name = league_name

    def set_league_division_standings(self) -> None:
        """
        Creates the league and division standings for one of the two leagues in MLB.
        """

        league_number = LEAGUE_MAPPING[self.league_name]
        league_list = []

        for division in LEAGUE_DIVISION_MAPPING[league_number]:
            division_results: pd.DataFrame = pd.DataFrame(
                statsapi.standings_data(league_number, season=SEASON_YEAR)[division]["teams"]  # type: ignore
            )
            league_list.append(division_results)

        league_standings = pd.concat(league_list, axis=0)

        (league_standings).to_csv(DATA_FILE_LOCATION + LEAGUE_STANDINGS_FILE_NAME)

        self.league_standings = league_standings

    def set_team_ids_and_names(self) -> None:
        """
        Creates a dictionary where the keys are the team_ids and values are the team names.
        """
        team_ids_names_df = self.league_standings[["team_id", "name"]]
        team_ids_names_df.set_index("team_id")
        team_ids_names = team_ids_names_df.to_dict(orient="records")

        self.team_id_name_mapping = {
            record["team_id"]: record["name"] for record in team_ids_names
        }

    def set_league_team_rosters_player_names(self) -> None:
        """
        Uses _set_league_division_standings to generate the player names for each team roster.
        Args:
            team_number (int): MLB team number

        Returns:
            dict: Dictionary where keys are team_ids and the values are player names of each roster
        """
        self.set_league_division_standings()
        team_ids = self.league_standings["team_id"].values

        league_team_rosters = {
            team_id: statsapi.roster(team_id, season=SEASON_YEAR).split("\n")
            for team_id in team_ids
        }

        league_team_rosters_player_names = {
            team_id: [
                " ".join(player.split(" ")[-2:])
                for player in league_team_rosters[team_id]
            ]
            for team_id in team_ids
        }

        self.league_team_rosters_player_names: dict[
            int, list[str]
        ] = league_team_rosters_player_names  # type: ignore

    def get_player_stats_dataframe_per_team(
        self,
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

        player_names_per_team = self.league_team_rosters_player_names[team_number]

        player_information_per_team = TeamStats(
            player_names_per_team=player_names_per_team
        )

        (
            team_player_stats,
            active_player_name_ids,
            inactive_player_info,
        ) = player_information_per_team.get_team_stats()

        corrected_player_name_ids: dict[str, int] = {
            name: id for name, id in active_player_name_ids.items() if len(name) >= 2
        }

        corrected_player_names = [ele for ele in corrected_player_name_ids.keys()]
        corrected_player_ids = [ele for ele in corrected_player_name_ids.values()]

        corrected_team_player_stats = team_player_stats.loc[corrected_player_ids]
        corrected_team_player_stats["playername"] = corrected_player_names

        return (
            _insert_col_in_first_position(corrected_team_player_stats),
            inactive_player_info,
        )

    def get_player_stats_per_league(
        self,
    ) -> tuple[pd.DataFrame, dict, list]:
        """
        Returns player individual stats per league.

        Returns:
            pd.DataFrame: Containing stats for a given league
            dict: Keys are team names and values are inactive players
            list: List of teams for which we failed to get stats
        """
        league_player_team_stats = {}
        inactive_players_per_team = {}
        failed_teams = []

        for team_number in self.league_team_rosters_player_names.keys():
            try:
                (
                    team_player_stats,
                    inactive_player_info,
                ) = self.get_player_stats_dataframe_per_team(team_number)
                league_player_team_stats[team_number] = team_player_stats
                if inactive_player_info:
                    inactive_players_per_team[team_number] = inactive_player_info
                successful_team_name = self.team_id_name_mapping[team_number]
                logging.info(
                    f"Extraction succeeded for the team {successful_team_name}, team number {team_number}"
                )
            except:
                failed_team_name = self.team_id_name_mapping[team_number]
                failed_teams.append(failed_team_name)
                logging.exception(
                    f"Extraction succeeded for the team {failed_team_name}, team number {team_number}"
                )
        player_stats = pd.concat(league_player_team_stats.values())
        return player_stats, inactive_players_per_team, failed_teams


if __name__ == "__main__":

    logging.info("Data extraction started")

    data_extractor = DataExtractor(league_name=LEAGUE_NAME)

    data_extractor.set_league_team_rosters_player_names()
    data_extractor.set_team_ids_and_names()

    (
        league_player_team_stats_df,
        inactive_players_per_team,
        failed_teams,
    ) = data_extractor.get_player_stats_per_league()

    league_player_team_stats_df.to_csv(DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME)

    logging.info("Full player stats were saved")
    logging.info(f"The inactive players per team are {inactive_players_per_team}")
    logging.info(f"The failed teams are {failed_teams}")

    logging.info("Data extraction finished")
