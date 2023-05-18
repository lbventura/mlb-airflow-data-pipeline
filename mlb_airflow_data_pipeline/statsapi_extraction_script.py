import logging
import pandas as pd

from datetime import datetime

import statsapi

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    IS_SEASON_STATS,
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    LEAGUE_NAME,
    DATE_TIME_EXECUTION,
    SEASON_YEAR,
)

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

LEAGUE_STANDINGS_FILE_NAME = (
    f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}_league_standings_df.csv"
)

PLAYER_DATA_FILE_NAME = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}_full_player_stats_df.csv"

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


def _extract_player_name(player: str) -> str:
    player_data: list = player.split(" ")
    return " ".join(player_data[-2:])


def _generate_player_stats(player_stats_str: list[str]) -> dict:
    player_stats = {
        stat.split(": ")[0]: stat.split(": ")[1]
        for stat in player_stats_str
        if ":" in stat
    }
    return player_stats


class TeamStats:
    def __init__(self, player_names_per_team: list[str]):
        self.player_names_per_team = player_names_per_team
        self.team_stats: dict = {}

    def get_team_stats(self) -> tuple[pd.DataFrame, dict, dict]:

        active_player_name_ids = {}
        inactive_player_info = {}

        self._set_player_name_ids()

        for name, id in self.player_name_ids.items():
            try:
                self.team_stats[name] = statsapi.player_stats(
                    id, type=self.get_stats_type()
                ).split("\n")
                active_player_name_ids[name] = id
            except TypeError:
                inactive_player_info[name] = id

        team_player_stats = self._get_team_player_stats()

        return team_player_stats, active_player_name_ids, inactive_player_info

    def _set_player_name_ids(self) -> None:
        player_name_ids = {
            player_name: statsapi.lookup_player(player_name, season=SEASON_YEAR)[0][
                "id"
            ]
            for player_name in self.player_names_per_team
        }
        self.player_name_ids = player_name_ids

    def _get_team_player_stats(self) -> pd.DataFrame:
        team_player_stats = pd.DataFrame(
            data={
                self.player_name_ids[player]: _generate_player_stats(player_stats_str)
                for player, player_stats_str in self.team_stats.items()
            }
        ).T
        return team_player_stats

    def get_stats_type(self, is_season_stats: bool = IS_SEASON_STATS) -> str:
        if is_season_stats:
            stats_type = "season"
        else:
            stats_type = "career"
        return stats_type


class DataExtractor:
    def __init__(self, league_name: str = LEAGUE_NAME) -> None:
        self.league_name = league_name

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

    def set_team_ids_and_names(self) -> None:
        """
        Creates a dictionary where the keys are the team_ids and values are the team names.
        """
        team_ids_names_df = self.league_standings[["team_id", "name"]]
        team_ids_names_df.set_index("team_id")
        team_ids_names = team_ids_names_df.to_dict(orient="records")

        self.team_id_name_mapping: dict[int, str] = {
            record["team_id"]: record["name"] for record in team_ids_names
        }

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
        self.league_standings = league_standings

    def set_league_team_rosters_player_names(self) -> None:
        """
        Uses set_league_division_standings to generate the player names for each team roster.
        """
        self.set_league_division_standings()
        team_ids = self.league_standings["team_id"].values

        league_team_rosters = {
            team_id: statsapi.roster(team_id, season=SEASON_YEAR).split("\n")
            for team_id in team_ids
        }

        league_team_rosters_player_names = {
            team_id: [
                _extract_player_name(player) for player in league_team_rosters[team_id]
            ]
            for team_id in team_ids
        }

        self.league_team_rosters_player_names: dict[
            int, list[str]
        ] = league_team_rosters_player_names  # type: ignore


if __name__ == "__main__":

    logging.info("Data extraction started")

    data_extractor = DataExtractor(league_name=LEAGUE_NAME)

    data_extractor.set_league_team_rosters_player_names()

    (data_extractor.league_standings).to_csv(
        DATA_FILE_LOCATION + LEAGUE_STANDINGS_FILE_NAME
    )

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
