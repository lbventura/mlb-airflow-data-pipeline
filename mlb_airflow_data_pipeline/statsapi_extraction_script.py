from datetime import datetime

import pandas as pd
import statsapi

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    IS_SEASON_STATS,
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    LEAGUE_NAME,
    SEASON_YEAR,
    expected_output_columns,
)
from mlb_airflow_data_pipeline.logging_setup import get_logger
from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    get_database_path,
    insert_dataframe,
)

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"

LEAGUE_STANDINGS_FILE_NAME = f"{OUTPUT_DETAILS}_league_standings_df.csv"

PLAYER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_full_player_stats_df.csv"

# Initialize structured logger
logger = get_logger("statsapi_extraction", league=LEAGUE_NAME)


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


def _get_stats_type(is_season_stats: bool = IS_SEASON_STATS) -> str:
    if is_season_stats:
        stats_type = "season"
    else:
        stats_type = "career"
    return stats_type


class TeamStats:
    def __init__(self, player_names_per_team: list[str]):
        self.player_names_per_team = player_names_per_team
        self.team_stats: dict = {}

    def get_team_stats(self) -> tuple[pd.DataFrame, dict, dict]:
        active_player_name_ids = {}
        inactive_player_info = {}

        self._set_player_name_ids()

        for name, player_id in self.player_name_ids.items():
            try:
                self.team_stats[name] = statsapi.player_stats(
                    player_id, type=_get_stats_type()
                ).split("\n")
                active_player_name_ids[name] = player_id
            except TypeError:
                inactive_player_info[name] = player_id

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


class DataExtractor:
    def __init__(self, league_name: str = LEAGUE_NAME) -> None:
        self.league_name = league_name
        self.team_id_name_mapping: dict[int, str] = {}
        self.league_standings: pd.DataFrame = pd.DataFrame()
        self.league_team_rosters_player_names: dict[int, list[str]] = {}

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
                logger.info(
                    "team_extraction_success",
                    team_name=successful_team_name,
                    team_number=team_number,
                    players_count=len(team_player_stats),
                )
            except Exception as e:
                failed_team_name = self.team_id_name_mapping[team_number]
                failed_teams.append(failed_team_name)
                logger.error(
                    "team_extraction_failed",
                    team_name=failed_team_name,
                    team_number=team_number,
                    error=str(e),
                    exc_info=True,
                )
        player_stats = pd.concat(league_player_team_stats.values())

        assert sorted(player_stats.columns.to_list()) == expected_output_columns()

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
        corrected_team_player_stats["team_id"] = team_number

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

        self.team_id_name_mapping = {
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
                statsapi.standings_data(league_number, season=SEASON_YEAR)[division][
                    "teams"
                ]  # type: ignore
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

        self.league_team_rosters_player_names = league_team_rosters_player_names  # type: ignore


if __name__ == "__main__":
    logger.info("extraction_started", league=LEAGUE_NAME, date=DATE_TIME_EXECUTION)

    db_path = get_database_path()
    conn = create_connection(db_path)

    data_extractor = DataExtractor(league_name=LEAGUE_NAME)

    data_extractor.set_league_team_rosters_player_names()
    logger.info(
        "league_standings_loaded", standings_shape=data_extractor.league_standings.shape
    )

    insert_dataframe(conn, "league_standings", data_extractor.league_standings)
    logger.info(
        "league_standings_saved", database_path=db_path, table="league_standings"
    )

    data_extractor.set_team_ids_and_names()
    logger.info(
        "team_mapping_created", teams_count=len(data_extractor.team_id_name_mapping)
    )

    (
        league_player_team_stats_df,
        inactive_players_per_team,
        failed_teams,
    ) = data_extractor.get_player_stats_per_league()

    insert_dataframe(conn, "player_stats", league_player_team_stats_df)

    logger.info(
        "extraction_completed",
        players_total=len(league_player_team_stats_df),
        inactive_players_count=sum(
            len(players) for players in inactive_players_per_team.values()
        ),
        failed_teams_count=len(failed_teams),
        database_path=db_path,
        table="player_stats",
    )

    if inactive_players_per_team:
        logger.warning(
            "inactive_players_found", inactive_players=inactive_players_per_team
        )

    if failed_teams:
        logger.error("teams_extraction_failed", failed_teams=failed_teams)

    conn.close()
