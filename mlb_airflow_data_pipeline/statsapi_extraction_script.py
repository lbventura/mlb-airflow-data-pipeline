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
    ensure_dataframe_columns,
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


def _get_team_roster_players(team_id: int) -> dict[int, str]:
    response = statsapi.get(
        "team_roster",
        {"teamId": team_id, "season": SEASON_YEAR, "rosterType": "active"},
    )
    try:
        roster = response["roster"]
    except (KeyError, TypeError) as error:
        error.add_note(f"Malformed roster for team {team_id}")
        raise
    players: dict[int, str] = {}
    for entry in roster:
        try:
            player = entry["person"]
            player_id = player["id"]
            full_name = player["fullName"]
        except (KeyError, TypeError) as error:
            error.add_note(f"Malformed roster entry for team {team_id}")
            raise
        if player_id in players:
            raise ValueError(
                f"Duplicate player ID {player_id} in team {team_id} roster"
            )
        players[player_id] = full_name
    return players


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
    def __init__(self, roster_players: dict[int, str]):
        self.roster_players = roster_players
        self.team_stats: dict[int, list[str]] = {}

    def get_team_stats(
        self,
    ) -> tuple[pd.DataFrame, dict[int, str], dict[int, str]]:
        active_players: dict[int, str] = {}
        inactive_players: dict[int, str] = {}

        for player_id, name in self.roster_players.items():
            try:
                self.team_stats[player_id] = statsapi.player_stats(
                    player_id, type=_get_stats_type()
                ).split("\n")
                active_players[player_id] = name
            except TypeError:
                inactive_players[player_id] = name

        team_player_stats = self._get_team_player_stats()

        return team_player_stats, active_players, inactive_players

    def _get_team_player_stats(self) -> pd.DataFrame:
        team_player_stats = pd.DataFrame(
            data={
                player_id: _generate_player_stats(player_stats_str)
                for player_id, player_stats_str in self.team_stats.items()
            }
        ).T
        return team_player_stats


class DataExtractor:
    def __init__(self, league_name: str = LEAGUE_NAME) -> None:
        self.league_name = league_name
        self.team_id_name_mapping: dict[int, str] = {}
        self.league_standings: pd.DataFrame = pd.DataFrame()
        self.league_team_roster_players: dict[int, dict[int, str]] = {}

    def get_player_stats_per_league(
        self,
    ) -> tuple[pd.DataFrame, dict[int, dict[int, str]], list[str]]:
        """
        Returns player individual stats per league.

        Returns:
            pd.DataFrame: Containing stats for a given league
            dict: Keys are team IDs and values are inactive players by ID
            list: List of teams for which we failed to get stats
        """
        league_player_team_stats = {}
        inactive_players_per_team = {}
        failed_teams = []

        for team_number in self.league_team_roster_players:
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
        player_stats["date"] = DATE_TIME_EXECUTION

        assert sorted(player_stats.columns.to_list()) == expected_output_columns()

        return player_stats, inactive_players_per_team, failed_teams

    def get_player_stats_dataframe_per_team(
        self,
        team_number: int,
    ) -> tuple[pd.DataFrame, dict[int, str]]:
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

        roster_players = self.league_team_roster_players[team_number]
        player_information_per_team = TeamStats(roster_players)

        (
            team_player_stats,
            active_players,
            inactive_player_info,
        ) = player_information_per_team.get_team_stats()
        team_player_stats["playername"] = list(active_players.values())
        team_player_stats["team_id"] = team_number

        return (
            _insert_col_in_first_position(team_player_stats),
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
        standings = statsapi.standings_data(league_number, season=SEASON_YEAR)

        for division in LEAGUE_DIVISION_MAPPING[league_number]:
            division_results: pd.DataFrame = pd.DataFrame(
                standings[division]["teams"]  # type: ignore
            )
            league_list.append(division_results)

        league_standings = pd.concat(league_list, axis=0)
        league_standings["date"] = DATE_TIME_EXECUTION
        self.league_standings = league_standings

    def set_league_team_roster_players(self) -> None:
        """
        Load player IDs and names for each team in the league standings.
        """
        self.set_league_division_standings()
        team_ids = self.league_standings["team_id"].values

        self.league_team_roster_players = {
            int(team_id): _get_team_roster_players(int(team_id)) for team_id in team_ids
        }


if __name__ == "__main__":
    logger.info("extraction_started", league=LEAGUE_NAME, date=DATE_TIME_EXECUTION)

    db_path = get_database_path()
    with create_connection(db_path) as conn:
        data_extractor = DataExtractor(league_name=LEAGUE_NAME)

        data_extractor.set_league_team_roster_players()
        logger.info(
            "league_standings_loaded",
            standings_shape=data_extractor.league_standings.shape,
        )

        ensure_dataframe_columns(
            conn, "league_standings", data_extractor.league_standings
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

        player_stats_for_run = league_player_team_stats_df.assign(
            league_name=LEAGUE_NAME
        )
        ensure_dataframe_columns(conn, "player_stats", player_stats_for_run)
        insert_dataframe(conn, "player_stats", player_stats_for_run)

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
