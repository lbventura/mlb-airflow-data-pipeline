from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, cast

import pandas as pd
import statsapi

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    FIELDING_POSITIONS,
    IS_SEASON_STATS,
    LEAGUE_DIVISION_MAPPING,
    LEAGUE_MAPPING,
    LEAGUE_NAME,
    PITCHING_STATS,
    SEASON_YEAR,
    expected_output_columns,
)
from mlb_airflow_data_pipeline.logging_setup import get_logger
from mlb_airflow_data_pipeline.db_utils import (
    backup_database_before_migration,
    create_connection,
    get_database_path,
    save_extraction_run,
)
from mlb_airflow_data_pipeline.treated_storage import invalidate_treated_run

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

logger = get_logger("statsapi_extraction", league=LEAGUE_NAME)


def _insert_col_in_first_position(
    dataframe: pd.DataFrame, column_name: str = "playername"
) -> pd.DataFrame:
    col = dataframe.pop(column_name)
    dataframe.insert(0, col.name, col)
    return dataframe


def _get_team_roster_players(team_id: int) -> dict[int, str]:
    response = cast(
        dict[str, Any],
        statsapi.get(
            "team_roster",
            {"teamId": team_id, "season": SEASON_YEAR, "rosterType": "active"},
        ),
    )
    try:
        roster = response["roster"]
    except (KeyError, TypeError) as error:
        logger.error(
            "malformed_team_roster", team_id=team_id, error=str(error), exc_info=True
        )
        raise
    players: dict[int, str] = {}
    for entry in roster:
        try:
            player = entry["person"]
            player_id = player["id"]
            full_name = player["fullName"]
        except (KeyError, TypeError) as error:
            logger.error(
                "malformed_roster_entry",
                team_id=team_id,
                error=str(error),
                exc_info=True,
            )
            raise
        if player_id in players:
            raise ValueError(
                f"Duplicate player ID {player_id} in team {team_id} roster"
            )
        players[player_id] = full_name
    return players


def _generate_player_stats(stat_sections: list[dict]) -> dict[str, str]:
    """Keep player totals by group/position, excluding their per-team components."""
    grouped_splits: dict[str, list[dict]] = {}
    for section in stat_sections:
        group = section["group"]["displayName"]
        for split in section["splits"]:
            prefix = group
            if group == "fielding":
                prefix = f"fielding_{split['stat']['position']['abbreviation']}"
            grouped_splits.setdefault(prefix, []).append(split)

    player_stats: dict[str, str] = {}
    for prefix, splits in grouped_splits.items():
        # Traded players have a combined split without a team, plus team splits.
        totals = [split for split in splits if "team" not in split] or splits
        if len(totals) != 1:
            raise ValueError(f"Ambiguous player stats: {prefix} ({len(totals)} splits)")
        player_stats.update(
            (f"{prefix}_{name}", str(value))
            for name, value in totals[0]["stat"].items()
            if name != "position"
        )
    return player_stats


def _validate_player_stats_columns(columns: Iterable[str]) -> None:
    """Validate one player's present groups before combining players' columns."""
    actual = set(columns)
    if unexpected := actual - set(expected_output_columns()):
        raise ValueError(f"Unexpected player stats: {sorted(unexpected)}")
    required_stats = {
        "hitting": BATTING_STATS,
        "pitching": PITCHING_STATS,
        **{f"fielding_{position}": ["gamesPlayed"] for position in FIELDING_POSITIONS},
    }
    for group, stats in required_stats.items():
        if any(column.startswith(f"{group}_") for column in actual):
            missing = {f"{group}_{stat}" for stat in stats} - actual
            if missing:
                raise ValueError(f"Missing expected player stats: {sorted(missing)}")


def _get_stats_type(is_season_stats: bool = IS_SEASON_STATS) -> str:
    if is_season_stats:
        stats_type = "season"
    else:
        stats_type = "career"
    return stats_type


def get_team_stats(
    roster_players: dict[int, str],
) -> tuple[pd.DataFrame, dict[int, str], dict[int, str]]:
    """Return roster players' statistics and active/inactive name mappings."""
    active_players: dict[int, str] = {}
    inactive_players: dict[int, str] = {}
    player_stats: dict[int, dict[str, str]] = {}
    stats_type = _get_stats_type()
    season = f",season={SEASON_YEAR}" if stats_type == "season" else ""

    for player_id, name in roster_players.items():
        response = cast(
            dict[str, Any],
            statsapi.get(
                "person",
                {
                    "personId": player_id,
                    "hydrate": f"stats(group=[hitting,pitching,fielding],type={stats_type}{season},sportId=1),currentTeam",
                },
            ),
        )
        sections = response["people"][0].get("stats", [])
        stats = _generate_player_stats(sections)
        if not stats:
            inactive_players[player_id] = name
            continue
        _validate_player_stats_columns(stats)
        player_stats[player_id] = stats
        active_players[player_id] = name

    team_player_stats = pd.DataFrame.from_dict(player_stats, orient="index")
    return team_player_stats, active_players, inactive_players


class DataExtractor:
    def __init__(self, league_name: str = LEAGUE_NAME) -> None:
        self.league_name = league_name
        self.team_id_name_mapping: dict[int, str] = {}
        self.league_standings: pd.DataFrame = pd.DataFrame()
        self.league_team_roster_players: dict[int, dict[int, str]] = {}

    def get_player_stats_per_league(
        self,
    ) -> tuple[pd.DataFrame, dict[int, dict[int, str]]]:
        """Collect league statistics, propagating failures before any writes."""
        league_player_team_stats = {}
        inactive_players_per_team = {}

        executor = ThreadPoolExecutor(max_workers=4)
        team_results = {}
        try:
            team_results = {
                team_number: executor.submit(
                    self.get_player_stats_dataframe_per_team, team_number
                )
                for team_number in self.league_team_roster_players
            }
            for team_number, result in team_results.items():
                team_player_stats, inactive_player_info = result.result()
                league_player_team_stats[team_number] = team_player_stats
                if inactive_player_info:
                    inactive_players_per_team[team_number] = inactive_player_info
                logger.info(
                    "team_extraction_success",
                    team_name=self.team_id_name_mapping[team_number],
                    team_number=team_number,
                    players_count=len(team_player_stats),
                )
        finally:
            # workers can fail during shutdown; inspect them after joining.
            executor.shutdown(wait=True, cancel_futures=True)
            for team_number, result in team_results.items():
                if not result.cancelled() and (error := result.exception()) is not None:
                    logger.error(
                        "team_extraction_failed",
                        league=self.league_name,
                        team_name=self.team_id_name_mapping[team_number],
                        team_number=team_number,
                        error_type=type(error).__name__,
                        error=str(error),
                        exc_info=(type(error), error, error.__traceback__),
                    )
        player_stats = pd.concat(league_player_team_stats.values())
        player_stats["date"] = DATE_TIME_EXECUTION

        return player_stats, inactive_players_per_team

    def get_player_stats_dataframe_per_team(
        self,
        team_number: int,
    ) -> tuple[pd.DataFrame, dict[int, str]]:
        """Return roster players' totals and players without statistics.

        team_id identifies roster membership; statistics include all teams the
        player represented during the selected season or career.
        """

        roster_players = self.league_team_roster_players[team_number]

        (
            team_player_stats,
            active_players,
            inactive_player_info,
        ) = get_team_stats(roster_players)
        team_player_stats["playername"] = pd.Series(active_players)
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
        standings = statsapi.standings_data(str(league_number), season=SEASON_YEAR)

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


def run_extraction(database_path: str, league_name: str, execution_date: str) -> None:
    """Extract one league and save it only when every team succeeds."""
    logger.info("extraction_started", league=league_name, date=execution_date)
    data_extractor = DataExtractor(league_name=league_name)
    data_extractor.set_league_team_roster_players()
    logger.info(
        "league_standings_loaded",
        standings_shape=data_extractor.league_standings.shape,
    )

    data_extractor.set_team_ids_and_names()
    logger.info(
        "team_mapping_created", teams_count=len(data_extractor.team_id_name_mapping)
    )
    player_stats, inactive_players_per_team = (
        data_extractor.get_player_stats_per_league()
    )

    backup_database_before_migration(database_path)
    with create_connection(database_path) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        save_extraction_run(
            conn,
            league_name,
            execution_date,
            data_extractor.league_standings,
            player_stats,
        )
        # A same-day rerun must not leave reports reading the old source's treatment.
        invalidate_treated_run(conn, league_name, execution_date)
    logger.info(
        "league_standings_saved", database_path=database_path, table="league_standings"
    )
    logger.info(
        "extraction_completed",
        players_total=len(player_stats),
        inactive_players_count=sum(
            len(players) for players in inactive_players_per_team.values()
        ),
        failed_teams_count=0,
        database_path=database_path,
        table="player_stats",
    )
    if inactive_players_per_team:
        logger.warning(
            "inactive_players_found", inactive_players=inactive_players_per_team
        )


if __name__ == "__main__":
    run_extraction(get_database_path(), LEAGUE_NAME, DATE_TIME_EXECUTION)
