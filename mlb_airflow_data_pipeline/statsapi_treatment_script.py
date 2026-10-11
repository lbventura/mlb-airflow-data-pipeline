import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel

from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    get_database_path,
    read_player_stats,
)
from mlb_airflow_data_pipeline.statsapi_extraction_script import DATE_TIME_EXECUTION
from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_babip,
    create_dif_strike_outs_base_on_balls,
    create_innings_pitched_normalization,
    create_mean_normalization,
    create_plate_appearance_normalization,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    BATTING_SOURCE_STATS,
    DATA_FILE_LOCATION,
    DEFENDER_POSITIONS,
    DEFENSIVE_STATS,
    FIELDING_SOURCE_STATS,
    LEAGUE_NAME,
    PITCHING_STATS,
    PITCHING_SOURCE_STATS,
    PLAYER_INFORMATION,
    expected_output_columns,
)

from mlb_airflow_data_pipeline.logging_setup import get_logger

# Initialize structured logger
logger = get_logger("statsapi_treatment", league=LEAGUE_NAME)

DATA_FILTER_THRESHOLD = 0.6


OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"
BATTER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_batter_stats_df.csv"
PITCHER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_pitcher_stats_df.csv"


class DataPaths(BaseModel):
    path_to_input_data: str
    path_to_output_data: Optional[str] = None


class TreatmentDataPaths(BaseModel):
    """SQLite source, league/date selection, and CSV destination for treatment."""

    database_path: str
    league_name: str
    execution_date: str
    path_to_output_data: str


class DataTreaterInputRepresentation(BaseModel):
    subset_columns: list
    filter_conditions_dict: dict
    transformation_dict: dict
    stats_group: str


def _convert_numeric_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert known metrics, treating undefined API rates as missing values."""
    numeric_columns = (
        (set(expected_output_columns()) - {"playername", "date"})
        | BATTING_SOURCE_STATS
        | PITCHING_SOURCE_STATS
        | FIELDING_SOURCE_STATS
        | {"player_id"}
    )
    for column in dataframe.columns.intersection(numeric_columns):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    return dataframe


class DataTreater:
    def __init__(
        self,
        data_paths: TreatmentDataPaths,
        input_parameters: DataTreaterInputRepresentation,
    ):
        self.data_paths = data_paths
        self.input_parameters = input_parameters

    def set_output_data_file(self) -> None:
        output_data = self.get_output_data()
        output_path = self.data_paths.path_to_output_data
        output_data.to_csv(output_path)  # type: ignore
        logger.info(
            "output_file_generated",
            file_path=output_path,
            data_shape=output_data.shape,
            columns=list(output_data.columns[:5]),  # First 5 columns for brevity
        )

    def get_output_data(self) -> pd.DataFrame:
        filtered_data = self.get_filter_data()

        transformation_dict = self.input_parameters.transformation_dict

        for function in transformation_dict.keys():
            if transformation_dict[function]:
                filtered_data = function(
                    filtered_data,
                    transformation_dict[function],
                )
            else:
                filtered_data = function(filtered_data)

        logger.info(
            "data_transformation_completed",
            transformations_applied=len(
                [k for k, v in transformation_dict.items() if v]
            ),
        )
        return filtered_data

    def get_filter_data(self) -> pd.DataFrame:
        filtered_data = filter_data(
            self.get_subset_data(), self.input_parameters.filter_conditions_dict
        )
        logger.info("data_filtering_completed", filtered_shape=filtered_data.shape)
        return filtered_data

    def get_subset_data(self) -> pd.DataFrame:
        input_data = self.get_input_data()
        columns = self.input_parameters.subset_columns
        group = self.input_parameters.stats_group
        grouped_columns = [
            column
            for column in input_data.columns
            if column.startswith(("hitting_", "pitching_", "fielding_"))
        ]
        # Old snapshots share the table's new columns, but contain no grouped values.
        has_grouped_stats = input_data[grouped_columns].notna().any(axis=None)
        if has_grouped_stats or (input_data.empty and grouped_columns):
            if group == "fielding":
                intermediate_data = _fielding_subset(input_data, columns)
            else:
                intermediate_data = _grouped_subset(input_data, columns, group)
        else:
            intermediate_data = input_data[columns]
        logger.info(
            "subset_data_created",
            subset_columns=self.input_parameters.subset_columns,
            subset_shape=intermediate_data.shape,
        )
        return intermediate_data

    def get_input_data(self) -> pd.DataFrame:
        with create_connection(self.data_paths.database_path) as conn:
            input_data = read_player_stats(
                conn,
                self.data_paths.league_name,
                self.data_paths.execution_date,
            )
        input_data = _convert_numeric_columns(input_data)
        logger.info(
            "data_input_loaded",
            database_path=self.data_paths.database_path,
            league_name=self.data_paths.league_name,
            execution_date=self.data_paths.execution_date,
            data_shape=input_data.shape,
        )
        return input_data


def _grouped_subset(
    input_data: pd.DataFrame, columns: list[str], group: str
) -> pd.DataFrame:
    source_columns = [
        name if name in PLAYER_INFORMATION else f"{group}_{name}" for name in columns
    ]
    if group.startswith("fielding_"):
        # Position-specific metrics (for example catcherERA) are optional.
        selected = input_data.reindex(columns=source_columns)
        selected[PLAYER_INFORMATION] = input_data[PLAYER_INFORMATION]
    elif not any(column.startswith(f"{group}_") for column in input_data.columns):
        return pd.DataFrame(columns=columns, dtype=float)
    else:
        selected = input_data[source_columns].copy()
    selected.columns = columns
    return selected


def _fielding_subset(input_data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    batting_columns = [
        name for name in columns if name in {"atBats", "plateAppearances"}
    ]
    player_positions = []
    for position in DEFENDER_POSITIONS:
        group = f"fielding_{position}"
        if f"{group}_gamesPlayed" not in input_data.columns:
            continue
        players = input_data[input_data[f"{group}_gamesPlayed"].notna()]
        if players.empty:
            continue
        selected = _grouped_subset(players, columns, group)
        selected[batting_columns] = _grouped_subset(players, batting_columns, "hitting")
        selected["position"] = position
        player_positions.append(selected)
    if not player_positions:
        return pd.DataFrame(columns=[*columns, "position"], dtype=float)
    return pd.concat(player_positions, ignore_index=True)


def filter_data(input_df: pd.DataFrame, conditions_dict: dict) -> pd.DataFrame:
    """Filter data from the input DataFrame as specified by the conditions.
    Raises a warning at runtime if the percentage of good data is below a certain threshold

    Args:
        input_df (pd.DataFrame)
        conditions_dict (dict)

    Returns:
        pd.DataFrame
    """
    if input_df.empty:
        return input_df.copy()
    conditions = np.logical_and.reduce(
        [(input_df[key] >= value) for key, value in conditions_dict.items()]
    )

    return_df = input_df[conditions].copy()

    good_data_percentage = len(return_df) / len(input_df)

    logger.info(
        "data_quality_check",
        good_data_percentage=round(good_data_percentage, 3),
        records_kept=len(return_df),
        records_total=len(input_df),
        filter_conditions=conditions_dict,
    )

    if good_data_percentage < DATA_FILTER_THRESHOLD:
        logger.warning(
            "data_quality_below_threshold",
            good_data_percentage=good_data_percentage,
            threshold=DATA_FILTER_THRESHOLD,
            filter_conditions=conditions_dict,
        )

    return return_df


# setting up information for the batter extraction
batting_stats_list = PLAYER_INFORMATION + BATTING_STATS
batter_filter_conditions_dict = {"plateAppearances": 100, "atBats": 50}

batter_plate_norm_stats = [
    "strikeOuts",
    "homeRuns",
    "hits",
    "rbi",
    "baseOnBalls",
    "totalBases",
    "difstrikeOutsbaseOnBalls",
]
batter_mean_norm_stats = [
    "avg",
    "babip",
    "obp",
    "ops",
    "hits",
    "rbi",
    "baseOnBalls",
    "totalBases",
    "difstrikeOutsbaseOnBalls",
]


batter_transformation_dict = {
    create_babip: None,
    create_dif_strike_outs_base_on_balls: None,
    create_plate_appearance_normalization: batter_plate_norm_stats,
    create_mean_normalization: batter_mean_norm_stats,
}

batter_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=batting_stats_list,
    filter_conditions_dict=batter_filter_conditions_dict,
    transformation_dict=batter_transformation_dict,
    stats_group="hitting",
)

# same thing for pitchers
pitching_stats_list = PLAYER_INFORMATION + PITCHING_STATS
pitcher_filter_conditions_dict = {"gamesPlayed": 5, "inningsPitched": 15}

pitcher_innings_norm_stats = ["intentionalWalks", "numberOfPitches", "strikes", "outs"]

pitcher_mean_norm_stats = [
    "era",
    "whip",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
]


pitcher_transformation_dict = {
    create_innings_pitched_normalization: pitcher_innings_norm_stats,
    create_mean_normalization: pitcher_mean_norm_stats,
}

pitcher_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=pitching_stats_list,
    filter_conditions_dict=pitcher_filter_conditions_dict,
    transformation_dict=pitcher_transformation_dict,
    stats_group="pitching",
)

# same thing for defenders
defending_stats_list = PLAYER_INFORMATION + DEFENSIVE_STATS

defender_filter_conditions_dict = {
    "plateAppearances": 100,
    "atBats": 50,
}  # this likely needs to be augmented in light of the analysis for batters


defender_mean_norm_stats = [
    "catcherERA",
    "assists",
    "errors",
    # "rangeFactorPer9Inn",
    "doublePlays",
    "fielding",
]

defender_transformation_dict = {
    create_mean_normalization: defender_mean_norm_stats,
}

defender_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=defending_stats_list,
    filter_conditions_dict=defender_filter_conditions_dict,
    transformation_dict=defender_transformation_dict,
    stats_group="fielding",
)


def treat_player_stats(
    database_path: str,
    league_name: str,
    execution_date: str,
    output_directory: str,
) -> None:
    """Create treated player-statistics files for one league run."""
    output_details = f"{league_name}_{execution_date}"
    player_configs = [
        ("batter", batter_input_data_repr, f"{output_details}_batter_stats_df.csv"),
        ("pitcher", pitcher_input_data_repr, f"{output_details}_pitcher_stats_df.csv"),
        (
            "defender",
            defender_input_data_repr,
            f"{output_details}_defender_stats_df.csv",
        ),
    ]

    for player_type, input_parameters, output_filename in player_configs:
        logger.info("processing_player_type", player_type=player_type)
        data_paths = TreatmentDataPaths(
            database_path=database_path,
            league_name=league_name,
            execution_date=execution_date,
            path_to_output_data=str(Path(output_directory) / output_filename),
        )
        DataTreater(data_paths, input_parameters).set_output_data_file()
        logger.info("player_type_completed", player_type=player_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Optional input arguments for treatment script",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--date", type=str)
    parser.add_argument("--league_name", type=str)

    args = parser.parse_args()
    config = vars(args)

    input_date = config.get("date")
    input_league_name = config.get("league_name")

    if input_date:
        DATE_TIME_EXECUTION = config["date"]

    if input_league_name:
        LEAGUE_NAME = config["league_name"]

    logger.info(
        "treatment_started",
        league=LEAGUE_NAME,
        date=DATE_TIME_EXECUTION,
        player_types=["batter", "pitcher", "defender"],
    )

    treat_player_stats(
        get_database_path(),
        LEAGUE_NAME,
        DATE_TIME_EXECUTION,
        DATA_FILE_LOCATION,
    )

    logger.info("treatment_completed", player_types_processed=3)
