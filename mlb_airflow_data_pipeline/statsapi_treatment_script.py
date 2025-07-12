import argparse
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    DATE_TIME_EXECUTION,
    PLAYER_DATA_FILE_NAME,
)
from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_babip,
    create_dif_strike_outs_base_on_balls,
    create_innings_pitched_normalization,
    create_mean_normalization,
    create_plate_appearance_normalization,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    DATA_FILE_LOCATION,
    DEFENSIVE_STATS,
    LEAGUE_NAME,
    PITCHING_STATS,
    PLAYER_INFORMATION,
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


class DataTreaterInputRepresentation(BaseModel):
    subset_columns: list
    filter_conditions_dict: dict
    transformation_dict: dict


class DataTreater:
    def __init__(
        self,
        data_paths: DataPaths,
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
        intermediate_data = self.get_input_data()[self.input_parameters.subset_columns]
        logger.info(
            "subset_data_created",
            subset_columns=self.input_parameters.subset_columns,
            subset_shape=intermediate_data.shape,
        )
        return intermediate_data

    def get_input_data(self) -> pd.DataFrame:
        input_data = pd.read_csv(self.data_paths.path_to_input_data, index_col=0)
        logger.info(
            "data_input_loaded",
            file_path=self.data_paths.path_to_input_data,
            data_shape=input_data.shape,
        )
        return input_data


def filter_data(input_df: pd.DataFrame, conditions_dict: dict) -> pd.DataFrame:
    """Filter data from the input DataFrame as specified by the conditions.
    Raises a warning at runtime if the percentage of good data is below a certain threshold

    Args:
        input_df (pd.DataFrame)
        conditions_dict (dict)

    Returns:
        pd.DataFrame
    """
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
)

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

    PLAYER_DATA_FILE_NAME = (
        f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}_full_player_stats_df.csv"
    )

    OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"

    BATTER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_batter_stats_df.csv"
    PITCHER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_pitcher_stats_df.csv"
    DEFENDER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_defender_stats_df.csv"

    logger.info(
        "treatment_started",
        league=LEAGUE_NAME,
        date=DATE_TIME_EXECUTION,
        player_types=["batter", "pitcher", "defender"],
    )

    # Process each player type
    player_configs = [
        ("batter", batter_input_data_repr, BATTER_DATA_FILE_NAME),
        ("pitcher", pitcher_input_data_repr, PITCHER_DATA_FILE_NAME),
        ("defender", defender_input_data_repr, DEFENDER_DATA_FILE_NAME),
    ]

    for player_type, input_params, output_filename in player_configs:
        logger.info("processing_player_type", player_type=player_type)

        try:
            input_paths = DataPaths(
                path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
                path_to_output_data=DATA_FILE_LOCATION + output_filename,
            )

            data_treater = DataTreater(
                data_paths=input_paths, input_parameters=input_params
            )

            data_treater.set_output_data_file()

            logger.info("player_type_completed", player_type=player_type)

        except Exception as e:
            logger.error(
                "player_type_failed",
                player_type=player_type,
                error=str(e),
                exc_info=True,
            )
            raise

    logger.info("treatment_completed", player_types_processed=len(player_configs))
