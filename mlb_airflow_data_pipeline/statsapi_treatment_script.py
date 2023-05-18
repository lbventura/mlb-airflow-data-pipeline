import logging

import numpy as np
import pandas as pd

from pydantic import BaseModel
from typing import Optional

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    LEAGUE_NAME,
    BATTING_STATS,
    PITCHING_STATS,
    DEFENSIVE_STATS,
)

from mlb_airflow_data_pipeline.statsapi_extraction_script import (
    DATE_TIME_EXECUTION,
    PLAYER_DATA_FILE_NAME,
)

from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_plate_appearance_normalization,
    create_innings_pitched_normalization,
    create_mean_normalization,
    create_babip,
    create_dif_strike_outs_base_on_balls,
)

import argparse

# creates a logger
logging.basicConfig(
    filename="mlb-airflow-debugger-treatment.log",
    format="%(asctime)s: %(levelname)s: %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    filemode="w",
)

DATA_FILTER_THRESHOLD = 0.6

# TODO: remove this duplication needed for the analysis script
OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"

BATTER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_batter_stats_df.csv"


class DataTreaterPaths(BaseModel):
    path_to_input_data: str
    path_to_output_data: Optional[str] = None


class DataTreaterInputRepresentation(BaseModel):
    subset_columns: list
    filter_conditions_dict: dict
    custom_features: dict
    transformation_list: list


class DataTreater:
    def __init__(
        self,
        data_paths: DataTreaterPaths,
        input_parameters: DataTreaterInputRepresentation,
    ):
        self.data_paths = data_paths
        self.input_parameters = input_parameters

    def get_output_data_file(self):
        (self.output_data).to_csv(self.data_paths.path_to_output_data)  # type: ignore
        logging.info(f"Generating output file was successful")
        return self

    def set_output_data(self) -> None:

        filtered_data = self.get_filter_data()

        for function in self.input_parameters.transformation_list:
            if function.__name__ in self.input_parameters.custom_features.keys():
                filtered_data = function(
                    filtered_data,
                    self.input_parameters.custom_features[function.__name__],
                )
            else:
                filtered_data = function(filtered_data)
            logging.info(f"Generating output data was successful")

        self.output_data = filtered_data

    def get_filter_data(self) -> pd.DataFrame:
        filtered_data = filter_data(
            self.get_subset_data(), self.input_parameters.filter_conditions_dict
        )
        logging.info(f"Filtering data was successful")
        return filtered_data

    def get_subset_data(self) -> pd.DataFrame:
        intermediate_data = self.get_input_data()[self.input_parameters.subset_columns]
        logging.info(
            f"The set of columns for this dataset is {self.input_parameters.subset_columns}"
        )
        logging.info(f"Creating sub-dataset columns was successful")
        return intermediate_data

    def get_input_data(self) -> pd.DataFrame:
        input_data = pd.read_csv(self.data_paths.path_to_input_data, index_col=0)
        logging.info(f"Data input was successful")
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
    logging.info(f"The percentage of good data is {round(good_data_percentage,3)}")

    if good_data_percentage < DATA_FILTER_THRESHOLD:
        logging.warning(
            f"Possible data problem: the percentage of good data, {good_data_percentage}, is below the threshold"
        )

    return return_df


# setting up information for the batter extraction
batting_stats_list = ["playername"] + BATTING_STATS
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

batter_custom_features_dict = {
    "create_plate_appearance_normalization": batter_plate_norm_stats,
    "create_mean_normalization": batter_mean_norm_stats,
}

batter_transformation_list = [
    create_babip,
    create_dif_strike_outs_base_on_balls,
    create_plate_appearance_normalization,
    create_mean_normalization,
]

batter_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=batting_stats_list,
    filter_conditions_dict=batter_filter_conditions_dict,
    custom_features=batter_custom_features_dict,
    transformation_list=batter_transformation_list,
)

# same thing for pitchers
pitching_stats_list = ["playername"] + PITCHING_STATS
pitcher_filter_conditions_dict = {"gamesPlayed": 25, "inningsPitched": 50}

pitcher_innings_norm_stats = ["intentionalWalks", "numberOfPitches", "strikes", "outs"]

pitcher_mean_norm_stats = [
    "era",
    "whip",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
]

pitcher_custom_features_dict = {
    "create_innings_pitched_normalization": pitcher_innings_norm_stats,
    "create_mean_normalization": pitcher_mean_norm_stats,
}

pitcher_transformation_list = [
    create_innings_pitched_normalization,
    create_mean_normalization,
]

pitcher_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=pitching_stats_list,
    filter_conditions_dict=pitcher_filter_conditions_dict,
    custom_features=pitcher_custom_features_dict,
    transformation_list=pitcher_transformation_list,
)

# same thing for defenders
defending_stats_list = ["playername"] + DEFENSIVE_STATS

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

defender_custom_features_dict = {
    "create_mean_normalization": defender_mean_norm_stats,
}

defender_transformation_list = [
    create_mean_normalization,
]

defender_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=defending_stats_list,
    filter_conditions_dict=defender_filter_conditions_dict,
    custom_features=defender_custom_features_dict,
    transformation_list=defender_transformation_list,
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

    logging.info("Data treatment started")

    # batter
    batter_input_paths = DataTreaterPaths(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + BATTER_DATA_FILE_NAME,
    )

    batter_data_treater = DataTreater(
        data_paths=batter_input_paths, input_parameters=batter_input_data_repr
    )

    batter_data_treater.set_output_data()
    batter_data_treater.get_output_data_file()

    logging.info("Data treatment for batters finished")

    # pitcher
    pitcher_input_paths = DataTreaterPaths(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + PITCHER_DATA_FILE_NAME,
    )

    pitcher_data_treater = DataTreater(
        data_paths=pitcher_input_paths, input_parameters=pitcher_input_data_repr
    )

    pitcher_data_treater.set_output_data()
    pitcher_data_treater.get_output_data_file()

    logging.info("Data treatment for pitchers finished")

    # defender
    defender_input_paths = DataTreaterPaths(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + DEFENDER_DATA_FILE_NAME,
    )

    defender_data_treater = DataTreater(
        data_paths=defender_input_paths, input_parameters=defender_input_data_repr
    )

    defender_data_treater.set_output_data()
    defender_data_treater.get_output_data_file()

    logging.info("Data treatment for defenders finished")

    logging.info("Data treatment finished")
