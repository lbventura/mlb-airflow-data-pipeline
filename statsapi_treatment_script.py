import logging

import numpy as np
import pandas as pd

from pydantic import BaseModel

from statsapi_parameters_script import (
    BATTER_DATA_FILE_NAME,
    DATA_FILE_LOCATION,
    DATA_FILTER_THRESHOLD,
    PITCHER_DATA_FILE_NAME,
    PLAYER_DATA_FILE_NAME,
    DEFENDER_DATA_FILE_NAME,
)

# creates a logger
logging.basicConfig(
    filename="mlb-airflow-debugger-treatment.log",
    format="%(asctime)s: %(levelname)s: %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    filemode="w",
)


class DataTreaterInputRepresentation(BaseModel):
    path_to_input_data: str
    path_to_output_data: str
    subset_columns: list
    filter_conditions_dict: dict
    custom_features: dict
    transformation_list: list


class DataTreater:
    def __init__(self, input_parameters: DataTreaterInputRepresentation):
        self.input_parameters = input_parameters

    def get_input_data(self) -> pd.DataFrame:
        input_data = pd.read_csv(self.input_parameters.path_to_input_data, index_col=0)
        logging.info(f"Data input was successful")
        return input_data

    def get_subset_data(self) -> pd.DataFrame:
        intermediate_data = self.get_input_data()[
            self.input_parameters.subset_columns
        ].dropna()
        logging.info(
            f"The set of columns for this dataset is {self.input_parameters.subset_columns}"
        )
        logging.info(f"Creating sub-dataset columns was successful")
        return intermediate_data

    def get_filter_data(self) -> pd.DataFrame:
        filtered_data = filter_data(
            self.get_subset_data(), self.input_parameters.filter_conditions_dict
        )
        logging.info(f"Filtering data was successful")
        return filtered_data

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

    def get_output_data_file(self):
        (self.output_data).to_csv(self.input_parameters.path_to_output_data)  # type: ignore
        logging.info(f"Generating output file was successful")
        return self


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


def create_plate_appearance_normalization(
    input_df: pd.DataFrame, feature_name_list: list
) -> pd.DataFrame:
    """Generates a list of features normalized by the number of a player's plate appearances.
    This is a better estimator of a player's performance because players can have
    an excellent total number (say, of homeRuns) simply because they have a lot of
    plate appearances.

    Args:
        input_df (pd.DataFrame)
        feature_name_list (list)

    Returns:
        pd.DataFrame
    """

    for feature_name in feature_name_list:
        input_df[feature_name + "perplateAppearance"] = (
            input_df[feature_name] / input_df["plateAppearances"]
        )

    return input_df


def create_innings_pitched_normalization(
    input_df: pd.DataFrame, feature_name_list: list
) -> pd.DataFrame:
    """Generates a list of features normalized by the number of a player's pitched innings.
    This is a better estimator of a player's performance because players can have
    an excellent total number (say, of outs) simply because they have a lot of
    innings played.

    Args:
        input_df (pd.DataFrame)
        feature_name_list (list)

    Returns:
        pd.DataFrame
    """

    for feature_name in feature_name_list:
        input_df[feature_name + "inningsPitched"] = (
            input_df[feature_name] / input_df["inningsPitched"]
        )

    return input_df


def create_mean_normalization(
    input_df: pd.DataFrame, feature_name_list: list
) -> pd.DataFrame:
    """Generates a list of features feature normalized by the league's mean (set to 100).
    This allows for a direct comparison between different players.

    Args:
        input_df (pd.DataFrame)
        feature_name_list (list)

    Returns:
        pd.DataFrame
    """
    for feature_name in feature_name_list:
        input_df[feature_name + "_mean"] = input_df[feature_name].mean()
        input_df[feature_name + "_std"] = input_df[feature_name].std()
        input_df["normalized_" + feature_name] = 100 * (
            input_df[feature_name] / input_df[feature_name + "_mean"]
        )
        input_df[feature_name + "_z_score"] = (
            input_df[feature_name] - input_df[feature_name + "_mean"]
        ) / input_df[feature_name + "_std"]

    return input_df


def create_babip(input_df: pd.DataFrame) -> pd.DataFrame:
    """Generates the BABIP statistic according to MLB.
    See https://www.mlb.com/glossary/advanced-stats/babip

    BABIP is present in the original set of stats, but because
    it is handled as a string, it is recreated here.

    Args:
        input_df (pd.DataFrame)

    Returns:
        pd.DataFrame
    """
    input_df["babip"] = (input_df["hits"] - input_df["homeRuns"]) / (
        input_df["atBats"]
        - input_df["strikeOuts"]
        - input_df["homeRuns"]
        + input_df["sacFlies"]
    )  # following MLB's formula

    return input_df


def create_dif_strike_outs_base_on_balls(input_df: pd.DataFrame) -> pd.DataFrame:
    """Generates the difference between strikeouts
    and base_on_balls (i.e, walks)

    Args:
        input_df (pd.DataFrame)

    Returns:
        pd.DataFrame
    """
    input_df["difstrikeOutsbaseOnBalls"] = (
        input_df["strikeOuts"] - input_df["baseOnBalls"]
    )
    return input_df


# separate data into batting, pitching and defense
batting_stats = [
    "gamesPlayed",
    "groundOuts",
    "airOuts",
    "runs",
    "doubles",
    "triples",
    "homeRuns",
    "strikeOuts",
    "baseOnBalls",
    "intentionalWalks",
    "hits",
    "hitByPitch",
    "avg",
    "atBats",
    "obp",
    "slg",
    "ops",
    "caughtStealing",
    "stolenBases",
    "groundIntoDoublePlay",
    "plateAppearances",
    "totalBases",
    "rbi",
    "leftOnBase",
    "sacBunts",
    "sacFlies",
    "groundOutsToAirouts",
    "catchersInterference",
]

pitching_stats = [
    "gamesPlayed",
    "gamesStarted",
    "intentionalWalks",
    "numberOfPitches",
    "inningsPitched",
    "era",
    "wins",
    "losses",
    "saves",
    "saveOpportunities",
    "holds",
    "blownSaves",
    "earnedRuns",
    "whip",
    "battersFaced",
    "gamesPitched",
    "completeGames",
    "shutouts",
    "strikes",
    "strikePercentage",
    "hitBatsmen",
    "balks",
    "wildPitches",
    "pickoffs",
    "pitchesPerInning",
    "gamesFinished",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
    "inheritedRunners",
    "inheritedRunnersScored",
    "innings",
    "outs",
]

defensive_stats = [
    "gamesPlayed",
    "plateAppearances",
    "atBats",
    "assists",
    "catcherERA",
    "chances",
    "doublePlays",
    "errors",
    "fielding",
    "games",
    "passedBall",
    "putOuts",
    "rangeFactorPer9Inn",
    "rangeFactorPerGame",
    "throwingErrors",
    "triplePlays",
]

# create final list
batting_stats_list = ["playername"] + batting_stats
pitching_stats_list = ["playername"] + pitching_stats
defending_stats_list = ["playername"] + defensive_stats

# setting up information for the batter extraction
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

# same thing for pitchers
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

# same thing for defenders
# there is a weird choice to be made here as defensive players are simultaneously offensive players

defensive_stats = [
    "gamesPlayed",
    "assists",
    "catcherERA",
    "chances",
    "doublePlays",
    "errors",
    "fielding",
    "games",
    "passedBall",
    "putOuts",
    # "rangeFactorPer9Inn", # probably need to compute rangeFactor by hand
    "rangeFactorPerGame",
    "throwingErrors",
    "triplePlays",
]

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

if __name__ == "__main__":

    logging.info("Data treatment started")

    # batter
    batter_input_data_repr = DataTreaterInputRepresentation(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + BATTER_DATA_FILE_NAME,
        subset_columns=batting_stats_list,
        filter_conditions_dict=batter_filter_conditions_dict,
        custom_features=batter_custom_features_dict,
        transformation_list=batter_transformation_list,
    )

    batter_data_treater = DataTreater(input_parameters=batter_input_data_repr)

    batter_data_treater.set_output_data()
    batter_data_treater.get_output_data_file()

    logging.info("Data treatment for batters finished")

    # pitcher
    pitcher_input_data_repr = DataTreaterInputRepresentation(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + PITCHER_DATA_FILE_NAME,
        subset_columns=pitching_stats_list,
        filter_conditions_dict=pitcher_filter_conditions_dict,
        custom_features=pitcher_custom_features_dict,
        transformation_list=pitcher_transformation_list,
    )

    pitcher_data_treater = DataTreater(input_parameters=pitcher_input_data_repr)

    pitcher_data_treater.set_output_data()
    pitcher_data_treater.get_output_data_file()

    logging.info("Data treatment for pitchers finished")

    # defender
    defender_input_data_repr = DataTreaterInputRepresentation(
        path_to_input_data=DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME,
        path_to_output_data=DATA_FILE_LOCATION + DEFENDER_DATA_FILE_NAME,
        subset_columns=defending_stats_list,
        filter_conditions_dict=defender_filter_conditions_dict,
        custom_features=defender_custom_features_dict,
        transformation_list=defender_transformation_list,
    )

    defender_data_treater = DataTreater(input_parameters=defender_input_data_repr)

    defender_data_treater.set_output_data()
    defender_data_treater.get_output_data_file()

    logging.info("Data treatment for defenders finished")

    logging.info("Data treatment finished")
