import logging

import numpy as np
import pandas as pd

from statsapi_parameters_script import (
    BATTER_DATA_FILE_NAME,
    DATA_FILE_LOCATION,
    DATA_FILTER_THRESHOLD,
    PLAYER_DATA_FILE_NAME,
)

# creates a logger
logging.basicConfig(
    filename="mlb-airflow-debugger-treatment.log",
    format="%(asctime)s: %(levelname)s: %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    filemode="w",
)


# helper functions
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
            f"There might be a data problem: the percentage of good data, {good_data_percentage}, is below the threshold"
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

# guarantee type correctness
batting_stats_int_list = [
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
    "atBats",
    "caughtStealing",
    "stolenBases",
    "groundIntoDoublePlay",
    "plateAppearances",
    "totalBases",
    "leftOnBase",
    "sacBunts",
    "sacFlies",
    "catchersInterference",
]

batting_stats_float_list = [
    ele for ele in batting_stats if ele not in batting_stats_int_list
]

# we threw it away, but we should guarantee column types here
# create batting stats list
batting_stats_list = ["playername"] + batting_stats_int_list + batting_stats_float_list

if __name__ == "__main__":

    # import data
    stats_df = pd.read_csv(DATA_FILE_LOCATION + PLAYER_DATA_FILE_NAME, index_col=0)

    logging.info("Data treatment started")

    # drop rows for which there is no playername
    batting_stats_df = stats_df[batting_stats_list].dropna()

    # filter data
    conditions_dict = {"plateAppearances": 100, "atBats": 50}

    batting_stats_df_red = filter_data(batting_stats_df, conditions_dict)

    # generate features
    batting_stats_df_red = create_babip(batting_stats_df_red)

    batting_stats_df_red = create_dif_strike_outs_base_on_balls(batting_stats_df_red)

    batting_stats_df_red = create_plate_appearance_normalization(
        batting_stats_df_red,
        [
            "strikeOuts",
            "homeRuns",
            "hits",
            "rbi",
            "baseOnBalls",
            "totalBases",
            "difstrikeOutsbaseOnBalls",
        ],
    )

    batting_stats_df_red = create_mean_normalization(
        batting_stats_df_red,
        [
            "avg",
            "babip",
            "obp",
            "ops",
            "hits",
            "rbi",
            "baseOnBalls",
            "totalBases",
            "difstrikeOutsbaseOnBalls",
        ],
    )

    # save data
    batting_stats_df_red.to_csv(DATA_FILE_LOCATION + BATTER_DATA_FILE_NAME)

    logging.info("Data treatment finished")
