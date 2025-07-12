import pandas as pd

from mlb_airflow_data_pipeline.logging_setup import get_logger

# Initialize logger for feature utilities
logger = get_logger("statsapi_feature_utils")


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

    logger.debug(
        "plate_appearance_normalization_started",
        features=feature_name_list,
        data_shape=input_df.shape,
    )

    for feature_name in feature_name_list:
        input_df[feature_name + "perplateAppearance"] = (
            input_df[feature_name] / input_df["plateAppearances"]
        )

    logger.debug(
        "plate_appearance_normalization_completed",
        features_created=len(feature_name_list),
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

    logger.debug(
        "innings_pitched_normalization_started",
        features=feature_name_list,
        data_shape=input_df.shape,
    )

    for feature_name in feature_name_list:
        input_df[feature_name + "inningsPitched"] = (
            input_df[feature_name] / input_df["inningsPitched"]
        )

    logger.debug(
        "innings_pitched_normalization_completed",
        features_created=len(feature_name_list),
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
    logger.debug("babip_calculation_started", data_shape=input_df.shape)

    input_df["babip"] = (input_df["hits"] - input_df["homeRuns"]) / (
        input_df["atBats"]
        - input_df["strikeOuts"]
        - input_df["homeRuns"]
        + input_df["sacFlies"]
    )  # following MLB's formula

    logger.debug("babip_calculation_completed")
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
