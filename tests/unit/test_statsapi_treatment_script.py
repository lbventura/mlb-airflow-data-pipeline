import pandas.api.types as pdtypes
import os
import pytest


from mlb_airflow_data_pipeline.statsapi_treatment_script import (
    batting_stats_list,
    batter_transformation_dict,
    DataTreater,
    DataTreaterInputRepresentation,
    DataPaths,
)

from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_plate_appearance_normalization,
    create_mean_normalization,
)

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

EXAMPLE_DATA_PATH = os.path.join(
    CURRENT_DIR, "national_league_example_full_player_stats_df.csv"
)


batter_filter_conditions_dict = {"plateAppearances": 100, "atBats": 50}

batter_input_paths = DataPaths(
    path_to_input_data=EXAMPLE_DATA_PATH,
)

batter_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=batting_stats_list,
    filter_conditions_dict=batter_filter_conditions_dict,
    transformation_dict=batter_transformation_dict,
)


def numeric_features_list() -> list[str]:
    return [
        "airOuts",
        "assists",
        "atBats",
        "avg",
        "balks",
        "baseOnBalls",
        "battersFaced",
        "blownSaves",
        "catcherERA",
        "catchersInterference",
        "caughtStealing",
        "chances",
        "completeGames",
        "doublePlays",
        "doubles",
        "earnedRuns",
        "era",
        "errors",
        "fielding",
        "games",
        "gamesFinished",
        "gamesPitched",
        "gamesPlayed",
        "gamesStarted",
        "groundIntoDoublePlay",
        "groundOuts",
        "hitBatsmen",
        "hitByPitch",
        "hits",
        "hitsPer9Inn",
        "holds",
        "homeRuns",
        "homeRunsPer9",
        "inheritedRunners",
        "inheritedRunnersScored",
        "innings",
        "inningsPitched",
        "intentionalWalks",
        "leftOnBase",
        "losses",
        "numberOfPitches",
        "obp",
        "ops",
        "outs",
        "passedBall",
        "pickoffs",
        "pitchesPerInning",
        "plateAppearances",
        "putOuts",
        "rangeFactorPerGame",
        "rbi",
        "runs",
        "runsScoredPer9",
        "sacBunts",
        "sacFlies",
        "saveOpportunities",
        "saves",
        "shutouts",
        "slg",
        "stolenBases",
        "strikeOuts",
        "strikePercentage",
        "strikeoutsPer9Inn",
        "strikes",
        "throwingErrors",
        "totalBases",
        "triplePlays",
        "triples",
        "walksPer9Inn",
        "whip",
        "wildPitches",
        "wins",
    ]


def object_features_list():
    return [
        "atBatsPerHomeRun",
        "babip",
        "groundOutsToAirouts",
        "playername",
        "rangeFactorPer9Inn",
        "stolenBasePercentage",
        "strikeoutWalkRatio",
        "winPercentage",
    ]


@pytest.mark.parametrize(
    "is_numeric, feature_list",
    [(True, numeric_features_list()), (False, object_features_list())],
)
def test_data_treater_input_data(is_numeric, feature_list):
    # temporary test until type conversion is implemented
    # checks that pd.read_csv type conversion is working as expected
    batter_data_treater = DataTreater(
        data_paths=batter_input_paths, input_parameters=batter_input_data_repr
    )
    output_df = batter_data_treater.get_input_data()
    output_df_is_numeric_columns = {
        col: pdtypes.is_numeric_dtype(output_df[col]) for col in output_df.columns
    }

    extracted_numeric_features = sorted(
        [
            key
            for key, value in output_df_is_numeric_columns.items()
            if value == is_numeric
        ]
    )
    expected_numeric_features = feature_list
    assert extracted_numeric_features == expected_numeric_features


def test_data_treater_filter_data():
    batter_data_treater = DataTreater(
        data_paths=batter_input_paths, input_parameters=batter_input_data_repr
    )
    output_df = batter_data_treater.get_filter_data()

    expected_output_shape = (90, 29)
    assert output_df.shape == expected_output_shape

    assert not sum(
        output_df["plateAppearances"]
        < batter_filter_conditions_dict.get("plateAppearances")
    )
    assert not sum(output_df["atBats"] < batter_filter_conditions_dict.get("atBats"))


def test_data_treater_get_output_data():
    batter_data_treater = DataTreater(
        data_paths=batter_input_paths, input_parameters=batter_input_data_repr
    )
    output_data = batter_data_treater.get_output_data()

    actual_features = set(output_data.columns)

    # for babip and difstrikeOutsbaseOnBalls, the test
    # is less restrictive than below because these two features
    # are used to generate other features below
    assert "babip" in actual_features
    assert "difstrikeOutsbaseOnBalls" in actual_features

    expected_per_plate_features = {
        ele + "perplateAppearance"
        for ele in batter_input_data_repr.transformation_dict[
            create_plate_appearance_normalization
        ]
    }

    assert expected_per_plate_features.difference(actual_features) == set()

    expected_normalized_features = {
        ele + suffix
        for ele in batter_input_data_repr.transformation_dict[create_mean_normalization]
        for suffix in ["_mean", "_std", "_z_score"]
    } | {
        "normalized_" + ele
        for ele in batter_input_data_repr.transformation_dict[create_mean_normalization]
    }

    assert expected_normalized_features.difference(actual_features) == set()
