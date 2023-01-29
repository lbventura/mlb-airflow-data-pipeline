import glob
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import uuid

from typing import Dict
from datetime import datetime
from matplotlib.ticker import MaxNLocator

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")


from statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    OUTPUT_FILE_LOCATION,
    LEAGUE_NAME_LOCATION,
)

matplotlib.use("Agg")

with open(LEAGUE_NAME_LOCATION, "r") as text_file:
    LEAGUE_NAME = text_file.readline().strip()


def sorting_and_index_reset(input_df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Sorting by team name, adding date as a column and then setting it as
    an index

    Args:
        input_df (pd.DataFrame)

    Returns:
        pd.DataFrame
    """

    mod_input_df = (
        input_df.sort_values(by=["name"], ascending=True)
        .reset_index(drop=True)
        .set_index("name")
        .T
    )

    # dropping the duplicate column for Luis Garcia
    mod_input_df = mod_input_df.loc[:, ~mod_input_df.columns.duplicated()]

    mod_input_df["date"] = date
    mod_input_df = mod_input_df.reset_index().set_index("date")

    return mod_input_df


def creating_time_series(
    DATASET_NAME: str,
    LEAGUE_NAME: str,
    TIME_SERIES_VARIABLES_LIST: list,
) -> tuple[dict, dict]:
    """Imports all the files from a particular dataset, extracting time series
    for the variables specified in TIME_SERIES_VARIABLES_LIST.

    DATASET_NAME can be ["full_player_stats", "league_standings", "batter_stats"].

    Returns:
        tuple

    """

    filenames = sorted(
        glob.glob(DATA_FILE_LOCATION + f"{LEAGUE_NAME}_*_{DATASET_NAME}_df.csv")
    )

    league_dataset_dict = {}

    time_series_data_dict: Dict[str, list] = {
        stat: [] for stat in TIME_SERIES_VARIABLES_LIST
    }

    for file in filenames:

        date = file.split("_")[2]
        date_df = pd.read_csv(file)
        date_df.rename(columns={"playername": "name"}, inplace=True)

        if DATASET_NAME == "league_standings":
            date_df["win-total-ratio"] = date_df["w"] / (date_df["w"] + date_df["l"])

        league_dataset_dict[date] = date_df

        for stat in TIME_SERIES_VARIABLES_LIST:
            time_series_data_dict[stat].append(
                sorting_and_index_reset(date_df[["name", stat]], date)
            )

    time_series_dict = {}

    for key in time_series_data_dict.keys():
        time_series_dict[key] = pd.concat(time_series_data_dict[key]).drop(
            columns=["index"]
        )
        time_series_dict[key].index = pd.to_datetime(time_series_dict[key].index)

    return (league_dataset_dict, time_series_dict)


def sort_dataframe_by_largest_values(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts dataframe by the largest values of the last row.

    Returns:
        pd.DataFrame: _description_

    """
    return input_df[
        input_df.columns[input_df.loc[input_df.last_valid_index()].argsort()][::-1]  # type: ignore
    ]


def time_series_stats_plotter(team_stats: bool = True):
    """Produces time series plots for either team or player stats."""

    if team_stats:
        DATASET_NAME = "league_standings"
        time_series_stats_list = ["w", "win-total-ratio"]
        time_series_stats_int_dict = {"w": True, "win-total-ratio": False}
        N_BEST = -1

    else:
        DATASET_NAME = "batter_stats"
        time_series_stats_list = ["homeRuns", "obp"]
        time_series_stats_int_dict = {"homeRuns": True, "obp": False}
        N_BEST = 10

    _, time_series_stats_dict = creating_time_series(
        DATASET_NAME, LEAGUE_NAME, time_series_stats_list
    )

    for stat in time_series_stats_dict:

        time_series_stat_df = time_series_stats_dict[stat].dropna(axis=1)

        if time_series_stats_int_dict[stat]:
            time_series_stat_df.astype(int)

        fig, ax = plt.subplots(figsize=(20, 6))
        sort_dataframe_by_largest_values(time_series_stat_df).iloc[:, :N_BEST].plot(
            ax=ax, style=".", linewidth=5, markersize=12, xlabel="", alpha=1
        )
        ax.set_ylabel(stat)
        ax.yaxis.set_major_locator(
            MaxNLocator(integer=time_series_stats_int_dict[stat])
        )
        ax.legend(
            loc="upper center",
            ncol=4,
            fontsize=12,
            bbox_to_anchor=(0.5, -0.2),
            frameon=False,
        )
        OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"
        plt.savefig(
            OUTPUT_FILE_LOCATION
            + OUTPUT_DETAILS
            + f"_time_series_{DATASET_NAME}_{stat}.png",
            bbox_inches="tight",
        )
        # plt.show()


time_series_stats_plotter(team_stats=True)
time_series_stats_plotter(team_stats=False)
