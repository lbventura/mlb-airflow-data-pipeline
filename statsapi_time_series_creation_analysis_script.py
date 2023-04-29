import glob
from datetime import datetime
from pydantic import BaseModel

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MaxNLocator

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")


from statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    LEAGUE_NAME_LOCATION,
    OUTPUT_FILE_LOCATION,
)

matplotlib.use("Agg")

with open(LEAGUE_NAME_LOCATION, "r") as text_file:
    LEAGUE_NAME = text_file.readline().strip()


class PlotterInputRepresentation(BaseModel):
    dataset_name: str
    time_series_stats_list: list[str]
    time_series_stats_int_dict: dict[str, bool]
    n_best: int


class PlotGenerator:
    def __init__(self, input_parameters: PlotterInputRepresentation):
        self.input_parameters = input_parameters
        self.league_dataset_dict: dict = {}
        self.time_series_data_dict: dict[str, list] = {
            stat: [] for stat in self.input_parameters.time_series_stats_list
        }

    def get_time_series_stats_plots(self):
        """Produces time series plots for either team or player stats."""

        self.set_time_series()

        for stat in self.time_series:

            time_series_stat_df: pd.DataFrame = self.time_series[stat].dropna(axis=1)

            if self.input_parameters.time_series_stats_int_dict[stat]:
                time_series_stat_df.astype(int)

            self._set_time_series_plot(time_series_stat_df, stat)

    def set_time_series(self) -> None:
        """Imports all the files from a particular dataset, extracting time series
        for the variables specified in TIME_SERIES_VARIABLES_LIST.

        DATASET_NAME can be ["full_player_stats", "league_standings", "batter_stats"].

        """

        filenames = self._get_filenames()

        for file in filenames:
            self._set_file_data(file=file)

        time_series: dict[str, pd.DataFrame] = self._get_time_series()
        self.time_series = time_series

    def _set_file_data(self, file: str) -> None:

        date = file.split("_")[2]
        date_df = pd.read_csv(file)
        date_df.rename(columns={"playername": "name"}, inplace=True)

        if self.input_parameters.dataset_name == "league_standings":
            date_df["win-total-ratio"] = date_df["w"] / (date_df["w"] + date_df["l"])

        self.league_dataset_dict[date] = date_df

        for stat in self.input_parameters.time_series_stats_list:
            self.time_series_data_dict[stat].append(
                sorting_and_index_reset(date_df[["name", stat]], date)
            )

    def _get_time_series(self) -> dict[str, pd.DataFrame]:

        time_series: dict = {}

        for key in self.time_series_data_dict.keys():
            time_series[key] = pd.concat(self.time_series_data_dict[key]).drop(
                columns=["index"]
            )
            time_series[key].index = pd.to_datetime(time_series[key].index)

        return time_series

    def _set_time_series_plot(self, dataframe: pd.DataFrame, stat: str) -> None:

        fig, ax = plt.subplots(figsize=(20, 6))
        sort_dataframe_by_largest_values(dataframe).iloc[
            :, : self.input_parameters.n_best
        ].plot(ax=ax, style=".", linewidth=5, markersize=12, xlabel="", alpha=1)
        ax.set_ylabel(stat)
        ax.yaxis.set_major_locator(
            MaxNLocator(integer=self.input_parameters.time_series_stats_int_dict[stat])
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
            + f"_time_series_{self.input_parameters.dataset_name}_{stat}.png",
            bbox_inches="tight",
        )

    def _get_filenames(self) -> list[str]:
        filenames = sorted(
            glob.glob(
                DATA_FILE_LOCATION
                + f"{LEAGUE_NAME}_*_{self.input_parameters.dataset_name}_df.csv"
            )
        )
        return filenames


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

    # dropping the duplicate columns
    mod_input_df = mod_input_df.loc[:, ~mod_input_df.columns.duplicated()]  # type: ignore

    mod_input_df["date"] = date
    mod_input_df = mod_input_df.reset_index().set_index("date")

    return mod_input_df


def sort_dataframe_by_largest_values(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts dataframe by the largest values of the last row.

    Returns:
        pd.DataFrame: _description_

    """
    return input_df[
        input_df.columns[input_df.loc[input_df.last_valid_index()].argsort()][::-1]  # type: ignore
    ]


league_input_parameters = PlotterInputRepresentation(
    dataset_name="league_standings",
    time_series_stats_list=["w", "win-total-ratio"],
    time_series_stats_int_dict={"w": True, "win-total-ratio": False},
    n_best=-1,
)


batter_input_parameters = PlotterInputRepresentation(
    dataset_name="batter_stats",
    time_series_stats_list=["homeRuns", "obp"],
    time_series_stats_int_dict={"homeRuns": True, "obp": False},
    n_best=10,
)


if __name__ == "__main__":
    # PlotGenerator(input_parameters=league_input_parameters)
    batter_plot_generator = PlotGenerator(input_parameters=batter_input_parameters)
    batter_plot_generator.get_time_series_stats_plots()
