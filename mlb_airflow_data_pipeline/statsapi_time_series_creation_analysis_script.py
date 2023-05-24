import glob
from datetime import datetime
from pydantic import BaseModel

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MaxNLocator


from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    LEAGUE_NAME_LOCATION,
    OUTPUT_FILE_LOCATION,
)

matplotlib.use("Agg")

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")


with open(LEAGUE_NAME_LOCATION, "r") as text_file:
    LEAGUE_NAME = text_file.readline().strip()


class PlotterInputRepresentation(BaseModel):
    dataset_name: str
    time_series_stats_is_int: dict[str, bool]
    n_best: int


class PlotGenerator:
    def __init__(self, input_parameters: PlotterInputRepresentation):
        self.input_parameters = input_parameters
        self.time_series_datasets: dict[str, list] = {
            stat: [] for stat in self.input_parameters.time_series_stats_is_int.keys()
        }
        self.figure: plt.figure.Figure
        self.axes: plt.axes.Axes

    def get_time_series_stats_plots(self):
        """Produces time series plots for either team or player stats."""

        time_series = self.get_time_series()

        for stat in time_series.keys():

            time_series_stat_df: pd.DataFrame = time_series[stat].fillna(method="ffill")

            is_int = self.input_parameters.time_series_stats_is_int[stat]
            if is_int:
                time_series_stat_df.astype("Int64")

            self._set_time_series_plot(time_series_stat_df, stat)

    def get_time_series(self) -> dict[str, pd.DataFrame]:
        """Imports all the files from a particular dataset, extracting time series
        for the variables specified in TIME_SERIES_VARIABLES_LIST.

        DATASET_NAME can be ["full_player_stats", "league_standings", "batter_stats"].

        """

        filenames = self._get_filenames()

        for file in filenames:
            self._generate_data_from_file(file=file)

        time_series: dict[str, pd.DataFrame] = self._get_time_series()

        return time_series

    def _generate_data_from_file(self, file: str) -> None:

        filename = file.split("/")[-1]
        date = filename.split("_")[2]
        date_df = pd.read_csv(file)
        date_df.rename(columns={"playername": "name"}, inplace=True)

        dataset_name = self.input_parameters.dataset_name
        if dataset_name == "league_standings":
            date_df["win-total-ratio"] = date_df["w"] / (date_df["w"] + date_df["l"])

        stats = self.input_parameters.time_series_stats_is_int.keys()
        for stat in stats:
            self.time_series_datasets[stat].append(
                sorting_and_index_reset(date_df[["name", stat]], date)
            )

    def _get_time_series(self) -> dict[str, pd.DataFrame]:

        time_series: dict = {}

        days = self.time_series_datasets.keys()
        for day in days:
            time_series[day] = pd.concat(self.time_series_datasets[day]).drop(
                columns=["index"]
            )
            time_series[day].index = pd.to_datetime(time_series[day].index)

        return time_series

    def _set_time_series_plot(self, dataframe: pd.DataFrame, stat: str) -> None:

        self.figure, self.axes = plt.subplots(figsize=(20, 6))

        self._generate_figure_from_dataframe(dataframe=dataframe)
        self._format_axes(stat=stat)

        dataset_name = self.input_parameters.dataset_name

        plt.savefig(
            OUTPUT_FILE_LOCATION
            + f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"
            + f"_time_series_{dataset_name}_{stat}.png",
            bbox_inches="tight",
        )

    def _generate_figure_from_dataframe(self, dataframe: pd.DataFrame):
        sort_dataframe_by_largest_values(dataframe).iloc[
            :, : self.input_parameters.n_best
        ].plot(ax=self.axes, style=".", linewidth=5, markersize=12, xlabel="", alpha=1)

    def _format_axes(self, stat: str):
        self.axes.set_ylabel(stat)
        self.axes.yaxis.set_major_locator(
            MaxNLocator(integer=self.input_parameters.time_series_stats_is_int[stat])
        )
        self.axes.legend(
            loc="upper center",
            ncol=4,
            fontsize=12,
            bbox_to_anchor=(0.5, -0.2),
            frameon=False,
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
        date (str)
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
        pd.DataFrame

    """
    return input_df[
        input_df.columns[input_df.loc[input_df.last_valid_index()].argsort()][::-1]  # type: ignore
    ]


league_input_parameters = PlotterInputRepresentation(
    dataset_name="league_standings",
    time_series_stats_is_int={"w": True, "win-total-ratio": False},
    n_best=-1,
)


batter_input_parameters = PlotterInputRepresentation(
    dataset_name="batter_stats",
    time_series_stats_is_int={"homeRuns": True, "obp": False},
    n_best=10,
)


if __name__ == "__main__":
    league_plot_generator = PlotGenerator(input_parameters=league_input_parameters)
    league_plot_generator.get_time_series_stats_plots()
    batter_plot_generator = PlotGenerator(input_parameters=batter_input_parameters)
    batter_plot_generator.get_time_series_stats_plots()
