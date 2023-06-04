import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    OUTPUT_FILE_LOCATION,
)
from mlb_airflow_data_pipeline.statsapi_treatment_script import (
    BATTER_DATA_FILE_NAME,
    OUTPUT_DETAILS,
    PITCHER_DATA_FILE_NAME,
    DataPaths,
)


class DataPlotter:
    def __init__(self, data_paths: DataPaths, representable_variables: list):
        self.data_paths = data_paths
        self.representable_variables = representable_variables

    def set_plots(self) -> None:
        input_data = self.get_input_data()

        for variable_pair in self.representable_variables:
            scatter_plot(input_data, variable_pair)
            plt.savefig(
                self.data_paths.path_to_output_data  # type:ignore
                + f"_{variable_pair[0]}_{variable_pair[1]}.png"
            )

    def get_input_data(self) -> pd.DataFrame:
        input_data = pd.read_csv(self.data_paths.path_to_input_data, index_col=0)
        return input_data


# helper functions
def scatter_plot(input_df: pd.DataFrame, variable_pair: tuple[str, str]):
    """Create scatter plot for two variables"""

    x_var, y_var = variable_pair

    fig, ax = plt.subplots()
    (
        input_df.plot(
            ax=ax,  # type: ignore
            kind="scatter",
            x=x_var,
            y=y_var,
        )
    )
    for i, txt in enumerate(input_df["playername"].values):
        ax.annotate(
            txt.split(" ")[-1],
            (
                input_df[x_var].values[i],
                input_df[y_var].values[i],
            ),  # type: ignore
            fontsize=6,
        )


batter_tuple_variable_list = [
    ("strikeOutsperplateAppearance", "homeRunsperplateAppearance"),
    ("strikeOutsperplateAppearance", "baseOnBallsperplateAppearance"),
    ("difstrikeOutsbaseOnBallsperplateAppearance", "normalized_obp"),
    ("strikeOutsperplateAppearance", "normalized_ops"),
    ("normalized_avg", "normalized_ops"),
    ("avg_z_score", "ops_z_score"),
    ("hits_z_score", "baseOnBalls_z_score"),
    ("hits_z_score", "totalBases_z_score"),
]

pitcher_tuple_variable_list = [
    ("normalized_strikeoutsPer9Inn", "normalized_walksPer9Inn"),
    ("normalized_strikeoutsPer9Inn", "normalized_hitsPer9Inn"),
    ("normalized_strikeoutsPer9Inn", "normalized_homeRunsPer9"),
]


if __name__ == "__main__":

    batter_plots_input_paths = DataPaths(
        path_to_input_data=DATA_FILE_LOCATION + BATTER_DATA_FILE_NAME,
        path_to_output_data=OUTPUT_FILE_LOCATION + OUTPUT_DETAILS,
    )

    batter_plotter = DataPlotter(
        data_paths=batter_plots_input_paths,
        representable_variables=batter_tuple_variable_list,
    )

    batter_plotter.set_plots()

    pitcher_plots_input_paths = DataPaths(
        path_to_input_data=DATA_FILE_LOCATION + PITCHER_DATA_FILE_NAME,
        path_to_output_data=OUTPUT_FILE_LOCATION + OUTPUT_DETAILS,
    )

    pitcher_plotter = DataPlotter(
        data_paths=pitcher_plots_input_paths,
        representable_variables=pitcher_tuple_variable_list,
    )

    pitcher_plotter.set_plots()
