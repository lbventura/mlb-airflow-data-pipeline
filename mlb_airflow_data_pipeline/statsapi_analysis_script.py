import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATA_FILE_LOCATION,
    OUTPUT_FILE_LOCATION,
)

from mlb_airflow_data_pipeline.statsapi_treatment_script import (
    OUTPUT_DETAILS,
    BATTER_DATA_FILE_NAME,
)


# helper functions
def scatter_plot(input_df: pd.DataFrame, x_var: str, y_var: str):
    """Create scatter plot for two variables"""

    fig, ax = plt.subplots()
    (
        input_df.plot(
            ax=ax,
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
    plt.savefig(OUTPUT_FILE_LOCATION + OUTPUT_DETAILS + f"_{x_var}_{y_var}.png")


# import data
batting_stats_df = pd.read_csv(DATA_FILE_LOCATION + BATTER_DATA_FILE_NAME, index_col=0)


# generate a few charts to see behavior of the league
tuple_variable_list = [
    ("strikeOutsperplateAppearance", "homeRunsperplateAppearance"),
    ("strikeOutsperplateAppearance", "baseOnBallsperplateAppearance"),
    ("difstrikeOutsbaseOnBallsperplateAppearance", "normalized_obp"),
    ("strikeOutsperplateAppearance", "normalized_ops"),
    ("normalized_avg", "normalized_ops"),
    ("avg_z_score", "ops_z_score"),
    ("hits_z_score", "baseOnBalls_z_score"),
    ("hits_z_score", "totalBases_z_score"),
]


for x_var, y_var in tuple_variable_list:
    scatter_plot(batting_stats_df, x_var, y_var)
