import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import pandas as pd

matplotlib.use("Agg")

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATE_TIME_EXECUTION,
    OUTPUT_FILE_LOCATION,
    LEAGUE_NAME,
)
from mlb_airflow_data_pipeline.db_utils import database_path, read_connection
from mlb_airflow_data_pipeline.treated_storage import read_treated_stats

from mlb_airflow_data_pipeline.logging_setup import get_logger

logger = get_logger("statsapi_analysis")


def create_category_plots(
    database: Path,
    league: str,
    execution_date: str,
    category: str,
    variable_pairs: list[tuple[str, str]],
    output_directory: str = OUTPUT_FILE_LOCATION,
) -> None:
    """Read a completed category snapshot and save its scatter plots."""
    with read_connection(database) as conn:
        input_data = read_treated_stats(conn, category, league, execution_date)

    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)
    for variables in variable_pairs:
        figure = scatter_plot(input_data, variables)
        path = (
            output_path / f"{league}_{execution_date}_{variables[0]}_{variables[1]}.png"
        )
        figure.savefig(path)
        plt.close(figure)
        logger.info(
            "plot_created",
            file_path=str(path),
            league=league,
            date=execution_date,
        )


def scatter_plot(input_df: pd.DataFrame, variable_pair: tuple[str, str]) -> Figure:
    """Create a scatter plot for two variables; the caller closes the figure."""

    x_var, y_var = variable_pair
    input_df = input_df.dropna(subset=[x_var, y_var])

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
    return fig


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


def create_analysis_plots(
    database: Path,
    league: str,
    execution_date: str,
    output_directory: str = OUTPUT_FILE_LOCATION,
) -> None:
    """Create each category's scatter plots from its completed snapshot."""
    for category, variables in (
        ("batter", batter_tuple_variable_list),
        ("pitcher", pitcher_tuple_variable_list),
    ):
        create_category_plots(
            database, league, execution_date, category, variables, output_directory
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot treated database statistics")
    parser.add_argument("--database", type=Path, default=database_path())
    parser.add_argument("--league_name", default=LEAGUE_NAME)
    parser.add_argument("--date", default=DATE_TIME_EXECUTION)
    args = parser.parse_args()
    create_analysis_plots(args.database, args.league_name, args.date)
