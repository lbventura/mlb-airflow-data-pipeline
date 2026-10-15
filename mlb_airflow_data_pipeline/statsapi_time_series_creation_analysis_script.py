import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator
import pandas as pd

from mlb_airflow_data_pipeline.db_utils import database_path, read_connection
from mlb_airflow_data_pipeline.treated_storage import (
    read_treated_history,
    validate_scope,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATE_TIME_EXECUTION,
    LEAGUE_NAME,
    OUTPUT_FILE_LOCATION,
)

matplotlib.use("Agg")


@dataclass(kw_only=True)
class PlotterInputRepresentation:
    dataset_name: str
    time_series_stats_is_int: dict[str, bool]
    n_best: int


class PlotGenerator:
    def __init__(
        self,
        input_parameters: PlotterInputRepresentation,
        database: Path,
        league: str,
        execution_date: str,
        output_directory: str = OUTPUT_FILE_LOCATION,
    ):
        self.input_parameters = input_parameters
        self.database = database
        self.league = league
        self.execution_date = execution_date
        self.output_directory = Path(output_directory)
        validate_scope(league, execution_date)
        if input_parameters.dataset_name not in ("league_standings", "batter_stats"):
            raise ValueError(
                f"Unsupported time series: {input_parameters.dataset_name}"
            )

    def get_history(self) -> pd.DataFrame:
        """Read only the requested league/calendar year through the report date."""
        start_date = self.execution_date[:4] + "-01-01"
        with read_connection(self.database) as conn:
            if self.input_parameters.dataset_name == "league_standings":
                history = pd.read_sql_query(
                    """
                    SELECT * FROM league_standings
                    WHERE league_name = ? AND date BETWEEN ? AND ?
                    ORDER BY date, team_id
                    """,
                    conn,
                    params=(self.league, start_date, self.execution_date),
                )
            else:
                history = read_treated_history(
                    conn, "batter", self.league, start_date, self.execution_date
                )
        return history

    def get_time_series(self) -> dict[str, pd.DataFrame]:
        """Reshape history into one date-by-entity dataframe for each statistic."""
        history = self.get_history()
        series = {
            stat: pd.DataFrame()
            for stat in self.input_parameters.time_series_stats_is_int
        }
        if not history.empty:
            history = _label_history(history, self.input_parameters.dataset_name)
            for stat in series:
                series[stat] = (
                    history.pivot(index="date", columns="series", values=stat)
                    .sort_index()
                    .rename_axis(index=None, columns=None)
                )
        return series

    def get_time_series_stats_plots(self) -> None:
        """Render and save each statistic's chart."""
        self.output_directory.mkdir(parents=True, exist_ok=True)
        for stat, frame in self.get_time_series().items():
            figure = self._create_figure(stat, frame)
            path = (
                self.output_directory
                / f"{self.league}_{self.execution_date}_time_series_{self.input_parameters.dataset_name}_{stat}.png"
            )
            figure.savefig(path, bbox_inches="tight")
            plt.close(figure)

    def _create_figure(self, stat: str, frame: pd.DataFrame) -> Figure:
        """Plot ranked series, or an explicit message when history is empty."""
        figure, axes = plt.subplots(figsize=(20, 6))
        if frame.empty:
            axes.text(
                0.5,
                0.5,
                "No saved history available",
                ha="center",
                transform=axes.transAxes,
            )
        else:
            frame = frame.copy()
            frame.index = pd.to_datetime(frame.index)
            ordered = sort_dataframe_by_largest_values(frame.ffill())
            if self.input_parameters.n_best > 0:
                ordered = ordered.iloc[:, : self.input_parameters.n_best]
            ordered.plot(
                ax=axes, style=".", linewidth=5, markersize=12, xlabel="", alpha=1
            )
            axes.legend(
                loc="upper center",
                ncol=4,
                fontsize=12,
                bbox_to_anchor=(0.5, -0.2),
                frameon=False,
            )
        axes.set_ylabel(stat)
        axes.yaxis.set_major_locator(
            MaxNLocator(integer=self.input_parameters.time_series_stats_is_int[stat])
        )
        return figure


def _label_history(history: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """Label each stable identity with its latest name in the ordered history."""
    history = history.copy()
    if dataset_name == "league_standings":
        history["entity"] = history.team_id.astype(str)
        history["win-total-ratio"] = history.w / (history.w + history.l)
        label = _team_label
    else:
        history["entity"] = history.apply(_player_identity, axis=1)
        label = _player_label

    latest = history.drop_duplicates("entity", keep="last")
    labels = {row.entity: label(row) for _, row in latest.iterrows()}
    history["series"] = history.entity.map(labels)
    return history


def _team_label(row: pd.Series) -> str:
    return f"{row['name']} [team {int(row.team_id)}]"


def _player_identity(row: pd.Series) -> str:
    return f"{row.layout}:{int(row.team_id)}:{int(row.player_id)}"


def _player_label(row: pd.Series) -> str:
    return f"{row.playername} [player {int(row.player_id)}, team {int(row.team_id)}, {row.layout}]"


def sort_dataframe_by_largest_values(input_df: pd.DataFrame) -> pd.DataFrame:
    """Rank series by their last day's value, placing missing values last."""
    return input_df[
        input_df.iloc[-1].sort_values(ascending=False, na_position="last").index
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=database_path())
    parser.add_argument("--league_name", default=LEAGUE_NAME)
    parser.add_argument("--date", default=DATE_TIME_EXECUTION)
    args = parser.parse_args()
    for config in (league_input_parameters, batter_input_parameters):
        PlotGenerator(
            config, args.database, args.league_name, args.date
        ).get_time_series_stats_plots()
