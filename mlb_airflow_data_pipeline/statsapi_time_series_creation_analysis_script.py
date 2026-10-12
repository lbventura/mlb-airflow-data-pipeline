"""Generate ordered, identity-preserving time series from SQLite snapshots."""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import pandas as pd
from pydantic import BaseModel

from mlb_airflow_data_pipeline.db_utils import (
    database_path,
    read_connection,
    read_treated_stats,
    validate_scope,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    DATE_TIME_EXECUTION,
    LEAGUE_NAME,
    OUTPUT_FILE_LOCATION,
)


class PlotterInputRepresentation(BaseModel):
    dataset_name: str
    time_series_stats_is_int: dict[str, bool]
    n_best: int


class PlotGenerator:
    def __init__(
        self,
        input_parameters: PlotterInputRepresentation,
        database: str | Path,
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
        start = self.execution_date[:4] + "-01-01"
        with read_connection(self.database) as conn:
            if self.input_parameters.dataset_name == "league_standings":
                return pd.read_sql_query(
                    """SELECT * FROM league_standings
                    WHERE league_name = ? AND date BETWEEN ? AND ? ORDER BY date, team_id""",
                    conn,
                    params=(self.league, start, self.execution_date),
                )
            has_snapshots = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='treated_snapshots'"
            ).fetchone()
            if not has_snapshots:
                return pd.DataFrame()
            dates = conn.execute(
                """SELECT date FROM treated_snapshots
                WHERE league_name = ? AND category = 'batter' AND date BETWEEN ? AND ? ORDER BY date""",
                (self.league, start, self.execution_date),
            ).fetchall()
            frames = [
                read_treated_stats(conn, "batter", self.league, day) for (day,) in dates
            ]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_time_series(self) -> dict[str, pd.DataFrame]:
        """Keep IDs and layout in series identity; names serve as display labels."""
        history = self.get_history()
        stats = self.input_parameters.time_series_stats_is_int
        if history.empty:
            return {stat: pd.DataFrame() for stat in stats}
        history = history.copy()
        if self.input_parameters.dataset_name == "league_standings":
            history["entity"] = history.team_id.astype(str)
            latest = history.drop_duplicates("entity", keep="last")
            labels = {
                row.entity: f"{row['name']} [team {int(row.team_id)}]"
                for _, row in latest.iterrows()
            }
            history["series"] = history.entity.map(labels)
            history["win-total-ratio"] = history.w / (history.w + history.l)
        else:
            history["entity"] = history.apply(_player_identity, axis=1)
            latest = history.drop_duplicates("entity", keep="last")
            labels = {row.entity: _player_label(row) for _, row in latest.iterrows()}
            history["series"] = history.entity.map(labels)
        return {
            stat: history.pivot(index="date", columns="series", values=stat)
            .sort_index()
            .rename_axis(index=None, columns=None)
            for stat in stats
        }

    def get_time_series_stats_plots(self) -> None:
        self.output_directory.mkdir(parents=True, exist_ok=True)
        for stat, frame in self.get_time_series().items():
            fig, ax = plt.subplots(figsize=(20, 6))
            if frame.empty:
                ax.text(
                    0.5,
                    0.5,
                    "No saved history available",
                    ha="center",
                    transform=ax.transAxes,
                )
            else:
                frame.index = pd.to_datetime(frame.index)
                ordered = sort_dataframe_by_largest_values(frame.ffill())
                if self.input_parameters.n_best > 0:
                    ordered = ordered.iloc[:, : self.input_parameters.n_best]
                ordered.plot(
                    ax=ax, style=".", linewidth=5, markersize=12, xlabel="", alpha=1
                )
                ax.legend(
                    loc="upper center",
                    ncol=4,
                    fontsize=12,
                    bbox_to_anchor=(0.5, -0.2),
                    frameon=False,
                )
            ax.set_ylabel(stat)
            ax.yaxis.set_major_locator(
                MaxNLocator(
                    integer=self.input_parameters.time_series_stats_is_int[stat]
                )
            )
            path = (
                self.output_directory
                / f"{self.league}_{self.execution_date}_time_series_{self.input_parameters.dataset_name}_{stat}.png"
            )
            fig.savefig(path, bbox_inches="tight")
            plt.close(fig)


def _player_identity(row: pd.Series) -> str:
    player = (
        f"id:{int(row.player_id)}"
        if pd.notna(row.player_id)
        else f"name:{row.playername}"
    )
    return f"{row.layout}:{int(row.team_id)}:{player}"


def _player_label(row: pd.Series) -> str:
    identity = (
        f"player {int(row.player_id)}" if pd.notna(row.player_id) else "historical name"
    )
    return f"{row.playername} [{identity}, team {int(row.team_id)}, {row.layout}]"


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
