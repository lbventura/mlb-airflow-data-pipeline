from datetime import date, timedelta
import os
import sqlite3

import pandas as pd

from mlb_airflow_data_pipeline.treated_storage import (
    read_treated_stats,
    SnapshotUnavailable,
    validate_scope,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import LEAGUE_NAME
from mlb_airflow_data_pipeline.treated_schema import metric_types


def report_scope() -> tuple[str, str]:
    """Notebook parameters supplied by orchestration; standalone execution defaults."""
    league = os.environ.get("MLB_REPORT_LEAGUE", LEAGUE_NAME)
    execution_date = os.environ.get("MLB_REPORT_DATE", date.today().isoformat())
    validate_scope(league, execution_date)
    return league, execution_date


def read_report_batters(
    conn: sqlite3.Connection, league: str, execution_date: str
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Require current treatment and optionally read the exact ten-day comparison."""
    current = read_treated_stats(conn, "batter", league, execution_date)
    report_date = date.fromisoformat(execution_date)
    comparison_date = report_date - timedelta(days=10)
    previous = None
    if comparison_date.year == report_date.year:
        try:
            previous = read_treated_stats(
                conn, "batter", league, comparison_date.isoformat()
            )
        except SnapshotUnavailable:
            pass
    return current, previous


def batter_difference(
    current: pd.DataFrame, previous: pd.DataFrame | None, metric: str
) -> pd.DataFrame:
    """Compare matching layout/team/player IDs; no comparison gives an empty frame."""
    keys = ["layout", "team_id", "player_id"]
    if previous is None:
        previous = current.iloc[:0]
    difference = current[[*keys, "playername", metric]].merge(
        previous[[*keys, metric]],
        on=keys,
        suffixes=("_current", "_previous"),
        validate="one_to_one",
    )
    difference[metric + "difference"] = (
        difference[metric + "_current"] - difference[metric + "_previous"]
    )
    return difference


def team_metric_averages(batters: pd.DataFrame) -> pd.DataFrame:
    """Average statistic columns only; exclude player and team IDs."""
    columns = [c for c in metric_types("batter") if c in batters]
    return batters.groupby("team_id")[columns].mean()
