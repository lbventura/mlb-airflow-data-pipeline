"""Scoped report inputs and comparisons with explicit player identity."""

from datetime import date, timedelta
import os
import sqlite3

import pandas as pd

from mlb_airflow_data_pipeline.db_utils import (
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
    comparison_date = (
        date.fromisoformat(execution_date) - timedelta(days=10)
    ).isoformat()
    if comparison_date[:4] != execution_date[:4]:
        return current, None
    try:
        previous = read_treated_stats(conn, "batter", league, comparison_date)
    except SnapshotUnavailable:
        previous = None
    return current, previous


def batter_difference(
    current: pd.DataFrame, previous: pd.DataFrame | None, metric: str
) -> pd.DataFrame | None:
    """Join compatible snapshots by player/team, preserving names only as labels."""
    if previous is None or current.empty or previous.empty:
        return None
    if set(current.layout) != set(previous.layout):
        return None
    modern = current.player_id.notna().all() and previous.player_id.notna().all()
    legacy = current.player_id.isna().all() and previous.player_id.isna().all()
    if not modern and not legacy:
        return None
    keys = ["team_id", "player_id" if modern else "playername"]
    if current.duplicated(keys).any() or previous.duplicated(keys).any():
        raise ValueError("Ambiguous player identity in report comparison")
    left = current[list(dict.fromkeys([*keys, "playername", metric]))]
    merged = left.merge(
        previous[[*keys, metric]], on=keys, suffixes=("TW", "LW"), validate="one_to_one"
    )
    merged[metric + "difference"] = merged[metric + "TW"] - merged[metric + "LW"]
    return merged


def team_metric_averages(batters: pd.DataFrame) -> pd.DataFrame:
    """Average statistic columns only; never include IDs or import metadata."""
    columns = [c for c in metric_types("batter") if c in batters]
    return batters.groupby("team_id")[columns].mean()
