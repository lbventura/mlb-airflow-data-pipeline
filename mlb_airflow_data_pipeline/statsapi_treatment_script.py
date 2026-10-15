import argparse
from contextlib import closing
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd
from pydantic import BaseModel

from mlb_airflow_data_pipeline.db_utils import (
    read_connection,
    database_path,
    read_player_stats,
)
from mlb_airflow_data_pipeline.treated_storage import (
    ensure_treated_tables,
    save_treated_category,
    validate_scope,
)
from mlb_airflow_data_pipeline.statsapi_extraction_script import DATE_TIME_EXECUTION
from mlb_airflow_data_pipeline.statsapi_feature_utils import (
    create_babip,
    create_dif_strike_outs_base_on_balls,
    create_innings_pitched_normalization,
    create_mean_normalization,
    create_plate_appearance_normalization,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    BATTING_SOURCE_STATS,
    DEFENDER_POSITIONS,
    DEFENSIVE_STATS,
    FIELDING_SOURCE_STATS,
    LEAGUE_NAME,
    PITCHING_STATS,
    PITCHING_SOURCE_STATS,
    PLAYER_INFORMATION,
    expected_output_columns,
)

from mlb_airflow_data_pipeline.logging_setup import get_logger

from mlb_airflow_data_pipeline.treated_schema import (
    BATTER_PLATE_STATS,
    BATTER_MEAN_STATS,
    PITCHER_INNINGS_STATS,
    PITCHER_MEAN_STATS,
    DEFENDER_MEAN_STATS,
)

logger = get_logger("statsapi_treatment")


DATA_FILTER_THRESHOLD = 0.6


class TreatmentDataPaths(BaseModel):
    """SQLite source and league/date selection for treatment."""

    database_path: Path
    league_name: str
    execution_date: str


class DataTreaterInputRepresentation(BaseModel):
    subset_columns: list
    filter_conditions_dict: dict
    transformation_dict: dict
    stats_group: str


def _convert_numeric_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert known metrics, treating undefined API rates as missing values."""
    numeric_columns = (
        (set(expected_output_columns()) - {"playername", "date"})
        | BATTING_SOURCE_STATS
        | PITCHING_SOURCE_STATS
        | FIELDING_SOURCE_STATS
        | {"player_id"}
    )
    for column in dataframe.columns.intersection(numeric_columns):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    return dataframe


class DataTreater:
    def __init__(
        self,
        data_paths: TreatmentDataPaths,
        input_parameters: DataTreaterInputRepresentation,
        input_data: pd.DataFrame | None = None,
    ):
        self.data_paths = data_paths
        self.input_parameters = input_parameters
        self.input_data = input_data

    def get_output_data(self) -> pd.DataFrame:
        filtered_data = self.get_filter_data()

        transformation_dict = self.input_parameters.transformation_dict

        for function in transformation_dict.keys():
            if transformation_dict[function]:
                filtered_data = function(
                    filtered_data,
                    transformation_dict[function],
                )
            else:
                filtered_data = function(filtered_data)

        logger.info(
            "data_transformation_completed",
            transformations_applied=len(
                [k for k, v in transformation_dict.items() if v]
            ),
        )
        return filtered_data

    def get_filter_data(self) -> pd.DataFrame:
        filtered_data = filter_data(
            self.get_subset_data(), self.input_parameters.filter_conditions_dict
        )
        logger.info("data_filtering_completed", filtered_shape=filtered_data.shape)
        return filtered_data

    def get_subset_data(self) -> pd.DataFrame:
        input_data = self.get_input_data()
        columns = list(self.input_parameters.subset_columns)
        if "player_id" in input_data:
            columns.insert(0, "player_id")
        group = self.input_parameters.stats_group
        grouped_columns = [
            column
            for column in input_data.columns
            if column.startswith(("hitting_", "pitching_", "fielding_"))
        ]
        # Old snapshots share the table's new columns, but contain no grouped values.
        has_grouped_stats = input_data[grouped_columns].notna().any(axis=None)
        if has_grouped_stats or (input_data.empty and grouped_columns):
            if group == "fielding":
                intermediate_data = _fielding_subset(input_data, columns)
            else:
                intermediate_data = _grouped_subset(input_data, columns, group)
        else:
            intermediate_data = input_data[columns]
        logger.info(
            "subset_data_created",
            subset_columns=self.input_parameters.subset_columns,
            subset_shape=intermediate_data.shape,
        )
        return intermediate_data

    def get_input_data(self) -> pd.DataFrame:
        if self.input_data is not None:
            return self.input_data.copy()
        with read_connection(self.data_paths.database_path) as conn:
            input_data = read_player_stats(
                conn,
                self.data_paths.league_name,
                self.data_paths.execution_date,
            )
        input_data = _convert_numeric_columns(input_data)
        logger.info(
            "data_input_loaded",
            database_path=self.data_paths.database_path,
            league_name=self.data_paths.league_name,
            execution_date=self.data_paths.execution_date,
            data_shape=input_data.shape,
        )
        return input_data


def _grouped_subset(
    input_data: pd.DataFrame, columns: list[str], group: str
) -> pd.DataFrame:
    source_columns = [
        name if name in [*PLAYER_INFORMATION, "player_id"] else f"{group}_{name}"
        for name in columns
    ]
    if group.startswith("fielding_"):
        # Position-specific metrics (for example catcherERA) are optional.
        selected = input_data.reindex(columns=source_columns)
        identity = [
            name for name in columns if name in [*PLAYER_INFORMATION, "player_id"]
        ]
        selected[identity] = input_data[identity]
    elif not any(column.startswith(f"{group}_") for column in input_data.columns):
        return pd.DataFrame(columns=columns, dtype=float)
    else:
        selected = input_data[source_columns].copy()
    selected.columns = columns
    return selected


def _fielding_subset(input_data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    batting_columns = [
        name for name in columns if name in {"atBats", "plateAppearances"}
    ]
    player_positions = []
    for position in DEFENDER_POSITIONS:
        group = f"fielding_{position}"
        if f"{group}_gamesPlayed" not in input_data.columns:
            continue
        players = input_data[input_data[f"{group}_gamesPlayed"].notna()]
        if players.empty:
            continue
        selected = _grouped_subset(players, columns, group)
        selected[batting_columns] = _grouped_subset(players, batting_columns, "hitting")
        selected["position"] = position
        player_positions.append(selected)
    if not player_positions:
        return pd.DataFrame(columns=[*columns, "position"], dtype=float)
    return pd.concat(player_positions, ignore_index=True)


def filter_data(input_df: pd.DataFrame, conditions_dict: dict) -> pd.DataFrame:
    """Filter data from the input DataFrame as specified by the conditions.
    Raises a warning at runtime if the percentage of good data is below a certain threshold

    Args:
        input_df (pd.DataFrame)
        conditions_dict (dict)

    Returns:
        pd.DataFrame
    """
    if input_df.empty:
        return input_df.copy()
    conditions = np.logical_and.reduce(
        [(input_df[key] >= value) for key, value in conditions_dict.items()]
    )

    return_df = input_df[conditions].copy()

    good_data_percentage = len(return_df) / len(input_df)

    logger.info(
        "data_quality_check",
        good_data_percentage=round(good_data_percentage, 3),
        records_kept=len(return_df),
        records_total=len(input_df),
        filter_conditions=conditions_dict,
    )

    if good_data_percentage < DATA_FILTER_THRESHOLD:
        logger.warning(
            "data_quality_below_threshold",
            good_data_percentage=good_data_percentage,
            threshold=DATA_FILTER_THRESHOLD,
            filter_conditions=conditions_dict,
        )

    return return_df


# setting up information for the batter extraction
batting_stats_list = PLAYER_INFORMATION + BATTING_STATS
batter_filter_conditions_dict = {"plateAppearances": 100, "atBats": 50}


batter_transformation_dict = {
    create_babip: None,
    create_dif_strike_outs_base_on_balls: None,
    create_plate_appearance_normalization: BATTER_PLATE_STATS,
    create_mean_normalization: BATTER_MEAN_STATS,
}

batter_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=batting_stats_list,
    filter_conditions_dict=batter_filter_conditions_dict,
    transformation_dict=batter_transformation_dict,
    stats_group="hitting",
)

# same thing for pitchers
pitching_stats_list = PLAYER_INFORMATION + PITCHING_STATS
pitcher_filter_conditions_dict = {"gamesPlayed": 5, "inningsPitched": 15}


pitcher_transformation_dict = {
    create_innings_pitched_normalization: PITCHER_INNINGS_STATS,
    create_mean_normalization: PITCHER_MEAN_STATS,
}

pitcher_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=pitching_stats_list,
    filter_conditions_dict=pitcher_filter_conditions_dict,
    transformation_dict=pitcher_transformation_dict,
    stats_group="pitching",
)

# same thing for defenders
defending_stats_list = PLAYER_INFORMATION + DEFENSIVE_STATS

defender_filter_conditions_dict = {
    "plateAppearances": 100,
    "atBats": 50,
}  # this likely needs to be augmented in light of the analysis for batters


defender_transformation_dict = {
    create_mean_normalization: DEFENDER_MEAN_STATS,
}

defender_input_data_repr = DataTreaterInputRepresentation(
    subset_columns=defending_stats_list,
    filter_conditions_dict=defender_filter_conditions_dict,
    transformation_dict=defender_transformation_dict,
    stats_group="fielding",
)


def _read_treatment_snapshot(
    conn: sqlite3.Connection, paths: TreatmentDataPaths
) -> tuple[pd.DataFrame, str]:
    """Load a keyed source snapshot and identify its statistics layout."""
    source = _convert_numeric_columns(
        read_player_stats(conn, paths.league_name, paths.execution_date)
    )
    scope = f"{paths.league_name}/{paths.execution_date}"
    if source.empty:
        raise ValueError(f"No source snapshot: {scope}")
    keys = ["team_id", "player_id"]
    if not set(keys).issubset(source.columns):
        raise ValueError(f"Invalid source identity: {scope}")
    if source[keys].isna().any(axis=None) or source.duplicated(keys).any():
        raise ValueError(f"Invalid source identity: {scope}")
    grouped = source.columns.str.startswith(("hitting_", "pitching_", "fielding_"))
    layout = (
        "grouped"
        if source.loc[:, grouped].notna().any(axis=None)
        else "legacy_unprefixed"
    )
    return source, layout


def treat_player_stats(
    database_path: Path,
    league_name: str,
    execution_date: str,
) -> dict[str, int]:
    """Transform one source snapshot and publish all three categories atomically."""
    validate_scope(league_name, execution_date)
    paths = TreatmentDataPaths(
        database_path=database_path,
        league_name=league_name,
        execution_date=execution_date,
    )
    configs = {
        "batter": batter_input_data_repr,
        "pitcher": pitcher_input_data_repr,
        "defender": defender_input_data_repr,
    }
    source_uri = paths.database_path.resolve().as_uri() + "?mode=rw"
    with closing(sqlite3.connect(source_uri, uri=True)) as conn, conn:
        # Lock the source until all derived categories have been published.
        conn.execute("BEGIN IMMEDIATE")
        source, layout = _read_treatment_snapshot(conn, paths)
        outputs = {
            category: DataTreater(paths, config, source).get_output_data()
            for category, config in configs.items()
        }
        ensure_treated_tables(conn)
        for category, frame in outputs.items():
            save_treated_category(
                conn, category, league_name, execution_date, frame, layout
            )

    counts = {category: len(frame) for category, frame in outputs.items()}
    logger.info(
        "treatment_completed",
        league_name=league_name,
        date=execution_date,
        row_counts=counts,
    )
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Store treated player statistics in SQLite"
    )
    parser.add_argument("--date", default=DATE_TIME_EXECUTION)
    parser.add_argument("--league_name", default=LEAGUE_NAME)
    parser.add_argument("--database", type=Path, default=database_path())
    args = parser.parse_args()
    treat_player_stats(args.database, args.league_name, args.date)
