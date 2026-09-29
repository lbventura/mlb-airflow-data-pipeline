"""Integration coverage for the extraction-to-treatment SQLite handoff."""

from pathlib import Path

import pandas as pd

from mlb_airflow_data_pipeline.db_utils import create_connection, save_extraction_run
from mlb_airflow_data_pipeline.statsapi_treatment_script import treat_player_stats


def test_treatment_reads_scoped_player_stats_and_writes_outputs(
    tmp_path: Path,
) -> None:
    league_name = "national_league"
    execution_date = "2026-01-01"
    player_stats = pd.read_csv(
        Path(__file__).parents[1]
        / "unit"
        / "national_league_example_full_player_stats_df.csv",
        index_col=0,
    )
    standings = pd.DataFrame({"team_id": player_stats.team_id.unique()})

    database_path = tmp_path / "mlb_data.db"
    with create_connection(str(database_path)) as conn:
        save_extraction_run(conn, league_name, execution_date, standings, player_stats)
        other_players = player_stats.assign(playername="Other league")
        save_extraction_run(
            conn, "american_league", execution_date, standings, other_players
        )
        save_extraction_run(
            conn,
            league_name,
            "2025-12-31",
            standings,
            player_stats.assign(playername="Other date"),
        )

    treat_player_stats(str(database_path), league_name, execution_date, str(tmp_path))

    for player_type in ("batter", "pitcher", "defender"):
        output_path = (
            tmp_path / f"{league_name}_{execution_date}_{player_type}_stats_df.csv"
        )
        output_data = pd.read_csv(output_path, index_col=0)
        assert not output_data.empty
        assert "playername" in output_data.columns
        assert not output_data.playername.isin(["Other league", "Other date"]).any()
