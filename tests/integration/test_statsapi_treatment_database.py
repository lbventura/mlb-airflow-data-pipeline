"""Integration coverage for the extraction-to-treatment SQLite handoff."""

from pathlib import Path

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import create_connection, save_extraction_run
from mlb_airflow_data_pipeline.treated_storage import read_treated_stats
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    PITCHING_STATS,
)
from mlb_airflow_data_pipeline.statsapi_treatment_script import (
    DataTreater,
    TreatmentDataPaths,
    batter_input_data_repr,
    defender_input_data_repr,
    pitcher_input_data_repr,
    treat_player_stats,
)


def test_grouped_stats_reach_role_specific_database_outputs(
    tmp_path: Path,
) -> None:
    players = pd.DataFrame(
        [
            {
                "playername": "Player One",
                "team_id": 147,
                **{f"hitting_{stat}": "0" for stat in BATTING_STATS},
                **{f"pitching_{stat}": "0" for stat in PITCHING_STATS},
                "hitting_hits": "40",
                "hitting_homeRuns": "5",
                "hitting_strikeOuts": "20",
                "hitting_sacFlies": "2",
                "hitting_stolenBases": "4",
                "hitting_atBats": "334",
                "hitting_plateAppearances": "388",
                "hitting_gamesPlayed": "123",
                "hitting_intentionalWalks": "1",
                "pitching_gamesPlayed": "75",
                "pitching_intentionalWalks": "3",
                "pitching_hits": "43",
                "pitching_inningsPitched": "20.0",
                "pitching_outs": "60",
                "fielding_C_gamesPlayed": "120",
                "fielding_C_catcherERA": "3.25",
                "fielding_C_assists": "6",
                "fielding_C_rangeFactorPer9Inn": "-.--",
                "fielding_1B_gamesPlayed": "2",
                "fielding_1B_assists": "2",
                "fielding_1B_rangeFactorPer9Inn": "2.50",
                "fielding_P_gamesPlayed": "75",
                "fielding_DH_gamesPlayed": "1",
            }
        ],
        index=pd.Index([101], name="player_id"),
    )
    database_path = tmp_path / "mlb_data.db"
    with create_connection(str(database_path)) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-10-01",
            pd.DataFrame({"team_id": [147]}),
            players,
        )
        for league, date in [
            ("national_league", "2026-10-01"),
            ("american_league", "2026-09-30"),
        ]:
            save_extraction_run(
                conn,
                league,
                date,
                pd.DataFrame({"team_id": [147]}),
                players.assign(playername="Other snapshot"),
            )

    paths = TreatmentDataPaths(
        database_path=database_path,
        league_name="american_league",
        execution_date="2026-10-01",
    )
    batter = DataTreater(paths, batter_input_data_repr).get_subset_data()
    pitcher = DataTreater(paths, pitcher_input_data_repr).get_subset_data()
    defender = DataTreater(paths, defender_input_data_repr).get_subset_data()

    assert batter.loc[0, ["hits", "stolenBases", "gamesPlayed"]].tolist() == [
        40,
        4,
        123,
    ]
    assert pitcher.loc[0, "gamesPlayed"] == 75
    assert batter.loc[0, "intentionalWalks"] == 1
    assert pitcher.loc[0, "intentionalWalks"] == 3
    assert defender.set_index("position")["gamesPlayed"].to_dict() == {
        "C": 120,
        "1B": 2,
    }
    assert defender["atBats"].tolist() == [334, 334]
    assert defender["plateAppearances"].tolist() == [388, 388]
    assert defender.set_index("position").loc["C", "catcherERA"] == 3.25
    assert pd.isna(defender.set_index("position").loc["1B", "catcherERA"])

    treat_player_stats(database_path, "american_league", "2026-10-01")
    with create_connection(str(database_path)) as conn:
        outputs = {
            role: read_treated_stats(conn, role, "american_league", "2026-10-01")
            for role in ("batter", "pitcher", "defender")
        }
    for result in outputs.values():
        assert set(result.playername) == {"Player One"}
    assert outputs["batter"].loc[0, "hitsperplateAppearance"] == pytest.approx(40 / 388)
    assert outputs["batter"].loc[0, "babip"] == pytest.approx(35 / 311)
    assert outputs["pitcher"].loc[0, "intentionalWalksinningsPitched"] == pytest.approx(
        3 / 20
    )
    defenders = outputs["defender"].set_index("position")
    assert defenders.index.tolist() == ["C", "1B"]
    assert defenders.normalized_assists.to_dict() == {"C": 150, "1B": 50}
    assert pd.isna(defenders.loc["C", "rangeFactorPer9Inn"])
    assert defenders.loc["1B", "rangeFactorPer9Inn"] == 2.5


def test_legacy_snapshot_stays_readable_after_grouped_columns_are_added(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "mlb_data.db"
    standings = pd.DataFrame({"team_id": [147]})
    grouped = pd.DataFrame(
        [
            {
                "playername": "New",
                "team_id": 147,
                **{f"hitting_{name}": "0" for name in BATTING_STATS},
                "hitting_hits": "4",
            }
        ],
        index=pd.Index([101], name="player_id"),
    )
    legacy = pd.DataFrame(
        [
            {
                "playername": "Old",
                "team_id": 147,
                **{name: "0" for name in BATTING_STATS},
                "hits": "7",
            }
        ],
        index=pd.Index([101], name="player_id"),
    )
    with create_connection(str(database_path)) as conn:
        save_extraction_run(conn, "american_league", "2026-09-30", standings, legacy)
        save_extraction_run(conn, "american_league", "2026-10-01", standings, legacy)
        save_extraction_run(conn, "american_league", "2026-10-01", standings, grouped)

    paths = TreatmentDataPaths(
        database_path=database_path,
        league_name="american_league",
        execution_date="2026-09-30",
    )
    subset = DataTreater(paths, batter_input_data_repr).get_subset_data()
    assert subset.loc[0, "hits"] == 7
    paths.execution_date = "2026-10-01"
    subset = DataTreater(paths, batter_input_data_repr).get_subset_data()
    assert subset.loc[0, "hits"] == 4
    assert subset.loc[0, "playername"] == "New"


def test_grouped_snapshot_without_pitching_or_defending_writes_empty_role_outputs(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "mlb_data.db"
    players = pd.DataFrame(
        [
            {
                "playername": "Hitter",
                "team_id": 147,
                **{f"hitting_{stat}": 0 for stat in BATTING_STATS},
                "hitting_atBats": 100,
                "hitting_plateAppearances": 110,
                "fielding_DH_gamesPlayed": 25,
            }
        ],
        index=[101],
    )
    with create_connection(str(database_path)) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-10-01",
            pd.DataFrame({"team_id": [147]}),
            players,
        )
    treat_player_stats(database_path, "american_league", "2026-10-01")
    with create_connection(str(database_path)) as conn:
        for role in ("pitcher", "defender"):
            result = read_treated_stats(conn, role, "american_league", "2026-10-01")
            assert result.empty
            assert "gamesPlayed" in result.columns


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

    treat_player_stats(database_path, league_name, execution_date)

    with create_connection(str(database_path)) as conn:
        for category in ("batter", "pitcher", "defender"):
            output = read_treated_stats(conn, category, league_name, execution_date)
            assert not output.empty
            assert "playername" in output.columns
            assert not output.playername.isin(["Other league", "Other date"]).any()
