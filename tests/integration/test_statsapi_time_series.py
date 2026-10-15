from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from mlb_airflow_data_pipeline import statsapi_treatment_script as treatment
from mlb_airflow_data_pipeline.treated_storage import save_treated_category
from mlb_airflow_data_pipeline.statsapi_time_series_creation_analysis_script import (
    PlotGenerator,
    batter_input_parameters,
    league_input_parameters,
)

from .database_helpers import DATE, LEAGUE, extract


def test_history_bounds_names_and_layout_identity(
    database: Path, players: pd.DataFrame
) -> None:
    for date in ("2025-12-31", "2026-10-01", DATE, "2026-10-12"):
        extract(
            database,
            players.assign(
                playername="Earlier Name" if date == "2026-10-01" else "Same Name"
            ),
            date=date,
        )
        treatment.treat_player_stats(database, LEAGUE, date)
    generator = PlotGenerator(batter_input_parameters, database, LEAGUE, DATE)
    frame = generator.get_time_series()["homeRuns"]
    assert frame.index.tolist() == ["2026-10-01", DATE]
    assert len(frame.columns) == 2
    assert all("Same Name" in name for name in frame.columns)
    assert not frame.isna().any(axis=None)


def test_empty_history_generates_explicit_empty_charts(
    database: Path, tmp_path: Path
) -> None:
    generator = PlotGenerator(
        batter_input_parameters,
        database,
        LEAGUE,
        DATE,
        str(tmp_path / "empty-plots"),
    )
    assert all(frame.empty for frame in generator.get_time_series().values())
    generator.get_time_series_stats_plots()
    assert len(list((tmp_path / "empty-plots").glob("*.png"))) == 2


@pytest.mark.parametrize(
    "config",
    [league_input_parameters, batter_input_parameters],
    ids=["standings", "batters"],
)
def test_history_is_scoped_to_league_and_calendar_year(
    database: Path, players: pd.DataFrame, config
) -> None:
    for date in ("2025-12-31", "2026-10-01", DATE, "2026-10-12"):
        for league in (LEAGUE, "national_league"):
            extract(database, players, league, date)
            treatment.treat_player_stats(database, league, date)
    history = PlotGenerator(config, database, LEAGUE, DATE).get_history()
    assert set(history.league_name) == {LEAGUE}
    assert sorted(history.date.unique()) == ["2026-10-01", DATE]


def test_standings_use_team_ids_and_latest_names(
    database: Path, players: pd.DataFrame
) -> None:
    extract(database, players, date="2026-10-01")
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE league_standings SET name = 'Earlier Team', w = 10, l = 10 WHERE date = '2026-10-01'"
        )
        conn.execute(
            "UPDATE league_standings SET name = 'Current Team', w = 60, l = 40 WHERE date = ?",
            (DATE,),
        )
    series = PlotGenerator(
        league_input_parameters, database, LEAGUE, DATE
    ).get_time_series()
    assert series["w"].to_dict() == {
        "Current Team [team 147]": {"2026-10-01": 10, DATE: 60}
    }
    assert series["win-total-ratio"].iloc[:, 0].tolist() == [0.5, 0.6]


def test_transferred_players_have_separate_team_series(
    database: Path, players: pd.DataFrame
) -> None:
    extract(database, players, date="2026-10-01")
    treatment.treat_player_stats(database, LEAGUE, "2026-10-01")
    extract(database, players.assign(team_id=[119, 147]))
    treatment.treat_player_stats(database, LEAGUE, DATE)
    frame = PlotGenerator(
        batter_input_parameters, database, LEAGUE, DATE
    ).get_time_series()["homeRuns"]
    assert frame.shape == (2, 3)
    assert frame.filter(like="player 101, team 147").iloc[:, 0].tolist()[0] == 1
    assert pd.isna(frame.filter(like="player 101, team 147").iloc[1, 0])
    assert pd.isna(frame.filter(like="player 101, team 119").iloc[0, 0])
    assert frame.filter(like="player 101, team 119").iloc[1, 0] == 1


def test_legacy_and_grouped_history_keep_distinct_series(
    published_database: Path,
) -> None:
    historical = pd.DataFrame(
        {
            "team_id": [147, 147],
            "player_id": [101, 102],
            "playername": ["Historical One", "Historical Two"],
            "homeRuns": [3, 4],
            "obp": [0.2, 0.3],
        }
    )
    with sqlite3.connect(published_database) as conn:
        save_treated_category(
            conn,
            "batter",
            LEAGUE,
            "2026-10-01",
            historical,
            "legacy_unprefixed",
        )
    frame = PlotGenerator(
        batter_input_parameters, published_database, LEAGUE, DATE
    ).get_time_series()["homeRuns"]
    assert frame.shape == (2, 4)
    legacy = frame.filter(like="legacy_unprefixed")
    grouped = frame.filter(like="grouped")
    pd.testing.assert_frame_equal(
        legacy,
        pd.DataFrame(
            [[3.0, 4.0], [float("nan"), float("nan")]],
            index=["2026-10-01", DATE],
            columns=legacy.columns,
        ),
    )
    pd.testing.assert_frame_equal(
        grouped,
        pd.DataFrame(
            [[float("nan"), float("nan")], [1.0, 1.0]],
            index=["2026-10-01", DATE],
            columns=grouped.columns,
        ),
    )


def test_history_omits_invalidated_treatment(
    published_database: Path, players: pd.DataFrame
) -> None:
    extract(published_database, players, date="2026-10-01")
    treatment.treat_player_stats(published_database, LEAGUE, "2026-10-01")
    extract(published_database, players)
    history = PlotGenerator(
        batter_input_parameters, published_database, LEAGUE, DATE
    ).get_history()
    assert history.date.unique().tolist() == ["2026-10-01"]


def test_completed_empty_snapshots_have_no_player_series(
    database: Path, players: pd.DataFrame
) -> None:
    extract(database, players.assign(hitting_atBats=0, pitching_gamesPlayed=0))
    treatment.treat_player_stats(database, LEAGUE, DATE)
    generator = PlotGenerator(batter_input_parameters, database, LEAGUE, DATE)
    assert generator.get_history().empty
    assert all(frame.empty for frame in generator.get_time_series().values())


def test_history_detects_inconsistent_completion_count(
    published_database: Path,
) -> None:
    with sqlite3.connect(published_database) as conn:
        conn.execute("DELETE FROM batter_stats WHERE player_id = 101")
    with pytest.raises(ValueError, match="row count mismatch"):
        PlotGenerator(
            batter_input_parameters, published_database, LEAGUE, DATE
        ).get_history()
