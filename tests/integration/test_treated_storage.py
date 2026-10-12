"""Treated publication, rollback, invalidation, and source preservation."""

from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from mlb_airflow_data_pipeline import statsapi_treatment_script as treatment
from mlb_airflow_data_pipeline.db_utils import (
    read_connection,
    read_treated_stats,
    save_extraction_run,
    save_treated_category,
    SnapshotUnavailable,
)

from .database_helpers import COUNTS, DATE, LEAGUE, extract, extracted_tables


def test_retry_preserves_other_leagues(
    published_database: Path, players: pd.DataFrame
) -> None:
    extract(published_database, players, "national_league")
    treatment.treat_player_stats(str(published_database), "national_league", DATE)
    assert treatment.treat_player_stats(str(published_database), LEAGUE, DATE) == COUNTS
    with sqlite3.connect(published_database) as conn:
        for league in (LEAGUE, "national_league"):
            for role, count in COUNTS.items():
                assert len(read_treated_stats(conn, role, league, DATE)) == count
    assert not list(published_database.parent.glob("*.csv"))


def test_player_and_fielding_identity_survives_treatment(
    published_database: Path,
) -> None:
    with sqlite3.connect(published_database) as conn:
        assert read_treated_stats(conn, "batter", LEAGUE, DATE).player_id.tolist() == [
            101,
            102,
        ]
        defenders = read_treated_stats(conn, "defender", LEAGUE, DATE)
    assert list(zip(defenders.player_id, defenders.position)) == [
        (101, "C"),
        (102, "C"),
        (101, "1B"),
        (102, "1B"),
    ]


def test_completed_empty_roles_differ_from_missing_snapshot(
    published_database: Path, players: pd.DataFrame
) -> None:
    extract(
        published_database, players.assign(hitting_atBats=0, pitching_gamesPlayed=0)
    )
    assert treatment.treat_player_stats(
        str(published_database), LEAGUE, DATE
    ) == dict.fromkeys(COUNTS, 0)
    with sqlite3.connect(published_database) as conn:
        for role in COUNTS:
            assert read_treated_stats(conn, role, LEAGUE, DATE).empty
            with pytest.raises(SnapshotUnavailable):
                read_treated_stats(conn, role, LEAGUE, "2026-10-10")


@pytest.mark.parametrize("category", ["pitcher", "defender"])
def test_failed_publication_restores_all_categories(
    published_database: Path, category: str
) -> None:
    with sqlite3.connect(published_database) as conn:
        conn.execute(
            f"CREATE TRIGGER reject_role BEFORE INSERT ON {category}_stats BEGIN SELECT RAISE(ABORT, 'rejected role'); END"
        )
        before = list(conn.iterdump())
    with pytest.raises(sqlite3.Error, match="rejected role"):
        treatment.treat_player_stats(str(published_database), LEAGUE, DATE)
    with sqlite3.connect(published_database) as conn:
        assert list(conn.iterdump()) == before


def test_initial_publication_failure_rolls_back_schema(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = treatment.save_treated_category

    def fail_last(conn, category, *args, **kwargs):
        if category == "defender":
            raise RuntimeError("failed final category")
        original(conn, category, *args, **kwargs)

    monkeypatch.setattr(treatment, "save_treated_category", fail_last)
    with sqlite3.connect(database) as conn:
        before = list(conn.iterdump())
    with pytest.raises(RuntimeError, match="final category"):
        treatment.treat_player_stats(str(database), LEAGUE, DATE)
    with sqlite3.connect(database) as conn:
        assert list(conn.iterdump()) == before


def test_missing_source_leaves_database_unchanged(database: Path) -> None:
    with sqlite3.connect(database) as conn:
        before = list(conn.iterdump())
    with pytest.raises(ValueError, match="No source"):
        treatment.treat_player_stats(database, LEAGUE, "2026-10-10")
    with sqlite3.connect(database) as conn:
        assert list(conn.iterdump()) == before


def test_failed_extraction_preserves_treatment(
    published_database: Path, players: pd.DataFrame
) -> None:
    with sqlite3.connect(published_database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_player BEFORE INSERT ON player_stats BEGIN SELECT RAISE(ABORT, 'rejected player'); END"
        )
        before = list(conn.iterdump())
    with pytest.raises(sqlite3.IntegrityError):
        extract(published_database, players)
    with sqlite3.connect(published_database) as conn:
        assert list(conn.iterdump()) == before


def test_source_replacement_invalidates_only_matching_snapshot(
    published_database: Path, players: pd.DataFrame
) -> None:
    other_scopes = [("national_league", DATE), (LEAGUE, "2026-10-01")]
    for league, date in other_scopes:
        extract(published_database, players, league, date)
        treatment.treat_player_stats(published_database, league, date)
    extract(published_database, players)
    with sqlite3.connect(published_database) as conn:
        for category, count in COUNTS.items():
            with pytest.raises(SnapshotUnavailable):
                read_treated_stats(conn, category, LEAGUE, DATE)
            for league, date in other_scopes:
                assert len(read_treated_stats(conn, category, league, date)) == count


def test_treatment_locks_source_until_publication(
    database: Path, players: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    read_snapshot = treatment._read_treatment_snapshot

    def read_while_extraction_competes(conn, paths):
        source = read_snapshot(conn, paths)
        with sqlite3.connect(database, timeout=0) as competing_conn:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                save_extraction_run(
                    competing_conn,
                    LEAGUE,
                    DATE,
                    pd.DataFrame({"team_id": [147]}),
                    players.assign(playername="Replacement"),
                )
        return source

    monkeypatch.setattr(
        treatment, "_read_treatment_snapshot", read_while_extraction_competes
    )
    treatment.treat_player_stats(str(database), LEAGUE, DATE)
    with sqlite3.connect(database) as conn:
        assert set(read_treated_stats(conn, "batter", LEAGUE, DATE).playername) == {
            "Same Name"
        }


def test_missing_database_is_not_created(tmp_path: Path) -> None:
    missing = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        treatment.treat_player_stats(str(missing), LEAGUE, DATE)
    with pytest.raises(sqlite3.OperationalError):
        with read_connection(str(missing)):
            pass
    assert not missing.exists()


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        pytest.param("player_id", None, ValueError, id="missing-player"),
        pytest.param("player_id", 101.5, ValueError, id="fractional-player"),
        pytest.param("team_id", None, ValueError, id="missing-team"),
        pytest.param(
            "position", "P", sqlite3.IntegrityError, id="unsupported-position"
        ),
    ],
)
def test_invalid_identity_preserves_snapshot(
    published_database: Path, field: str, value: object, error: type[Exception]
) -> None:
    with sqlite3.connect(published_database) as conn:
        original = read_treated_stats(conn, "defender", LEAGUE, DATE)
        replacement = original.drop(columns=["league_name", "date", "layout", "origin"])
        replacement = replacement.astype({field: object})
        replacement.loc[0, field] = value
        with pytest.raises(error), conn:
            save_treated_category(
                conn, "defender", LEAGUE, DATE, replacement, "grouped"
            )
        pd.testing.assert_frame_equal(
            read_treated_stats(conn, "defender", LEAGUE, DATE), original
        )


@pytest.mark.parametrize("layout", ["grouped", "legacy_unprefixed"])
def test_treatment_preserves_extracted_schemas_and_rows(
    tmp_path: Path, players: pd.DataFrame, layout: str
) -> None:
    if layout == "legacy_unprefixed":
        players = pd.read_csv(
            Path(__file__).parents[1]
            / "unit"
            / "national_league_example_full_player_stats_df.csv",
            index_col=0,
        )
    database = tmp_path / "source.db"
    extract(database, players)
    with sqlite3.connect(database) as conn:
        for table in ("player_stats", "league_standings"):
            conn.execute(f"CREATE TABLE legacy_{table} AS SELECT * FROM {table}")
        before = extracted_tables(conn)
    treatment.treat_player_stats(str(database), LEAGUE, DATE)
    with sqlite3.connect(database) as conn:
        assert extracted_tables(conn) == before
        assert conn.execute(
            "SELECT DISTINCT layout FROM treated_snapshots"
        ).fetchall() == [(layout,)]


@pytest.mark.parametrize(
    ("league", "date"),
    [("unknown_league", DATE), (LEAGUE, "2026-02-30"), (LEAGUE, "20261011")],
    ids=["unsupported-league", "invalid-date", "noncanonical-date"],
)
def test_invalid_scope_cannot_replace_snapshot(
    published_database: Path, league: str, date: str
) -> None:
    with sqlite3.connect(published_database) as conn:
        before = list(conn.iterdump())
    with pytest.raises(ValueError):
        treatment.treat_player_stats(published_database, league, date)
    with sqlite3.connect(published_database) as conn:
        assert list(conn.iterdump()) == before
