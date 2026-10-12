"""Historical CSV import preserves values and cleans up only verified files."""

from pathlib import Path
import sqlite3
import shutil

import numpy as np
import pandas as pd
import pytest

from mlb_airflow_data_pipeline import statsapi_csv_migration as migration
from mlb_airflow_data_pipeline.db_utils import (
    read_treated_stats,
    invalidate_treated_run,
    ensure_treated_tables,
    save_treated_category,
)
from mlb_airflow_data_pipeline.treated_schema import metric_types

from .database_helpers import extracted_tables


@pytest.fixture
def csv_database(tmp_path: Path) -> tuple[Path, Path]:
    data = tmp_path / "data"
    data.mkdir()
    database = data / "mlb_data.db"
    sqlite3.connect(database).close()
    for category in ("batter", "pitcher", "defender"):
        frame = pd.DataFrame(1, index=[40, 2], columns=list(metric_types(category)))
        frame["playername"] = ["First Player", "Second Player"]
        frame["team_id"] = [147, 147]
        if category == "pitcher":
            frame["innings"] = [10.1, 20.2]
            frame["era_z_score"] = [np.inf, -np.inf]
        if category == "defender":
            frame["catcherERA"] = [np.nan, 3.2]
            frame["rangeFactorPer9Inn"] = ["-.--", "2.5"]
        frame.to_csv(data / f"american_league_2026-09-25_{category}_stats_df.csv")
    return database, tmp_path / "backup"


def test_lossless_import_idempotency_and_verified_cleanup(
    csv_database: tuple[Path, Path],
) -> None:
    database, backup = csv_database
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots, backup)
    migration.import_snapshots(database, snapshots, backup.with_name("second-backup"))
    migration.verify_imports(database, snapshots)
    with sqlite3.connect(database) as conn:
        defenders = read_treated_stats(
            conn, "defender", "american_league", "2026-09-25"
        )
        assert defenders.player_id.isna().all()
        assert defenders.position.isna().all()
        assert pd.isna(defenders.rangeFactorPer9Inn.iloc[0])
        assert "-.--" in defenders.source_values.iloc[0]
        assert defenders.source_index.tolist() == ["40", "2"]
        pitchers = read_treated_stats(conn, "pitcher", "american_league", "2026-09-25")
        assert pitchers.innings.tolist() == [10.1, 20.2]
        assert pitchers.era_z_score.tolist() == [np.inf, -np.inf]
        assert conn.execute("SELECT COUNT(*) FROM csv_imports").fetchone()[0] == 3
    migration.cleanup_imports(database, snapshots)
    assert not list(database.parent.glob("*.csv"))
    assert len(list(backup.glob("*.csv"))) == 3
    migration.cleanup_imports(database, migration.inventory(database.parent))


@pytest.mark.parametrize("change", ["value", "manifest", "invalidation"])
def test_changed_database_prevents_cleanup(
    csv_database: tuple[Path, Path], change: str
) -> None:
    database, backup = csv_database
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots, backup)
    with sqlite3.connect(database) as conn:
        if change == "invalidation":
            invalidate_treated_run(conn, "american_league", "2026-09-25")
        else:
            conn.execute(
                {
                    "value": "UPDATE batter_stats SET hits = 999",
                    "manifest": "UPDATE csv_imports SET checksum = 'changed'",
                }[change]
            )
    with pytest.raises((ValueError, AssertionError)):
        migration.cleanup_imports(database, snapshots)
    assert len(list(database.parent.glob("*.csv"))) == 3


def test_changed_file_and_conflicting_target_never_overwrite(
    csv_database: tuple[Path, Path],
) -> None:
    database, backup = csv_database
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots, backup)
    path = snapshots[0].path
    frame = pd.read_csv(path, index_col=0)
    frame.loc[40, "hits"] = 999
    frame.to_csv(path)
    with pytest.raises(ValueError, match="Changed previously imported"):
        migration.import_snapshots(
            database,
            migration.inventory(database.parent),
            backup.with_name("conflict-backup"),
        )
    with pytest.raises(ValueError, match="changed before cleanup"):
        migration.cleanup_imports(database, snapshots)
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT hits FROM batter_stats").fetchall() == [(1,), (1,)]
    assert len(list(database.parent.glob("*.csv"))) == 3


def test_partial_inventory_and_interrupted_cleanup_resume(
    csv_database: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    database, backup = csv_database
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots[:1], backup)
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT category FROM treated_snapshots").fetchall() == [
            ("batter",)
        ]
    migration.import_snapshots(
        database, snapshots[1:], backup.with_name("remaining-backup")
    )
    unlink = Path.unlink
    first_path = snapshots[0].path

    def interrupted(path, *args, **kwargs):
        if path != first_path:
            raise OSError("cleanup interrupted")
        unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        migration.cleanup_imports(database, snapshots)
    monkeypatch.setattr(Path, "unlink", unlink)
    migration.cleanup_imports(database, migration.inventory(database.parent))
    assert not list(database.parent.glob("*.csv"))


@pytest.mark.parametrize("invalid", ["unknown", "duplicate", "date", "index"])
def test_inventory_rejects_unsupported_or_ambiguous_data(
    csv_database: tuple[Path, Path], invalid: str
) -> None:
    database, _ = csv_database
    path = next(database.parent.glob("*batter*.csv"))
    frame = pd.read_csv(path)
    if invalid == "unknown":
        frame["unknown_stat"] = 1
    elif invalid == "duplicate":
        frame.loc[1, "playername"] = frame.loc[0, "playername"]
    elif invalid == "index":
        frame[""] = 1
    else:
        path = path.rename(
            path.with_name(path.name.replace("2026-09-25", "2026-02-30"))
        )
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError):
        migration.inventory(database.parent)
    assert len(list(database.parent.glob("*.csv"))) == 3


def test_identical_live_partition_is_preserved(csv_database: tuple[Path, Path]) -> None:
    database, backup = csv_database
    snapshot = migration.inventory(database.parent)[0]
    frame = snapshot.frame.drop(
        columns=["source_checksum", "source_row", "source_index", "source_values"]
    )
    frame["player_id"] = [101, 102]
    with sqlite3.connect(database) as conn:
        ensure_treated_tables(conn)
        save_treated_category(
            conn, "batter", snapshot.league, snapshot.date, frame, snapshot.layout
        )
    migration.import_snapshots(database, [snapshot], backup)
    migration.verify_imports(database, [snapshot])
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT DISTINCT origin FROM batter_stats").fetchall() == [
            ("treatment",)
        ]
        assert conn.execute(
            "SELECT player_id FROM batter_stats ORDER BY player_id"
        ).fetchall() == [(101,), (102,)]


def test_identical_file_contents_on_different_dates_are_distinct_snapshots(
    csv_database: tuple[Path, Path],
) -> None:
    database, backup = csv_database
    path = next(database.parent.glob("*batter*.csv"))
    shutil.copy2(path, path.with_name(path.name.replace("2026-09-25", "2026-09-26")))
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots, backup)
    migration.verify_imports(database, snapshots)
    with sqlite3.connect(database) as conn:
        assert conn.execute(
            "SELECT date, COUNT(*) FROM batter_stats GROUP BY date ORDER BY date"
        ).fetchall() == [("2026-09-25", 2), ("2026-09-26", 2)]


def test_csv_import_preserves_extracted_schemas_and_rows(
    csv_database: tuple[Path, Path],
) -> None:
    database, backup = csv_database
    with sqlite3.connect(database) as conn:
        conn.executescript("""
            CREATE TABLE player_stats (player_id INTEGER PRIMARY KEY, hitting_hits TEXT, fielding_C_assists TEXT);
            INSERT INTO player_stats VALUES (101, '40', '6');
            CREATE TABLE league_standings (team_id INTEGER PRIMARY KEY, w INTEGER);
            INSERT INTO league_standings VALUES (147, 50);
            CREATE TABLE legacy_player_stats (playername TEXT, hits TEXT);
            INSERT INTO legacy_player_stats VALUES ('Historical Player', '7');
            CREATE TABLE legacy_league_standings AS SELECT * FROM league_standings;
        """)
        before = extracted_tables(conn)
    snapshots = migration.inventory(database.parent)
    migration.import_snapshots(database, snapshots, backup)
    migration.verify_imports(database, snapshots)
    migration.cleanup_imports(database, snapshots)
    with sqlite3.connect(database) as conn:
        assert extracted_tables(conn) == before
