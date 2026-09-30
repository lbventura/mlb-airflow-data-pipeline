"""Database behavior for a complete extraction run."""

import sqlite3
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    backup_database_before_migration,
    create_connection,
    read_player_stats,
    save_extraction_run,
)


def _standings(*teams: int) -> pd.DataFrame:
    return pd.DataFrame({"team_id": teams, "name": [f"Team {team}" for team in teams]})


def _players(*rows: tuple[int, int, str, int]) -> pd.DataFrame:
    return pd.DataFrame(
        rows, columns=["player_id", "team_id", "playername", "hits"]
    ).set_index("player_id")


def _insert_legacy_rows(
    conn: sqlite3.Connection, table_name: str, rows: pd.DataFrame
) -> None:
    rows.to_sql(table_name, conn, if_exists="append", index=False)


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    with create_connection(str(tmp_path / "runs.db")) as connection:
        yield connection


@pytest.fixture
def snapshot_db(conn: sqlite3.Connection) -> sqlite3.Connection:
    """One saved American League snapshot for replacement and rollback tests."""
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )
    return conn


def test_same_day_rerun_replaces_only_its_snapshot(conn: sqlite3.Connection) -> None:
    standings = _standings(1, 2)
    players = _players((10, 1, "Alex", 3), (10, 2, "Alex", 3), (20, 2, "Ben", 4))
    save_extraction_run(conn, "american_league", "2026-09-26", standings, players)
    save_extraction_run(conn, "american_league", "2026-09-26", standings, players)
    save_extraction_run(
        conn,
        "national_league",
        "2026-09-26",
        _standings(3),
        _players((30, 3, "Chris", 5)),
    )
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-25",
        _standings(1),
        _players((10, 1, "Alex", 2)),
    )
    assert conn.execute(
        "SELECT team_id FROM player_stats WHERE player_id = 10 "
        "AND league_name = 'american_league' AND date = '2026-09-26' "
        "ORDER BY team_id"
    ).fetchall() == [(1,), (2,)]

    changed = _players((10, 1, "Alex", 7), (10, 1, "Alex", 9))
    changed_standings = pd.DataFrame(
        {"team_id": [1, 1], "name": ["Old name", "Latest name"]}
    )
    save_extraction_run(
        conn, "american_league", "2026-09-26", changed_standings, changed
    )

    current_standings = conn.execute(
        "SELECT team_id, name FROM league_standings WHERE league_name = ? AND date = ?",
        ("american_league", "2026-09-26"),
    ).fetchall()
    current_players = conn.execute(
        "SELECT player_id, team_id, hits FROM player_stats "
        "WHERE league_name = ? AND date = ?",
        ("american_league", "2026-09-26"),
    ).fetchall()
    assert current_standings == [(1, "Latest name")]
    assert current_players == [(10, 1, 9)]
    assert conn.execute(
        "SELECT team_id FROM league_standings WHERE league_name = 'national_league'"
    ).fetchall() == [(3,)]
    assert conn.execute(
        "SELECT hits FROM player_stats WHERE league_name = 'american_league' "
        "AND date = '2026-09-25'"
    ).fetchall() == [(2,)]
    assert conn.execute("SELECT COUNT(*) FROM league_standings").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM player_stats").fetchone()[0] == 3


def test_failed_insert_restores_both_tables(snapshot_db: sqlite3.Connection) -> None:
    conn = snapshot_db
    conn.execute(
        "CREATE TRIGGER reject_bad_player BEFORE INSERT ON player_stats "
        "WHEN NEW.playername = 'Bad' "
        "BEGIN SELECT RAISE(FAIL, 'bad player'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="bad player"):
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(2),
            _players((20, 2, "Bad", 9)),
        )

    assert conn.execute("SELECT team_id FROM league_standings").fetchall() == [(1,)]
    assert conn.execute("SELECT playername FROM player_stats").fetchall() == [("Alex",)]


def test_legacy_rows_are_archived_and_ambiguous_values_are_removed(
    conn: sqlite3.Connection,
) -> None:
    _insert_legacy_rows(
        conn,
        "league_standings",
        pd.DataFrame(
            {
                "team_id": [3, 1],
                "name": ["Old NL team", "Old AL team"],
                "date": [None, "2026-09-26"],
            }
        ),
    )
    _insert_legacy_rows(
        conn,
        "player_stats",
        pd.DataFrame(
            {
                "team_id": [3, 1],
                "playername": ["Old NL player", "Old AL player"],
                "date": [None, "2026-09-26"],
                "league_name": [None, "american_league"],
            }
        ),
    )
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )

    assert (
        conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone()[0] == 2
    )
    assert conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM league_standings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM player_stats").fetchone()[0] == 1


def test_existing_conflicting_rows_are_archived(conn: sqlite3.Connection) -> None:
    old_standings = _standings(3, 3).assign(
        league_name="national_league", date="2026-09-25", name=["Old", "Changed"]
    )
    old_players = _players((30, 3, "Chris", 5), (30, 3, "Chris", 5)).reset_index()
    old_players = old_players.assign(league_name="national_league", date="2026-09-25")
    _insert_legacy_rows(conn, "league_standings", old_standings)
    _insert_legacy_rows(conn, "player_stats", old_players)
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )

    assert (
        conn.execute(
            "SELECT name FROM league_standings WHERE league_name = 'national_league'"
        ).fetchall()
        == []
    )
    assert conn.execute(
        "SELECT playername FROM player_stats WHERE league_name = 'national_league'"
    ).fetchall() == [("Chris",)]
    assert (
        conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone()[0] == 2
    )
    assert conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0] == 2


def test_pre_migration_backup_keeps_original_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy.db"
    with create_connection(str(database_path)) as conn:
        _insert_legacy_rows(
            conn, "league_standings", pd.DataFrame({"team_id": [3], "name": ["Old"]})
        )

    backup_database_before_migration(str(database_path))
    with create_connection(str(database_path)) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1),
            _players((10, 1, "Alex", 3)),
        )
    backup_database_before_migration(str(database_path))

    backup_path = tmp_path / "legacy.db.pre-issue-62.bak"
    with sqlite3.connect(backup_path) as backup:
        assert backup.execute(
            "SELECT team_id, name FROM league_standings"
        ).fetchall() == [(3, "Old")]
        assert (
            backup.execute(
                "SELECT name FROM sqlite_master WHERE name = 'legacy_league_standings'"
            ).fetchone()
            is None
        )


@pytest.mark.parametrize("league_name", ["american_league", "national_league"])
def test_dated_rows_without_keys_are_only_in_archive(
    conn: sqlite3.Connection, league_name: str
) -> None:
    _insert_legacy_rows(
        conn,
        "league_standings",
        pd.DataFrame({"team_id": [3, None], "date": ["2026-09-25"] * 2}),
    )
    _insert_legacy_rows(
        conn,
        "player_stats",
        pd.DataFrame(
            {
                "team_id": [3, 3, None],
                "player_id": [None, None, 30],
                "playername": ["Old"] * 3,
                "league_name": [league_name] * 3,
                "date": ["2026-09-25"] * 3,
            }
        ),
    )
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )
    assert read_player_stats(conn, league_name, "2026-09-25").empty
    assert conn.execute("SELECT COUNT(*) FROM league_standings").fetchone() == (1,)
    assert conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone() == (
        2,
    )
    assert conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone() == (3,)


@pytest.mark.parametrize("migrated", [False, True])
@pytest.mark.parametrize(
    "table_name,key_columns",
    [
        ("league_standings", ("league_name", "date", "team_id")),
        ("player_stats", ("league_name", "date", "team_id", "player_id")),
    ],
)
def test_database_rejects_null_and_duplicate_keys(
    conn: sqlite3.Connection,
    migrated: bool,
    table_name: str,
    key_columns: tuple[str, ...],
) -> None:
    if migrated:
        _insert_legacy_rows(conn, "league_standings", _standings(3))
        _insert_legacy_rows(conn, "player_stats", _players((30, 3, "Old", 2)))
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )
    cursor = conn.execute(f"SELECT * FROM {table_name}")
    columns = [column[0] for column in cursor.description]
    existing_row = list(cursor.fetchone())
    placeholders = ", ".join("?" for _ in existing_row)
    for null_key in key_columns:
        row = existing_row.copy()
        row[columns.index(null_key)] = None
        with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
            conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", existing_row)
    assert conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone() == (1,)


@pytest.mark.parametrize(
    "table_name,invalid_rows",
    [
        pytest.param("standings", pd.DataFrame(), id="empty-standings"),
        pytest.param("player_stats", pd.DataFrame(), id="empty-players"),
        pytest.param(
            "standings",
            _standings(2).drop(columns="team_id"),
            id="missing-standings-team",
        ),
        pytest.param(
            "player_stats",
            _players((20, 2, "Ben", 7)).drop(columns="team_id"),
            id="missing-player-team",
        ),
        pytest.param(
            "standings", _standings(2).assign(team_id=None), id="null-standings-team"
        ),
        pytest.param(
            "player_stats",
            _players((20, 2, "Ben", 7)).assign(team_id=None),
            id="null-player-team",
        ),
        pytest.param(
            "player_stats",
            _players((20, 2, "Ben", 7)).set_axis([None]),
            id="null-player-id",
        ),
    ],
)
def test_invalid_rows_preserve_snapshot(
    snapshot_db: sqlite3.Connection, table_name: str, invalid_rows: pd.DataFrame
) -> None:
    before = list(snapshot_db.iterdump())
    batches = {"standings": _standings(2), "player_stats": _players((20, 2, "Ben", 7))}
    batches[table_name] = invalid_rows
    with pytest.raises(ValueError, match="no rows|missing run key"):
        save_extraction_run(
            snapshot_db,
            "american_league",
            "2026-09-26",
            batches["standings"],
            batches["player_stats"],
        )
    assert list(snapshot_db.iterdump()) == before


@pytest.mark.parametrize("missing_field", ["league_name", "execution_date"])
def test_missing_snapshot_key_preserves_snapshot(
    snapshot_db: sqlite3.Connection, missing_field: str
) -> None:
    before = list(snapshot_db.iterdump())
    arguments: dict[str, Any] = {
        "league_name": "american_league",
        "execution_date": "2026-09-26",
    }
    arguments[missing_field] = None
    with pytest.raises(ValueError, match="missing run key"):
        save_extraction_run(
            snapshot_db,
            standings=_standings(2),
            player_stats=_players((20, 2, "Ben", 7)),
            **arguments,
        )
    assert list(snapshot_db.iterdump()) == before


@pytest.mark.parametrize("legacy", [False, True])
def test_insert_failure_rolls_back_schema_and_archives(
    conn: sqlite3.Connection, legacy: bool
) -> None:
    if legacy:
        _insert_legacy_rows(conn, "league_standings", _standings(3))
        _insert_legacy_rows(conn, "player_stats", _players((30, 3, "Old", 2)))
    else:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1),
            _players((10, 1, "Alex", 3)),
        )
    before = list(conn.iterdump())
    # SQLite cannot bind a list; fail after standings and both schemas change.
    players = _players((20, 2, "Changed", 7)).assign(new_stat=[[1, 2]])
    with pytest.raises(sqlite3.ProgrammingError, match="not supported"):
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(2).assign(new_stat=5),
            players,
        )
    assert list(conn.iterdump()) == before


def test_migration_preserves_history_and_archive_on_later_saves(
    conn: sqlite3.Connection,
) -> None:
    _insert_legacy_rows(
        conn,
        "league_standings",
        _standings(3).assign(
            league_name="national_league", date="2026-09-25", old_stat=8
        ),
    )
    _insert_legacy_rows(
        conn,
        "player_stats",
        _players((30, 3, "Old", 2))
        .reset_index()
        .assign(league_name="national_league", date="2026-09-25", old_stat=9),
    )
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )
    archive_before = {
        table: conn.execute(f"SELECT * FROM legacy_{table}").fetchall()
        for table in ("league_standings", "player_stats")
    }
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1).assign(new_stat=4),
        _players((10, 1, "Alex", 7)).assign(new_stat=5),
    )
    assert conn.execute(
        "SELECT team_id, old_stat FROM league_standings WHERE league_name='national_league'"
    ).fetchall() == [(3, 8)]
    assert conn.execute(
        "SELECT player_id, old_stat FROM player_stats WHERE league_name='national_league'"
    ).fetchall() == [(30, 9)]
    for table, rows in archive_before.items():
        assert conn.execute(f"SELECT * FROM legacy_{table}").fetchall() == rows
        assert "new_stat" not in [
            r[1] for r in conn.execute(f"PRAGMA table_info(legacy_{table})")
        ]
    assert conn.execute(
        "SELECT new_stat FROM player_stats WHERE player_id=10"
    ).fetchone() == (5,)


def test_existing_archive_keeps_its_rows_and_column_mapping(
    conn: sqlite3.Connection,
) -> None:
    _insert_legacy_rows(
        conn,
        "legacy_player_stats",
        pd.DataFrame({"playername": ["Archived"], "team_id": [4]}),
    )
    _insert_legacy_rows(
        conn,
        "player_stats",
        pd.DataFrame({"team_id": [3], "playername": ["Old"], "hits": [2]}),
    )
    save_extraction_run(
        conn,
        "american_league",
        "2026-09-26",
        _standings(1),
        _players((10, 1, "Alex", 3)),
    )
    assert conn.execute(
        "SELECT playername, team_id, hits FROM legacy_player_stats"
    ).fetchall() == [
        ("Archived", 4, None),
        ("Old", 3, 2),
    ]


def test_nullable_statistic_types_are_stored_as_sql_values(
    conn: sqlite3.Connection,
) -> None:
    players = _players((10, 1, "Alex", 3), (20, 1, "Ben", 4))
    players["count"] = pd.array([7, None], dtype="Int64")
    players["active"] = pd.array([True, None], dtype="boolean")
    players["average"] = [0.25, float("nan")]
    players["note"] = pd.array(["a", None], dtype="string")
    save_extraction_run(conn, "american_league", "2026-09-26", _standings(1), players)
    assert conn.execute(
        'SELECT "count", active, average, note FROM player_stats ORDER BY player_id'
    ).fetchall() == [
        (7, 1, 0.25, "a"),
        (None, None, None, None),
    ]
