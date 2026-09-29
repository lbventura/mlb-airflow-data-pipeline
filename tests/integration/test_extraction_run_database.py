"""Database behavior for a complete extraction run."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    backup_database_before_migration,
    create_connection,
    insert_dataframe,
    read_player_stats,
    save_extraction_run,
)

from mlb_airflow_data_pipeline.statsapi_extraction_script import run_extraction


def _standings(*teams: int) -> pd.DataFrame:
    return pd.DataFrame({"team_id": teams, "name": [f"Team {team}" for team in teams]})


def _players(*rows: tuple[int, int, str, int]) -> pd.DataFrame:
    return pd.DataFrame(
        rows, columns=["player_id", "team_id", "playername", "hits"]
    ).set_index("player_id")


def test_same_day_rerun_replaces_only_its_snapshot(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
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
            "SELECT team_id, name FROM league_standings "
            "WHERE league_name = ? AND date = ?",
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


def test_failed_insert_restores_both_tables(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1),
            _players((10, 1, "Alex", 3)),
        )
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
        assert conn.execute("SELECT playername FROM player_stats").fetchall() == [
            ("Alex",)
        ]


def test_legacy_rows_are_archived_and_ambiguous_values_are_removed(
    tmp_path: Path,
) -> None:
    with create_connection(str(tmp_path / "legacy.db")) as conn:
        insert_dataframe(
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
        insert_dataframe(
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
            conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone()[0]
            == 2
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0] == 2
        )
        assert conn.execute("SELECT COUNT(*) FROM league_standings").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM player_stats").fetchone()[0] == 1


def test_existing_conflicting_rows_are_archived(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "legacy.db")) as conn:
        old = pd.DataFrame(
            {
                "league_name": ["national_league", "national_league"],
                "date": ["2026-09-25", "2026-09-25"],
                "team_id": [3, 3],
                "name": ["Old", "Changed"],
            }
        )
        insert_dataframe(conn, "league_standings", old)
        insert_dataframe(
            conn,
            "player_stats",
            pd.DataFrame(
                {
                    "league_name": ["national_league", "national_league"],
                    "date": ["2026-09-25", "2026-09-25"],
                    "team_id": [3, 3],
                    "player_id": [30, 30],
                    "playername": ["Chris", "Chris"],
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
            conn.execute(
                "SELECT COUNT(*) FROM league_standings "
                "WHERE league_name = 'national_league'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM player_stats WHERE league_name = 'national_league'"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone()[0]
            == 2
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0] == 2
        )


def test_pre_migration_backup_keeps_original_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy.db"
    with create_connection(str(database_path)) as conn:
        insert_dataframe(
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


def test_failed_team_preserves_previous_snapshot(tmp_path: Path) -> None:
    database_path = str(tmp_path / "runs.db")
    with create_connection(database_path) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1).assign(name="Original"),
            _players((10, 1, "Alex", 3)),
        )

    with patch(
        "mlb_airflow_data_pipeline.statsapi_extraction_script.DataExtractor",
        autospec=True,
    ) as extractor_class:
        extractor = extractor_class.return_value
        extractor.league_standings = _standings(1).assign(name="Changed")
        extractor.team_id_name_mapping = {1: "Changed"}
        extractor.get_player_stats_per_league.return_value = (
            _players((10, 1, "Alex", 3)),
            {},
            ["Failed team"],
        )
        with pytest.raises(RuntimeError, match="Failed team"):
            run_extraction(database_path, "american_league", "2026-09-26")

    with create_connection(database_path) as conn:
        assert conn.execute("SELECT name FROM league_standings").fetchall() == [
            ("Original",)
        ]
        assert conn.execute("SELECT playername FROM player_stats").fetchall() == [
            ("Alex",)
        ]


@pytest.mark.parametrize("league_name", ["american_league", "national_league"])
def test_dated_rows_without_keys_are_only_in_archive(
    tmp_path: Path, league_name: str
) -> None:
    with create_connection(str(tmp_path / "legacy.db")) as conn:
        insert_dataframe(
            conn,
            "league_standings",
            pd.DataFrame({"team_id": [3, None], "date": ["2026-09-25"] * 2}),
        )
        insert_dataframe(
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
        assert conn.execute(
            "SELECT COUNT(*) FROM legacy_league_standings"
        ).fetchone() == (2,)
        assert conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone() == (
            3,
        )


@pytest.mark.parametrize("migrated", [False, True])
@pytest.mark.parametrize(
    "table_name,key_columns",
    [
        ("league_standings", ("league_name", "date", "team_id")),
        ("player_stats", ("league_name", "date", "team_id", "player_id")),
    ],
)
def test_database_rejects_null_and_duplicate_keys(
    tmp_path: Path,
    migrated: bool,
    table_name: str,
    key_columns: tuple[str, ...],
) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        if migrated:
            insert_dataframe(conn, "league_standings", _standings(3))
            insert_dataframe(conn, "player_stats", _players((30, 3, "Old", 2)))
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
            conn.execute(
                f"INSERT INTO {table_name} VALUES ({placeholders})", existing_row
            )
        assert conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone() == (1,)


@pytest.mark.parametrize(
    "invalid_input",
    [
        "empty_standings",
        "empty_players",
        "missing_standings_team",
        "missing_players_team",
        "null_standings_team",
        "null_players_team",
        "null_player_id",
        "null_league",
        "null_date",
    ],
)
def test_invalid_batch_preserves_both_snapshots(
    tmp_path: Path, invalid_input: str
) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1),
            _players((10, 1, "Alex", 3)),
        )
        before = list(conn.iterdump())
        standings = _standings(2)
        players = _players((20, 2, "Changed", 7))
        arguments: dict = {
            "league_name": "american_league",
            "execution_date": "2026-09-26",
        }
        if invalid_input == "empty_standings":
            standings = standings.iloc[:0]
        elif invalid_input == "empty_players":
            players = players.iloc[:0]
        elif invalid_input == "missing_standings_team":
            standings = standings.drop(columns="team_id")
        elif invalid_input == "missing_players_team":
            players = players.drop(columns="team_id")
        elif invalid_input == "null_standings_team":
            standings = standings.assign(team_id=None)
        elif invalid_input == "null_players_team":
            players = players.assign(team_id=None)
        elif invalid_input == "null_player_id":
            players.index = pd.Index([None])
        elif invalid_input == "null_league":
            arguments["league_name"] = None
        else:
            arguments["execution_date"] = None
        with pytest.raises(ValueError, match="no rows|missing run key"):
            save_extraction_run(
                conn, standings=standings, player_stats=players, **arguments
            )
        assert list(conn.iterdump()) == before


@pytest.mark.parametrize("legacy", [False, True])
def test_insert_failure_rolls_back_schema_and_archives(
    tmp_path: Path, legacy: bool
) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        if legacy:
            insert_dataframe(conn, "league_standings", _standings(3))
            insert_dataframe(conn, "player_stats", _players((30, 3, "Old", 2)))
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


def test_migration_preserves_history_and_archive_on_later_saves(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        insert_dataframe(
            conn,
            "league_standings",
            _standings(3).assign(
                league_name="national_league", date="2026-09-25", old_stat=8
            ),
        )
        insert_dataframe(
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
        # Archive writes are forbidden after migration, including schema extension.
        for table in archive_before:
            conn.execute(
                f"CREATE TRIGGER no_archive_write_{table} BEFORE INSERT ON legacy_{table} "
                "BEGIN SELECT RAISE(FAIL, 'archive is immutable'); END"
            )
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


def test_existing_archive_keeps_its_rows_and_column_mapping(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        insert_dataframe(
            conn,
            "legacy_player_stats",
            pd.DataFrame({"playername": ["Archived"], "team_id": [4]}),
        )
        insert_dataframe(
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


def test_nullable_statistic_types_are_stored_as_sql_values(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        players = _players((10, 1, "Alex", 3), (20, 1, "Ben", 4))
        players["count"] = pd.array([7, None], dtype="Int64")
        players["active"] = pd.array([True, None], dtype="boolean")
        players["average"] = [0.25, float("nan")]
        players["note"] = pd.array(["a", None], dtype="string")
        save_extraction_run(
            conn, "american_league", "2026-09-26", _standings(1), players
        )
        assert conn.execute(
            'SELECT "count", active, average, note FROM player_stats ORDER BY player_id'
        ).fetchall() == [
            (7, 1, 0.25, "a"),
            (None, None, None, None),
        ]
