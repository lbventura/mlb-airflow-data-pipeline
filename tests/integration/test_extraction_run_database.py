"""Database behavior for a complete extraction run."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from mlb_airflow_data_pipeline.db_utils import (
    backup_database_before_migration,
    create_connection,
    insert_dataframe,
    save_extraction_run,
)


def _standings(*teams: int) -> pd.DataFrame:
    return pd.DataFrame(
        {"team_id": teams, "name": [f"Team {team}" for team in teams]}
    )


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

        assert conn.execute("SELECT team_id FROM league_standings").fetchall() == [
            (1,)
        ]
        assert conn.execute("SELECT playername FROM player_stats").fetchall() == [
            ("Alex",)
        ]


def test_missing_keys_do_not_change_database(tmp_path: Path) -> None:
    with create_connection(str(tmp_path / "runs.db")) as conn:
        save_extraction_run(
            conn,
            "american_league",
            "2026-09-26",
            _standings(1),
            _players((10, 1, "Alex", 3)),
        )
        with pytest.raises(ValueError, match="missing run keys"):
            save_extraction_run(
                conn,
                "american_league",
                "2026-09-26",
                _standings(2),
                _players((None, 2, "Bad", 9)),
            )
        assert conn.execute("SELECT team_id FROM league_standings").fetchall() == [
            (1,)
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
            conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0]
            == 2
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

        assert conn.execute(
            "SELECT COUNT(*) FROM league_standings "
            "WHERE league_name = 'national_league'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM player_stats WHERE league_name = 'national_league'"
        ).fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM legacy_league_standings").fetchone()[0]
            == 2
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM legacy_player_stats").fetchone()[0]
            == 2
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
        ).fetchall() == [
            (3, "Old")
        ]
        assert backup.execute(
            "SELECT name FROM sqlite_master WHERE name = 'legacy_league_standings'"
        ).fetchone() is None
