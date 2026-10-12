"""One-time import and verified cleanup of historical treated CSVs."""

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import re

import pandas as pd

from mlb_airflow_data_pipeline.db_utils import (
    database_path,
    ensure_treated_tables,
    read_connection,
    read_treated_stats,
    save_treated_category,
    SnapshotUnavailable,
    validate_scope,
)
from mlb_airflow_data_pipeline.statsapi_parameters_script import DEFENDER_POSITIONS
from mlb_airflow_data_pipeline.treated_schema import (
    BASE_STATS,
    table_statements,
    RATE_STATS,
    metric_types,
)

FILE_PATTERN = re.compile(
    r"(american_league|national_league)_(\d{4}-\d{2}-\d{2})_(batter|pitcher|defender)_stats_df\.csv"
)


@dataclass
class CsvSnapshot:
    path: Path
    checksum: str
    league: str
    date: str
    category: str
    layout: str
    frame: pd.DataFrame
    size: int

    def inventory_record(self) -> dict:
        return {
            "filename": self.path.name,
            "checksum": self.checksum,
            "size": self.size,
            "league_name": self.league,
            "date": self.date,
            "category": self.category,
            "layout": self.layout,
            "row_count": len(self.frame),
            "columns": list(self.frame),
        }


def load_csv_snapshot(path: Path) -> CsvSnapshot:
    """Parse a supported layout without guessing IDs, positions, or unknown metrics."""
    match = FILE_PATTERN.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Unsupported runtime CSV: {path.name}")
    league, date, category = match.groups()
    validate_scope(league, date)
    data = path.read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    # Reading text first preserves names, empty values, original index, and infinity.
    reader = csv.DictReader(StringIO(data.decode("utf-8-sig")))
    headers = reader.fieldnames or []
    if len(headers) != len(set(headers)):
        raise ValueError(f"Duplicate CSV columns: {path.name}")
    rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"Malformed CSV row: {path.name}")
    frame = pd.DataFrame(rows, columns=headers)
    index_columns = [c for c in headers if c in ("", "Unnamed: 0")]
    if len(index_columns) > 1:
        raise ValueError(f"Ambiguous CSV index: {path.name}")
    index = (
        frame.pop(index_columns[0])
        if index_columns
        else pd.Series([None] * len(frame), dtype=object)
    )
    for field, expected in (("league_name", league), ("date", date)):
        if field in frame:
            if not frame[field].eq(expected).all():
                raise ValueError(f"Conflicting {field}: {path.name}")
            frame = frame.drop(columns=field)
    metrics = metric_types(category)
    allowed = set(metrics) | {"playername", "team_id", "player_id"}
    if category == "defender":
        allowed.add("position")
    unknown = set(frame) - allowed
    required = {"playername", "team_id", *BASE_STATS[category]}
    missing = required - set(frame)
    if unknown or missing:
        raise ValueError(
            f"Unsupported layout: {path.name}; unknown={sorted(unknown)}, missing={sorted(missing)}"
        )
    for name in frame.columns.intersection([*metrics, "team_id", "player_id"]):
        frame[name] = pd.to_numeric(
            frame[name].replace(["", "-.--"] if name in RATE_STATS else [""], None),
            errors="raise",
        )
        if name in ("team_id", "player_id") or metrics.get(name) == "INTEGER":
            values = frame[name].dropna()
            if ((values % 1) != 0).any():
                raise ValueError(f"Noninteger {name}: {path.name}")
    if frame.playername.eq("").any() or frame.team_id.isna().any():
        raise ValueError(f"Incomplete CSV identity: {path.name}")
    if "player_id" in frame and frame.player_id.isna().any():
        raise ValueError(f"Incomplete CSV player IDs: {path.name}")
    layout = (
        "grouped"
        if category == "defender" and "position" in frame
        else "legacy_unprefixed"
    )
    # Historical batters/pitchers with verified IDs are the newer grouped layout.
    if category != "defender" and "player_id" in frame:
        layout = "grouped"
    keys = ["team_id", "player_id" if "player_id" in frame else "playername"]
    if "position" in frame:
        keys.append("position")
    if "position" in frame and not frame.position.isin(DEFENDER_POSITIONS).all():
        raise ValueError(f"Unsupported CSV fielding position: {path.name}")
    if frame.duplicated(keys).any():
        raise ValueError(f"Duplicate CSV identity: {path.name}")
    frame["source_checksum"] = checksum
    frame["source_row"] = range(len(frame))
    frame["source_index"] = index.replace("", None)
    frame["source_values"] = [json.dumps(row, ensure_ascii=False) for row in rows]
    return CsvSnapshot(path, checksum, league, date, category, layout, frame, len(data))


def inventory(directory: Path) -> list[CsvSnapshot]:
    """Validate all CSVs in the runtime directory before making any changes."""
    snapshots = [load_csv_snapshot(path) for path in sorted(directory.glob("*.csv"))]
    keys = [(s.league, s.date, s.category) for s in snapshots]
    if len(keys) != len(set(keys)):
        raise ValueError("Multiple files identify the same snapshot")
    return snapshots


def verify_snapshot(conn: sqlite3.Connection, snapshot: CsvSnapshot) -> None:
    """Compare original values, plus provenance when the stored partition is imported."""
    stored = read_treated_stats(conn, snapshot.category, snapshot.league, snapshot.date)
    record = conn.execute(
        "SELECT layout, origin FROM treated_snapshots WHERE league_name = ? AND date = ? AND category = ?",
        (snapshot.league, snapshot.date, snapshot.category),
    ).fetchone()
    if record[0] != snapshot.layout:
        raise ValueError(f"Conflicting stored layout: {snapshot.path.name}")
    expected = snapshot.frame.copy()
    if record[1] == "csv":
        stored = stored.sort_values("source_row")
    else:
        expected = expected.drop(
            columns=["source_checksum", "source_row", "source_index", "source_values"]
        )
        keys = ["team_id", "player_id" if "player_id" in expected else "playername"]
        if "position" in expected:
            keys.append("position")
        stored = stored.sort_values(keys)
        expected = expected.sort_values(keys)
    try:
        pd.testing.assert_frame_equal(
            stored[list(expected)].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_dtype=False,
            check_exact=True,
            check_index_type=False,
        )
    except AssertionError as error:
        raise ValueError(f"Conflicting stored values: {snapshot.path.name}") from error


def backup_inputs(
    database: Path, snapshots: list[CsvSnapshot], backup_directory: Path
) -> None:
    """Take a fresh SQLite backup and preserve each original CSV before import."""
    if backup_directory.resolve().is_relative_to(database.parent.resolve()):
        raise ValueError("Backup directory must be outside the live data directory")
    backup_directory.mkdir(parents=True, exist_ok=False)
    with (
        read_connection(str(database)) as source,
        closing(sqlite3.connect(backup_directory / database.name)) as target,
    ):
        source.backup(target)
    for snapshot in snapshots:
        shutil.copy2(snapshot.path, backup_directory / snapshot.path.name)
        if (
            hashlib.sha256(
                (backup_directory / snapshot.path.name).read_bytes()
            ).hexdigest()
            != snapshot.checksum
        ):
            raise ValueError(f"CSV changed during backup: {snapshot.path.name}")
    (backup_directory / "inventory.json").write_text(
        json.dumps([s.inventory_record() for s in snapshots], indent=2) + "\n"
    )


def import_snapshots(
    database: Path, snapshots: list[CsvSnapshot], backup_directory: Path
) -> None:
    """Back up first; import each file atomically and verify after commit."""
    backup_inputs(database, snapshots, backup_directory)
    with closing(sqlite3.connect(database)) as conn:
        for snapshot in snapshots:
            conn.execute("BEGIN IMMEDIATE")
            try:
                ensure_treated_tables(conn)
                for statement in table_statements("csv_imports"):
                    conn.execute(statement)
                previous = conn.execute(
                    "SELECT checksum FROM csv_imports WHERE filename = ?",
                    (snapshot.path.name,),
                ).fetchone()
                if previous and previous[0] != snapshot.checksum:
                    raise ValueError(
                        f"Changed previously imported CSV: {snapshot.path.name}"
                    )
                try:
                    read_treated_stats(
                        conn, snapshot.category, snapshot.league, snapshot.date
                    )
                except SnapshotUnavailable:
                    if previous:
                        raise ValueError(
                            f"Imported partition was invalidated: {snapshot.path.name}"
                        ) from None
                    save_treated_category(
                        conn,
                        snapshot.category,
                        snapshot.league,
                        snapshot.date,
                        snapshot.frame,
                        snapshot.layout,
                        origin="csv",
                    )
                else:
                    verify_snapshot(conn, snapshot)
                conn.execute(
                    "INSERT OR IGNORE INTO csv_imports VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        snapshot.path.name,
                        snapshot.checksum,
                        snapshot.league,
                        snapshot.date,
                        snapshot.category,
                        len(snapshot.frame),
                        json.dumps(
                            snapshot.inventory_record()
                            | {"source_values": snapshot.frame.source_values.tolist()}
                        ),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            verify_snapshot(conn, snapshot)


def verify_imports(database: Path, snapshots: list[CsvSnapshot]) -> None:
    """Require the manifest and exact stored values for every remaining source file."""
    with read_connection(str(database)) as conn:
        for snapshot in snapshots:
            row = conn.execute(
                "SELECT checksum FROM csv_imports WHERE filename = ?",
                (snapshot.path.name,),
            ).fetchone()
            if row != (snapshot.checksum,):
                raise ValueError(
                    f"Missing or changed import manifest: {snapshot.path.name}"
                )
            verify_snapshot(conn, snapshot)


def cleanup_imports(database: Path, snapshots: list[CsvSnapshot]) -> None:
    """Verify the entire inventory, then lock and recheck each file before deletion."""
    verify_imports(database, snapshots)
    with closing(sqlite3.connect(database)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for snapshot in snapshots:
                verify_snapshot(conn, snapshot)
                if (
                    hashlib.sha256(snapshot.path.read_bytes()).hexdigest()
                    != snapshot.checksum
                ):
                    raise ValueError(
                        f"CSV changed before cleanup: {snapshot.path.name}"
                    )
                snapshot.path.unlink()
        finally:
            conn.rollback()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inventory", "import", "verify", "cleanup"))
    parser.add_argument("--database", type=Path, default=database_path())
    parser.add_argument("--backup-directory", type=Path)
    args = parser.parse_args()
    snapshots = inventory(args.database.parent)
    if args.action == "inventory":
        print(json.dumps([s.inventory_record() for s in snapshots], indent=2))
    elif args.action == "import":
        backup = args.backup_directory or args.database.parent.parent / (
            "csv-backup-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        )
        import_snapshots(args.database, snapshots, backup)
        print(f"Imported and verified {len(snapshots)} files; backup: {backup}")
    elif args.action == "verify":
        verify_imports(args.database, snapshots)
        print(f"Verified {len(snapshots)} files")
    else:
        cleanup_imports(args.database, snapshots)
        print(f"Removed {len(snapshots)} verified files")


if __name__ == "__main__":
    main()
