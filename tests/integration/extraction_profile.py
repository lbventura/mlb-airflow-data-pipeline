"""Direct extraction measurements; production code and HTTP policy stay unchanged."""

import argparse
import cProfile
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import pstats
import signal
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterator

import pandas as pd
import pytest
import statsapi

from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction
from mlb_airflow_data_pipeline.db_utils import (
    create_connection,
    ensure_dataframe_columns,
    insert_dataframe,
)

ROOT = Path(__file__).resolve().parents[2]
SOURCE_FILES = (
    "mlb_airflow_data_pipeline/statsapi_extraction_script.py",
    "mlb_airflow_data_pipeline/statsapi_parameters_script.py",
    "mlb_airflow_data_pipeline/db_utils.py",
    "mlb_airflow_data_pipeline/logging_setup.py",
    "tests/integration/extraction_profile.py",
    "tests/integration/test_statsapi_extraction_performance.py",
)
PARAMETERS = (
    "teamId",
    "personId",
    "season",
    "rosterType",
    "hydrate",
    "sportId",
    "leagueId",
)


class AttemptDeadline(BaseException):
    """Bypass the extractor's per-team Exception handler when the run deadline expires."""


def deadline_signal(signum: int, frame: Any) -> None:
    raise AttemptDeadline("The supervisor's measurement deadline expired")


def write_json(path: Path, data: Any) -> None:
    """Replace a checkpoint atomically so termination cannot leave half a document."""
    pending = path.with_suffix(".tmp")
    pending.write_text(json.dumps(data, indent=2, default=str) + "\n")
    pending.replace(path)


def error_details(error: BaseException) -> dict[str, Any]:
    response = getattr(error, "response", None)
    return {
        "type": type(error).__name__,
        "message": str(error),
        "http_status": response.status_code if response is not None else None,
    }


def source_hashes() -> dict[str, str]:
    return {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in SOURCE_FILES
    }


def metadata(league: str, profile: bool, data_source: str) -> dict[str, Any]:
    assert Path(extraction.__file__).resolve().is_relative_to(ROOT)
    return {
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "source_status": subprocess.check_output(
            ["git", "status", "--short"], cwd=ROOT, text=True
        ).splitlines(),
        "source_hashes": source_hashes(),
        "module_paths": {
            "extraction": str(Path(extraction.__file__).resolve()),
            "statsapi": str(Path(statsapi.__file__).resolve()),
        },
        "versions": {
            name: importlib.metadata.version(name)
            for name in ("MLB-StatsAPI", "requests", "pandas", "pytest")
        },
        "python": sys.version,
        "platform": platform.platform(),
        "statsapi_source_sha256": hashlib.sha256(
            Path(statsapi.__file__).read_bytes()
        ).hexdigest(),
        "league": league,
        "season_year": extraction.SEASON_YEAR,
        "stats_type": extraction._get_stats_type(),
        "player_stats_season_argument": None,
        "execution_date": extraction.DATE_TIME_EXECUTION,
        "data_source": data_source,
        "profile_enabled": profile,
        "workers": 1,
    }


class Recorder:
    """Record completed calls and stream starts so a stalled request remains visible."""

    def __init__(self, directory: Path, original_get: Callable[..., Any]) -> None:
        self.directory = directory
        self.original_get = original_get
        self.calls: list[dict[str, Any]] = []
        self.started_calls: list[dict[str, Any]] = []
        self.teams: list[dict[str, Any]] = []
        self.rosters: dict[str, list[dict[str, Any]]] = {}
        self.frames: dict[int, pd.DataFrame] = {}
        self.lock = threading.RLock()
        self.journal = (directory / "events.jsonl").open("a")
        self.observer_seconds = 0.0
        self.sequence = 0
        self.context = threading.local()
        self.artifact_errors: list[dict[str, Any]] = []

    def emit(self, event: dict[str, Any]) -> None:
        original_error = sys.exception()
        with self.lock:
            try:
                self.journal.write(json.dumps(event, default=str) + "\n")
                self.journal.flush()
            except OSError as error:
                if original_error is None:
                    raise
                original_error.add_note(f"Request journal write also failed: {error}")
                self.artifact_errors.append(error_details(error))

    def close(self) -> None:
        original_error = sys.exception()
        try:
            self.journal.close()
        except OSError as error:
            if original_error is None:
                raise
            original_error.add_note(f"Request journal close also failed: {error}")

    def get(self, endpoint: str, *args: Any, **kwargs: Any) -> Any:
        observer_start = time.perf_counter()
        params = args[0] if args else kwargs.get("params", {})
        with self.lock:
            self.sequence += 1
            call: dict[str, Any] = {
                "call_id": self.sequence,
                "endpoint": endpoint,
                "params": {
                    key: str(params[key]) for key in PARAMETERS if key in params
                },
                "started_at": datetime.now(timezone.utc).isoformat(),
                "stage": getattr(self.context, "stage", None),
                "team_id": getattr(self.context, "team_id", None),
            }
            self.started_calls.append(call)
        self.emit({"event": "api_started", **call})
        started = time.perf_counter()
        try:
            result = self.original_get(endpoint, *args, **kwargs)
        except BaseException as error:
            call["error"] = error_details(error)
            call["interrupted"] = isinstance(error, AttemptDeadline)
            raise
        finally:
            call["seconds"] = time.perf_counter() - started
            with self.lock:
                self.calls.append(call)
                self.emit({"event": "api_completed", **call})
                self.observer_seconds += (
                    time.perf_counter() - observer_start - call["seconds"]
                )
        if endpoint == "team_roster" and isinstance(result, dict):
            with self.lock:
                self.rosters[str(params["teamId"])] = [
                    {
                        "player_id": entry.get("person", {}).get("id"),
                        "full_name": entry.get("person", {}).get("fullName"),
                    }
                    for entry in result.get("roster", [])
                ]
        return result

    def endpoints(self) -> dict[str, Any]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with self.lock:
            for call in self.calls:
                groups[call["endpoint"]].append(call)
            started_counts = Counter(call["endpoint"] for call in self.started_calls)
        output = {}
        for endpoint, started_count in started_counts.items():
            calls = groups[endpoint]
            durations = sorted(call["seconds"] for call in calls)
            errors = Counter(
                f"{call['error']['type']}:{call['error']['http_status']}"
                for call in calls
                if "error" in call
            )
            output[endpoint] = {
                "count": len(calls),
                "started_count": started_count,
                "completed_count": sum(
                    not call.get("interrupted", False) for call in calls
                ),
                "interrupted_count": sum(
                    call.get("interrupted", False) for call in calls
                ),
                "in_flight_count": started_count - len(calls),
                "total_seconds": sum(durations) if durations else None,
                "median_seconds": median(durations) if durations else None,
                "p95_seconds": durations[math.ceil(0.95 * len(durations)) - 1]
                if durations
                else None,
                "maximum_seconds": max(durations) if durations else None,
                "exceptions": dict(errors),
                "parameter_sets": [
                    json.loads(value)
                    for value in sorted(
                        {json.dumps(call["params"], sort_keys=True) for call in calls}
                    )
                ],
            }
        return output

    def checkpoint(self, summary: dict[str, Any]) -> None:
        original_error = sys.exception()
        with self.lock:
            summary["endpoints"] = self.endpoints()
            summary["teams"] = self.teams
            summary["api_recorder_overhead_seconds"] = self.observer_seconds
            summary["journal_errors"] = self.artifact_errors
            try:
                write_json(self.directory / "summary.json", summary)
            except OSError as error:
                if original_error is None:
                    raise
                original_error.add_note(f"Checkpoint write also failed: {error}")
                summary.setdefault("artifact_errors", []).append(error_details(error))


@contextmanager
def stage(name: str, summary: dict[str, Any], recorder: Recorder) -> Iterator[None]:
    summary["current_stage"] = name
    recorder.context.stage = name
    recorder.checkpoint(summary)
    started = time.perf_counter()
    try:
        yield
    finally:
        summary["stage_seconds"][name] = time.perf_counter() - started
        recorder.checkpoint(summary)


def snapshot(frame: pd.DataFrame, directory: Path, name: str) -> dict[str, Any]:
    frame.to_pickle(directory / f"{name}.pkl")
    frame.to_csv(directory / f"{name}.csv", index_label="player_id")
    return {
        "rows": len(frame),
        "columns": frame.columns.tolist(),
        "dtypes": {str(key): str(value) for key, value in frame.dtypes.items()},
        "pickle": f"{name}.pkl",
        "csv": f"{name}.csv",
    }


def run_extraction(
    directory: Path, league: str, profile: bool, data_source: str = "live"
) -> dict[str, Any]:
    """Observe the serial script operations, then validate and save their outputs."""
    summary: dict[str, Any] = {
        **metadata(league, profile, data_source),
        "run_id": directory.name,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "outcome": "running",
        "extraction_completed": False,
        "validation_status": "not_run",
        "profile_artifacts": [],
        "stage_seconds": {},
        "failed_teams": [],
        "inactive_players": {},
    }
    for name in SOURCE_FILES:
        destination = directory / "source" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / name).read_bytes())
    profiler = cProfile.Profile()
    recorder = Recorder(directory, statsapi.get)
    extractor: extraction.DataExtractor
    original_team: Callable[[int], tuple[pd.DataFrame, dict]]
    players: pd.DataFrame | None = None
    standings = pd.DataFrame()
    recorder.checkpoint(summary)

    def measured_team(team_id: int) -> tuple[pd.DataFrame, dict]:
        recorder.context.team_id = int(team_id)
        team: dict[str, Any] = {
            "team_id": int(team_id),
            "team_name": extractor.team_id_name_mapping[team_id],
        }
        started = time.perf_counter()
        try:
            frame, inactive = original_team(team_id)
        except BaseException as error:
            team["error"] = error_details(error)
            raise
        finally:
            team["seconds"] = time.perf_counter() - started
            recorder.teams.append(team)
            recorder.checkpoint(summary)
            recorder.context.team_id = None
        team["rows"] = len(frame)
        team["player_ids"] = [int(value) for value in frame.index]
        team["inactive_players"] = {name: int(pid) for name, pid in inactive.items()}
        recorder.frames[int(team_id)] = frame
        recorder.checkpoint(summary)
        return frame, inactive

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(statsapi, "get", recorder.get)
            patch.setattr(extraction, "logger", extraction.logger.bind(league=league))
            wall_start, cpu_start = time.perf_counter(), time.process_time()
            if profile:
                profiler.enable()
            try:
                summary["current_stage"] = "initialization"
                with create_connection(str(directory / "extraction.db")) as conn:
                    extractor = extraction.DataExtractor(league_name=league)
                    original_team = extractor.get_player_stats_dataframe_per_team
                    patch.setattr(
                        extractor, "get_player_stats_dataframe_per_team", measured_team
                    )
                    with stage("standings_and_rosters", summary, recorder):
                        extractor.set_league_team_rosters_player_names()
                    standings = extractor.league_standings
                    summary["requested_team_ids"] = [
                        int(value) for value in standings["team_id"]
                    ]
                    summary["input_name_counts"] = {
                        str(team): len(names)
                        for team, names in extractor.league_team_rosters_player_names.items()
                    }
                    with stage("standings_write", summary, recorder):
                        ensure_dataframe_columns(conn, "league_standings", standings)
                        insert_dataframe(conn, "league_standings", standings)
                    with stage("team_mapping", summary, recorder):
                        extractor.set_team_ids_and_names()
                    with stage("player_extraction", summary, recorder):
                        players, inactive, failed = (
                            extractor.get_player_stats_per_league()
                        )
                    summary["failed_teams"] = failed
                    summary["inactive_players"] = {
                        str(team): {name: int(pid) for name, pid in members.items()}
                        for team, members in inactive.items()
                    }
                    with stage("player_write", summary, recorder):
                        stored = players.assign(league_name=league)
                        ensure_dataframe_columns(conn, "player_stats", stored)
                        insert_dataframe(conn, "player_stats", stored)
            finally:
                profiler.disable()
                summary["wall_seconds"] = time.perf_counter() - wall_start
                summary["process_cpu_seconds"] = time.process_time() - cpu_start

        summary["extraction_completed"] = True
        summary["current_stage"] = "validation"
        summary["validation_status"] = "running"
        assert players is not None and not players.empty, "No extracted rows"
        assert not summary["failed_teams"], f"Failed teams: {summary['failed_teams']}"
        assert sorted(players.columns.tolist()) == extraction.expected_output_columns()
        actual_teams = {int(value) for value in players["team_id"]}
        assert actual_teams == set(summary["requested_team_ids"]), (
            "Missing output teams"
        )
        with create_connection(str(directory / "extraction.db")) as conn:
            saved = pd.read_sql_query("SELECT * FROM player_stats", conn)
        assert saved.columns.tolist() == [*players.columns, "league_name"]
        assert len(saved) == len(players) and set(saved["league_name"]) == {league}
        pd.testing.assert_frame_equal(
            saved, stored.reset_index(drop=True), check_dtype=False
        )

        differences = {}
        for team, entries in recorder.rosters.items():
            expected = Counter(entry["player_id"] for entry in entries)
            observed = Counter(
                int(pid) for pid in players.index[players["team_id"] == int(team)]
            )
            inactive_ids = Counter(summary["inactive_players"].get(team, {}).values())
            missing = expected - observed - inactive_ids
            unexpected = observed + inactive_ids - expected
            if missing or unexpected:
                differences[team] = {
                    "missing_ids": sorted(missing, key=str),
                    "unexpected_ids": sorted(int(value) for value in unexpected),
                    "missing_counts": dict(missing),
                    "unexpected_counts": dict(unexpected),
                }
        summary["coverage_differences"] = differences
        assert not differences, (
            f"Unexplained roster identity differences: {differences}"
        )
        assert source_hashes() == summary["source_hashes"], (
            "Source changed during attempt"
        )
        summary["outcome"] = "success"
        summary["validation_status"] = "passed"
        return summary
    except BaseException as error:
        summary["outcome"] = "failed"
        if summary["validation_status"] == "running":
            summary["validation_status"] = "failed"
        summary["error"] = error_details(error)
        summary["traceback"] = traceback.format_exc()
        raise
    finally:
        original_error = sys.exception()
        try:
            summary["source_unchanged"] = source_hashes() == summary["source_hashes"]
            save_artifacts(summary, recorder, standings, players, profiler, profile)
        except Exception as artifact_error:
            summary.setdefault("artifact_errors", []).append(
                error_details(artifact_error)
            )
            if original_error is None:
                summary["outcome"] = "failed"
                summary["current_stage"] = "artifact_output"
                summary["error"] = error_details(artifact_error)
                raise
            original_error.add_note(f"Artifact writing also failed: {artifact_error}")
        finally:
            summary["finished_at"] = datetime.now(timezone.utc).isoformat()
            summary["profile_artifacts"] = [
                name
                for name in ("extraction.prof", "pstats.txt")
                if (directory / name).exists()
            ]
            try:
                recorder.checkpoint(summary)
            finally:
                recorder.close()


def save_artifacts(
    summary: dict[str, Any],
    recorder: Recorder,
    standings: pd.DataFrame,
    players: pd.DataFrame | None,
    profiler: cProfile.Profile,
    profile: bool,
) -> None:
    """Serialize retained results after measurement, including partial extraction."""
    directory = recorder.directory
    summary["failed_teams"] = [
        team["team_name"] for team in recorder.teams if "error" in team
    ]
    summary["inactive_players"] = {
        str(team["team_id"]): team["inactive_players"]
        for team in recorder.teams
        if team.get("inactive_players")
    }
    summary["roster_entry_counts"] = {
        team: len(entries) for team, entries in recorder.rosters.items()
    }
    if not standings.empty:
        summary["standings_snapshot"] = snapshot(standings, directory, "standings")
    if players is None and recorder.frames:
        players = pd.concat(recorder.frames.values())
        summary["partial_player_snapshot"] = True
    if players is not None:
        summary["player_snapshot"] = snapshot(players, directory, "player_stats")
        summary["unique_player_ids"] = int(players.index.nunique())
        summary["memberships"] = [
            {"team_id": int(team), "player_id": int(player)}
            for team, player in zip(players["team_id"], players.index, strict=True)
        ]
    write_json(directory / "rosters.json", recorder.rosters)
    if profile:
        profiler.dump_stats(str(directory / "extraction.prof"))
        with (directory / "pstats.txt").open("w") as stream:
            stats = pstats.Stats(profiler, stream=stream)
            stats.sort_stats(pstats.SortKey.CUMULATIVE).print_stats(30)
            stats.sort_stats(pstats.SortKey.TIME).print_stats(30)


def supervise(
    command: list[str], directory: Path, deadline_seconds: float = 1800
) -> dict[str, Any]:
    """Retain checkpoints on failures and terminate only this attempt's process group."""
    path = directory / "summary.json"
    initial: dict[str, Any] = {
        "run_id": directory.name,
        "outcome": "running",
        "wall_seconds": None,
        "process_cpu_seconds": None,
        "extraction_completed": False,
        "validation_status": "not_run",
        "profile_artifacts": [],
    }
    write_json(path, initial)
    environment = {**os.environ, "PYTHONPATH": str(ROOT)}
    started = time.perf_counter()
    with (directory / "console.log").open("w") as console:
        try:
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                env=environment,
                stdout=console,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            initial.update(
                outcome="failed",
                current_stage="worker_start",
                error=error_details(error),
                supervised_seconds=time.perf_counter() - started,
                deadline_seconds=deadline_seconds,
                returncode=None,
            )
            write_json(path, initial)
            return initial
        timed_out = False
        try:
            process.wait(timeout=deadline_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    summary: dict[str, Any] = json.loads(path.read_text())
    summary["supervised_seconds"] = time.perf_counter() - started
    summary["deadline_seconds"] = deadline_seconds
    summary["returncode"] = process.returncode
    if timed_out:
        summary["outcome"] = "timed_out"
        summary["interrupted_error"] = summary.get("error")
        summary["error"] = {
            "type": "TimeoutExpired",
            "message": "Attempt deadline exceeded",
        }
    elif process.returncode != 0 or summary["outcome"] == "running":
        summary["outcome"] = "failed"
        summary.setdefault(
            "error", {"type": "ChildProcessError", "message": "See console.log"}
        )
    write_json(path, summary)
    return summary


def run_supervised(league: str, profile: bool, output_root: Path) -> tuple[Path, dict]:
    mode = "profile" if profile else "benchmark"
    run_id = f"{league}-{mode}-workers1-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    directory = (output_root / run_id).resolve()
    directory.mkdir(parents=True)
    print(f"Extraction artifacts: {directory}", flush=True)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--directory",
        str(directory),
        "--league",
        league,
    ]
    if profile:
        command.append("--profile")
    summary = supervise(command, directory)
    print(f"{summary['outcome']}: {directory}", flush=True)
    return directory, summary


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, deadline_signal)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--league", choices=["american_league", "national_league"], required=True
    )
    parser.add_argument("--profile", action="store_true")
    arguments = parser.parse_args()
    run_extraction(arguments.directory, arguments.league, arguments.profile)
