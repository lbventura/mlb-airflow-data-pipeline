import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from types import SimpleNamespace

import pandas as pd
import pytest
import requests  # type: ignore[import-untyped]

from integration.extraction_profile import Recorder, run_extraction, supervise
from integration import extraction_profile as profiling
from mlb_airflow_data_pipeline import statsapi_extraction_script as extraction


def test_recorder_forwards_calls_and_retains_http_errors(tmp_path: Path) -> None:
    calls = []
    response = requests.Response()
    response.status_code = 503
    failure = requests.HTTPError("unavailable", response=response)

    def api(*args: Any, **kwargs: Any) -> dict:
        calls.append((args, kwargs))
        if len(calls) == 2:
            raise failure
        return {"people": []}

    recorder = Recorder(tmp_path, api)
    recorder.context.stage = "player_extraction"
    recorder.context.team_id = 147
    params = {"personId": 123}
    assert recorder.get("person", params, request_kwargs={"timeout": 7}) == {
        "people": []
    }
    with pytest.raises(requests.HTTPError) as caught:
        recorder.get("person", params)
    assert caught.value is failure
    assert calls == [
        (("person", params), {"request_kwargs": {"timeout": 7}}),
        (("person", params), {}),
    ]
    stats = recorder.endpoints()["person"]
    assert stats["count"] == 2
    assert stats["started_count"] == stats["completed_count"] == 2
    assert recorder.calls[0]["stage"] == "player_extraction"
    assert recorder.calls[0]["team_id"] == 147
    assert stats["exceptions"] == {"HTTPError:503": 1}
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
    ]
    assert [event["event"] for event in events] == [
        "api_started",
        "api_completed",
        "api_started",
        "api_completed",
    ]
    recorder.journal.close()


def test_recorder_does_not_hold_lock_during_requests(tmp_path: Path) -> None:
    barrier = threading.Barrier(2, timeout=5)

    def api(*args: Any) -> dict:
        barrier.wait()
        return {}

    recorder = Recorder(tmp_path, api)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(recorder.get, "person", {"personId": pid}) for pid in (1, 2)
        ]
        assert [future.result(timeout=10) for future in futures] == [{}, {}]
    assert recorder.endpoints()["person"]["count"] == 2
    assert sorted(call["call_id"] for call in recorder.calls) == [1, 2]
    recorder.journal.close()


@pytest.fixture
def sample_extraction(monkeypatch: pytest.MonkeyPatch) -> None:
    def setup(self: extraction.DataExtractor) -> None:
        extraction.statsapi.get("team_roster", {"teamId": 147, "season": 2023})
        self.league_standings = pd.DataFrame({"team_id": [147], "name": ["Yankees"]})
        self.league_team_roster_players = {147: {123: "Sample Player"}}

    def team(self: extraction.DataExtractor, team_id: int) -> tuple[pd.DataFrame, dict]:
        extraction.statsapi.get("person", {"personId": 123})
        row: dict[str, Any] = {
            name: "1" for name in extraction.expected_output_columns()
        }
        row.update(playername="Sample Player", team_id=team_id)
        row.pop("date")
        return pd.DataFrame([row], index=[123]), {}

    def api(endpoint: str, params: dict) -> dict:
        if endpoint == "team_roster":
            return {"roster": [{"person": {"id": 123, "fullName": "Sample Player"}}]}
        return {}

    monkeypatch.setattr(extraction.statsapi, "get", api)
    monkeypatch.setattr(
        extraction.DataExtractor, "set_league_team_roster_players", setup
    )
    monkeypatch.setattr(
        extraction.DataExtractor, "get_player_stats_dataframe_per_team", team
    )


@pytest.mark.parametrize("profile", [False, True])
def test_direct_runner_keeps_identity_and_persistence(
    tmp_path: Path, sample_extraction: None, profile: bool
) -> None:
    result = run_extraction(tmp_path, "american_league", profile, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert result["outcome"] == saved["outcome"] == "success"
    assert saved["data_source"] == "fixture"
    assert saved["extraction_completed"] is True
    assert saved["validation_status"] == "passed"
    assert saved["source_unchanged"] is True
    assert Path(saved["module_paths"]["extraction"]).is_relative_to(profiling.ROOT)
    assert saved["endpoints"]["person"]["count"] == 1
    assert saved["coverage_differences"] == {}
    assert saved["memberships"] == [{"team_id": 147, "player_id": 123}]
    assert list(saved["stage_seconds"]) == [
        "standings_and_rosters",
        "standings_write",
        "team_mapping",
        "player_extraction",
        "player_write",
    ]
    frame = pd.read_pickle(tmp_path / "player_stats.pkl")
    assert frame.index.tolist() == [123]
    assert frame["playername"].tolist() == ["Sample Player"]
    assert (tmp_path / "extraction.prof").exists() is profile
    assert (tmp_path / "pstats.txt").exists() is profile


def test_setup_failure_saves_diagnostic_artifacts(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = requests.ConnectionError("offline")

    def broken_api(*args: Any, **kwargs: Any) -> Any:
        raise failure

    monkeypatch.setattr(extraction.statsapi, "get", broken_api)
    with pytest.raises(requests.ConnectionError) as caught:
        run_extraction(tmp_path, "american_league", True, "fixture")
    assert caught.value is failure
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["outcome"] == "failed"
    assert saved["current_stage"] == "standings_and_rosters"
    assert saved["extraction_completed"] is False
    assert saved["validation_status"] == "not_run"
    assert saved["endpoints"]["team_roster"]["exceptions"] == {
        "ConnectionError:None": 1
    }
    assert (tmp_path / "pstats.txt").stat().st_size > 0


def test_all_team_failure_retains_team_error(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_team(*args: Any) -> Any:
        raise requests.ConnectionError("team request failed")

    monkeypatch.setattr(
        extraction.DataExtractor, "get_player_stats_dataframe_per_team", broken_team
    )
    with pytest.raises(ValueError, match="No objects to concatenate"):
        run_extraction(tmp_path, "american_league", False, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["outcome"] == "failed"
    assert saved["teams"][0]["error"]["type"] == "ConnectionError"
    assert saved["current_stage"] == "player_extraction"


def test_validation_error_retains_output_snapshot(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def mismatched_roster(endpoint: str, params: dict) -> dict:
        return {"roster": [{"person": {"id": 999, "fullName": "Different Player"}}]}

    monkeypatch.setattr(extraction.statsapi, "get", mismatched_roster)
    with pytest.raises(AssertionError, match="roster identity differences"):
        run_extraction(tmp_path, "american_league", False, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["current_stage"] == "validation"
    assert saved["coverage_differences"] == {
        "147": {
            "missing_ids": [999],
            "unexpected_ids": [123],
            "missing_counts": {"999": 1},
            "unexpected_counts": {"123": 1},
        }
    }
    assert saved["extraction_completed"] is True
    assert saved["validation_status"] == "failed"
    assert (tmp_path / "player_stats.pkl").exists()


@pytest.mark.parametrize(
    "code, deadline, expected",
    [
        ("import time; time.sleep(60)", 0.1, "timed_out"),
        ("raise RuntimeError('child failed before checkpoint')", 10, "failed"),
    ],
)
def test_supervisor_records_terminal_failures(
    tmp_path: Path, code: str, deadline: float, expected: str
) -> None:
    result = supervise([sys.executable, "-c", code], tmp_path, deadline)
    assert result["outcome"] == expected
    assert result["returncode"] != 0
    assert json.loads((tmp_path / "summary.json").read_text())["outcome"] == expected


def test_supervisor_records_executable_launch_failure(tmp_path: Path) -> None:
    result = supervise([str(tmp_path / "missing-python")], tmp_path)
    assert result["outcome"] == "failed"
    assert result["current_stage"] == "worker_start"
    assert result["error"]["type"] == "FileNotFoundError"
    assert result["returncode"] is None
    assert result["wall_seconds"] is None
    assert result["profile_artifacts"] == []


def test_roster_duplicate_is_not_hidden_by_set_comparison(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def api(endpoint: str, params: dict) -> dict:
        return {"roster": [{"person": {"id": 123, "fullName": "Sample Player"}}] * 2}

    monkeypatch.setattr(extraction.statsapi, "get", api)
    with pytest.raises(AssertionError, match="roster identity differences"):
        run_extraction(tmp_path, "american_league", False, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["coverage_differences"]["147"]["missing_counts"] == {"123": 1}


def test_persisted_value_corruption_fails_validation(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    insert = profiling.insert_dataframe

    def corrupt(conn: Any, table: str, frame: pd.DataFrame) -> None:
        if table == "player_stats":
            frame = frame.assign(playername="Corrupted Name")
        insert(conn, table, frame)

    monkeypatch.setattr(profiling, "insert_dataframe", corrupt)
    with pytest.raises(AssertionError):
        run_extraction(tmp_path, "american_league", False, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["validation_status"] == "failed"
    assert saved["extraction_completed"] is True


def test_artifact_error_preserves_original_extraction_error(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = requests.ConnectionError("API unavailable")
    write = profiling.write_json

    def api(*args: Any, **kwargs: Any) -> Any:
        raise failure

    def fail_roster_write(path: Path, data: Any) -> None:
        if path.name == "rosters.json":
            raise OSError("artifact disk error")
        write(path, data)

    monkeypatch.setattr(extraction.statsapi, "get", api)
    monkeypatch.setattr(profiling, "write_json", fail_roster_write)
    with pytest.raises(requests.ConnectionError) as caught:
        run_extraction(tmp_path, "american_league", True, "fixture")
    assert caught.value is failure
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["error"]["type"] == "ConnectionError"
    assert saved["artifact_errors"][0]["type"] == "OSError"
    assert saved["profile_artifacts"] == []
    assert any("artifact disk error" in note for note in failure.__notes__)


def test_journal_error_preserves_request_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = requests.ConnectionError("API unavailable")

    def api(*args: Any, **kwargs: Any) -> Any:
        raise failure

    recorder = Recorder(tmp_path, api)
    journal = recorder.journal
    writes = 0

    def write(line: str) -> int:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("journal disk error")
        return journal.write(line)

    monkeypatch.setattr(
        recorder,
        "journal",
        SimpleNamespace(write=write, flush=journal.flush, close=journal.close),
    )
    try:
        with pytest.raises(requests.ConnectionError) as caught:
            recorder.get("person", {"personId": 123})
        assert caught.value is failure
        assert recorder.artifact_errors[0]["type"] == "OSError"
    finally:
        recorder.close()


def test_interrupted_request_is_counted_separately(tmp_path: Path) -> None:
    def api(*args: Any, **kwargs: Any) -> Any:
        raise profiling.AttemptDeadline("deadline")

    recorder = Recorder(tmp_path, api)
    try:
        with pytest.raises(profiling.AttemptDeadline):
            recorder.get("person", {"personId": 123})
        result = recorder.endpoints()["person"]
        assert result["started_count"] == result["interrupted_count"] == 1
        assert result["completed_count"] == result["in_flight_count"] == 0
    finally:
        recorder.close()


def test_artifact_error_fails_otherwise_successful_run(
    tmp_path: Path, sample_extraction: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_snapshot(*args: Any, **kwargs: Any) -> Any:
        raise OSError("snapshot disk error")

    monkeypatch.setattr(profiling, "snapshot", broken_snapshot)
    with pytest.raises(OSError, match="snapshot disk error"):
        run_extraction(tmp_path, "american_league", False, "fixture")
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["outcome"] == "failed"
    assert saved["extraction_completed"] is True
    assert saved["validation_status"] == "passed"
    assert saved["current_stage"] == "artifact_output"


def test_checkpoint_error_preserves_stage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = requests.ConnectionError("stage failed")
    recorder = Recorder(tmp_path, lambda *args: {})
    summary: dict[str, Any] = {"stage_seconds": {}}
    write = profiling.write_json
    writes = 0

    def fail_second_write(path: Path, data: Any) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("checkpoint disk error")
        write(path, data)

    monkeypatch.setattr(profiling, "write_json", fail_second_write)
    try:
        with pytest.raises(requests.ConnectionError) as caught:
            with profiling.stage("player_extraction", summary, recorder):
                raise failure
        assert caught.value is failure
        assert summary["artifact_errors"][0]["type"] == "OSError"
    finally:
        recorder.close()


def test_in_flight_request_has_no_completed_duration(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path, lambda *args: {})
    recorder.started_calls.append({"endpoint": "person"})
    try:
        result = recorder.endpoints()["person"]
        assert result["started_count"] == result["in_flight_count"] == 1
        assert result["completed_count"] == 0
        assert result["total_seconds"] is None
        assert result["median_seconds"] is None
        assert result["p95_seconds"] is None
        assert result["maximum_seconds"] is None
    finally:
        recorder.close()
