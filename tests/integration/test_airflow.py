"""Integration tests for the American League Airflow DAG."""

import os
import shutil
import signal
import socket
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest


DAG_ID = "mlb-airflow-data-pipeline-al-dag"
FIRST_TASK_ID = "setting_league_name_task"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_FILE_PATH = PROJECT_ROOT / "league_name_choice.txt"
TEST_DATE = datetime.now().strftime("%Y-%m-%d")
CLI_TIMEOUT_SECONDS = 60
DAG_RUN_TIMEOUT_SECONDS = 20 * 60
DAG_STATE_POLL_INTERVAL_SECONDS = 5


def airflow(*args: str, timeout: int = CLI_TIMEOUT_SECONDS) -> str:
    """Run the Airflow CLI and return stdout."""
    result = subprocess.run(
        ["airflow", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return result.stdout


@pytest.fixture(scope="session", autouse=True)
def setup_airflow(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Initialize Airflow with temporary local data when it is available."""
    if os.environ.get("CI") and not os.environ.get("AIRFLOW_HOME"):
        pytest.skip("Airflow integration tests need AIRFLOW_HOME in CI")
    if shutil.which("airflow") is None:
        pytest.skip("Airflow is not installed")

    os.environ.setdefault(
        "AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow-home"))
    )
    os.environ.setdefault(
        "MLB_AIRFLOW_DATA_DIR", str(tmp_path_factory.mktemp("mlb-airflow-data"))
    )
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(PROJECT_ROOT / "dags"))

    airflow("db", "migrate", timeout=120)
    airflow("dags", "reserialize")


@pytest.fixture
def airflow_runtime(
    setup_airflow: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Run the Airflow components needed by a full DAG run."""
    # Keep the daily schedule from starting a second run alongside the manual run.
    monkeypatch.setenv("AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP", "0")
    processes: list[subprocess.Popen[bytes]] = []
    try:
        for component in ("api-server", "dag-processor", "scheduler"):
            processes.append(
                subprocess.Popen(
                    ["airflow", component],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )

        api_port = int(os.environ.get("AIRFLOW__API__PORT", "8080"))
        for _ in range(30):
            if any(process.poll() is not None for process in processes):
                pytest.fail("An Airflow runtime component exited during startup")

            jobs_ready = all(
                subprocess.run(
                    ["airflow", "jobs", "check", "--job-type", job_type, "--local"],
                    capture_output=True,
                    timeout=10,
                    check=False,
                ).returncode
                == 0
                for job_type in ("DagProcessorJob", "SchedulerJob")
            )
            with socket.socket() as connection:
                connection.settimeout(1)
                api_ready = connection.connect_ex(("127.0.0.1", api_port)) == 0

            if jobs_ready and api_ready:
                break
            time.sleep(1)
        else:
            pytest.fail("Airflow runtime did not become ready within 30 seconds")

        yield
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                process.wait(timeout=30)


def test_dag_details_accessible() -> None:
    assert DAG_ID in airflow("dags", "details", DAG_ID)


def test_dag_appears_in_listing() -> None:
    assert DAG_ID in airflow("dags", "list")


def test_first_task_produces_expected_output() -> None:
    airflow("tasks", "test", DAG_ID, FIRST_TASK_ID, TEST_DATE)
    assert OUTPUT_FILE_PATH.read_text().strip() == "american_league"


@pytest.mark.manual
def test_full_dag_execution(airflow_runtime: None) -> None:
    airflow("dags", "unpause", DAG_ID)

    run_id = f"manual__pytest_{uuid4().hex}"
    airflow("dags", "trigger", "--run-id", run_id, DAG_ID)

    deadline = time.monotonic() + DAG_RUN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        state = airflow("dags", "state", DAG_ID, run_id).strip().lower()
        if "success" in state:
            return
        if "failed" in state:
            pytest.fail(f"DAG execution failed: {state}")
        time.sleep(DAG_STATE_POLL_INTERVAL_SECONDS)

    pytest.fail(f"DAG did not complete within {DAG_RUN_TIMEOUT_SECONDS} seconds")
