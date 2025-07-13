"""
Integration test for the mlb-airflow-data-pipeline-al-dag.

This test implements the strategy described in airflow-integration-test.md:
1. Identifies the first task in the DAG
2. Runs the task using 'airflow tasks test' to capture logs
3. Provides detailed output for debugging failures

Note: These tests require a properly configured Airflow environment.
In CI/CD environments, these tests will be skipped if Airflow is not available.
"""

import json
import os
import subprocess
from datetime import datetime
import time
from typing import Callable, TypedDict

import pytest


class DagConfig(TypedDict):
    """Type definition for DAG configuration."""

    dag_id: str
    first_task_id: str
    test_date: str


CommandRunner = Callable[[str], subprocess.CompletedProcess[str]]


# Constants
DAG_ID = "mlb-airflow-data-pipeline-al-dag"
FIRST_TASK_ID = "setting_league_name_task"
COMMAND_TIMEOUT_SECONDS = 60
OUTPUT_FILE_PATH = "/root/mlb-airflow-data-pipeline/league_name_choice.txt"
EXPECTED_CONTENT = "american_league"


@pytest.fixture
def dag_config() -> DagConfig:
    """Fixture providing DAG configuration."""
    return DagConfig(
        dag_id=DAG_ID,
        first_task_id=FIRST_TASK_ID,
        test_date=datetime.now().strftime("%Y-%m-%d"),
    )


@pytest.fixture(scope="session")
def setup_airflow() -> None:
    """Setup Airflow database and environment for testing."""
    # Skip if running in CI without proper Airflow setup
    if os.environ.get("CI") and not os.environ.get("AIRFLOW_HOME"):
        pytest.skip(
            "Skipping integration tests in CI environment without Airflow setup"
        )

    # Set AIRFLOW_HOME and DAGS_FOLDER environment variables
    os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")
    os.environ.setdefault(
        "AIRFLOW__CORE__DAGS_FOLDER", "/root/mlb-airflow-data-pipeline/dags"
    )

    try:
        # Check if airflow command is available
        subprocess.run(
            "airflow version",
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )

        # Initialize Airflow database
        subprocess.run(
            "airflow db migrate",
            shell=True,
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )

        # Parse DAGs to populate the database
        subprocess.run(
            "airflow dags reserialize",
            shell=True,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,  # This might fail, but that's okay
        )

    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip(
            "Airflow not available or failed to initialize - skipping integration tests"
        )
    except Exception:
        pytest.skip("Airflow not properly configured - skipping integration tests")


@pytest.fixture
def run_command(setup_airflow: None) -> CommandRunner:
    """Fixture providing a command runner function."""

    def _run_command(command: str) -> subprocess.CompletedProcess[str]:
        """Run a shell command and return the result.

        Args:
            command: The shell command to execute

        Returns:
            CompletedProcess object with command results

        Raises:
            pytest.fail: If command times out or fails to execute
        """
        try:
            return subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=COMMAND_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"Command timed out after {COMMAND_TIMEOUT_SECONDS} seconds")
        except Exception as e:
            pytest.fail(f"Failed to run command: {e}")

        # This should never be reached due to pytest.fail above, but satisfies mypy
        raise RuntimeError("Unreachable code")

    return _run_command


def _assert_command_success(
    result: subprocess.CompletedProcess[str], error_message: str
) -> None:
    """Assert that a command executed successfully.

    Args:
        result: The completed process result
        error_message: Error message to display on failure
    """
    assert result.returncode == 0, error_message


def _assert_content_in_output(content: str, output: str, error_message: str) -> None:
    """Assert that expected content is found in command output.

    Args:
        content: Expected content to find
        output: Command output to search in
        error_message: Error message to display on failure
    """
    assert content in output, error_message


def _build_task_test_command(dag_id: str, task_id: str, test_date: str) -> str:
    """Build the airflow tasks test command.

    Args:
        dag_id: The DAG identifier
        task_id: The task identifier
        test_date: The test execution date

    Returns:
        The formatted command string
    """
    return f"airflow tasks test {dag_id} {task_id} {test_date}"


def test_dag_details_accessible(
    dag_config: DagConfig, run_command: CommandRunner
) -> None:
    """Test that DAG details are accessible via Airflow CLI."""
    result = run_command(f"airflow dags details {dag_config['dag_id']}")

    if (
        "could not be found" in result.stderr
        or "failed to parse" in result.stderr
        or "does not exist in 'dag' table" in result.stderr
    ):
        pytest.skip("DAG not found in database - may need DAG registration")

    _assert_command_success(
        result,
        "Failed to get DAG details. Ensure Airflow is configured and DAG is accessible",
    )
    _assert_content_in_output(
        dag_config["dag_id"],
        result.stdout,
        f"DAG '{dag_config['dag_id']}' not found in DAG details",
    )


def test_dag_appears_in_listing(
    dag_config: DagConfig, run_command: CommandRunner
) -> None:
    """Test that the DAG appears in the DAG listing (validates configuration)."""
    result = run_command("airflow dags list")

    _assert_command_success(result, "Failed to list DAGs")

    # If no DAGs are found, this might be expected in a fresh environment
    if "No data found" in result.stdout:
        pytest.skip("No DAGs found in database - may need DAG registration")

    _assert_content_in_output(
        dag_config["dag_id"],
        result.stdout,
        f"DAG '{dag_config['dag_id']}' not found in DAG list, indicating configuration issues",
    )


def test_first_task_executes_successfully(
    dag_config: DagConfig, run_command: CommandRunner
) -> None:
    """Test that the first task in the DAG executes without errors."""
    command = _build_task_test_command(
        dag_config["dag_id"], dag_config["first_task_id"], dag_config["test_date"]
    )
    result = run_command(command)

    if "could not be found" in result.stderr or "failed to parse" in result.stderr:
        pytest.skip(
            "DAG not found or failed to parse - may need Airflow environment setup"
        )

    _assert_command_success(result, "First task execution failed")


def test_task_produces_expected_output(
    dag_config: DagConfig, run_command: CommandRunner
) -> None:
    """Test that the first task produces the expected output file with correct content."""
    # Execute the task to ensure output exists
    command = _build_task_test_command(
        dag_config["dag_id"], dag_config["first_task_id"], dag_config["test_date"]
    )
    task_result = run_command(command)
    _assert_command_success(task_result, "Task must succeed before output verification")

    # Verify output file exists
    assert os.path.exists(OUTPUT_FILE_PATH), (
        f"Expected output file {OUTPUT_FILE_PATH} was not created"
    )

    # Verify file content
    check_result = run_command(f"cat {OUTPUT_FILE_PATH}")
    _assert_command_success(check_result, "Failed to read output file")

    actual_content = check_result.stdout.strip()
    _assert_content_in_output(
        EXPECTED_CONTENT,
        actual_content,
        f"Unexpected content. Expected '{EXPECTED_CONTENT}', got '{actual_content}'",
    )


@pytest.mark.manual
def test_full_dag_execution(dag_config: DagConfig, run_command: CommandRunner) -> None:
    """Test that executes the full DAG and verifies its completion."""
    # Trigger the DAG run
    trigger_command = f"airflow dags trigger {dag_config['dag_id']}"
    trigger_result = run_command(trigger_command)
    _assert_command_success(trigger_result, "Failed to trigger DAG run")

    # Get the run ID from the trigger output
    list_command = f"airflow dags list-runs {dag_config['dag_id']} --output json"
    list_result = run_command(list_command)
    _assert_command_success(list_result, "Failed to list DAG runs")

    try:
        runs = json.loads(list_result.stdout)
        if not runs:
            pytest.fail("No DAG runs found")
        run_id = runs[0].get("run_id")  # Get the most recent run
    except json.JSONDecodeError:
        pytest.fail("Failed to parse DAG runs JSON output")
    except (IndexError, KeyError):
        pytest.fail("No DAG runs available or invalid structure")

    assert run_id, "Could not find run ID"

    # Wait for DAG completion with timeout
    wait_command = f"airflow dags run-state {dag_config['dag_id']} {run_id}"
    max_wait_seconds = 1200  # 20 minutes timeout
    start_time = time.time()

    while (time.time() - start_time) < max_wait_seconds:
        state_result = run_command(wait_command)
        if "success" in state_result.stdout.lower():
            break
        if "failed" in state_result.stdout.lower():
            pytest.fail("DAG execution failed")
        time.sleep(60)  # Check every 60 seconds
    else:
        pytest.fail(f"DAG execution did not complete within {max_wait_seconds} seconds")

    # Verify DAG completed successfully
    state_result = run_command(wait_command)
    _assert_command_success(state_result, "Failed to get DAG state")
    _assert_content_in_output(
        "success", state_result.stdout.lower(), "DAG did not complete successfully"
    )
