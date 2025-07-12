"""
Integration test for the mlb-airflow-data-pipeline-al-dag.

This test implements the strategy described in airflow-integration-test.md:
1. Identifies the first task in the DAG
2. Runs the task using 'airflow tasks test' to capture logs
3. Provides detailed output for debugging failures
"""

import os
import subprocess
from datetime import datetime
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


@pytest.fixture
def run_command() -> CommandRunner:
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


def test_complete_integration_workflow(
    dag_config: DagConfig, run_command: CommandRunner
) -> None:
    """Execute the complete integration test workflow following the markdown strategy."""
    # Step 1: Verify DAG accessibility
    details_result = run_command(f"airflow dags details {dag_config['dag_id']}")
    _assert_command_success(details_result, "Failed to get DAG details")

    # Step 2: Execute first task
    task_command = _build_task_test_command(
        dag_config["dag_id"], dag_config["first_task_id"], dag_config["test_date"]
    )
    task_result = run_command(task_command)
    _assert_command_success(task_result, "First task execution failed")

    # Step 3: Validate DAG configuration
    list_result = run_command("airflow dags list")
    _assert_command_success(list_result, "DAG validation failed")
    _assert_content_in_output(
        dag_config["dag_id"], list_result.stdout, "DAG not found in validation step"
    )
