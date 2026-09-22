from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

default_args = {"start_date": datetime(2023, 4, 7)}

dag_nl = DAG(
    "mlb-airflow-data-pipeline-reporting-nl-dag",
    default_args=default_args,
    schedule="30 17 * * 1",
    catchup=False,
)

t0 = BashOperator(
    task_id="set_league_name_task",
    bash_command="echo 'national_league' > league_name_choice.txt",
    cwd=str(PROJECT_ROOT),
    dag=dag_nl,
)

t1 = BashOperator(
    task_id="time_series_creation_task",
    bash_command="bash bash/statsapi_time_series_generation_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_nl,
)

t2 = BashOperator(
    task_id="report_task",
    bash_command="bash bash/statsapi_reporting_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_nl,
)

t3 = BashOperator(
    task_id="rename_task",
    bash_command="bash bash/statsapi_name_change_nl_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_nl,
)

t0 >> t1 >> t2 >> t3
