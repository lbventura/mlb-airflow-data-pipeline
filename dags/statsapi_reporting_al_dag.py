from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

default_args = {"start_date": datetime(2023, 4, 7)}

dag_al = DAG(
    "mlb-airflow-data-pipeline-reporting-al-dag",
    default_args=default_args,
    schedule="0 17 * * 1",
    catchup=False,
)

t0 = BashOperator(
    task_id="set_league_name_task",
    bash_command="echo 'american_league' > league_name_choice.txt",
    cwd=str(PROJECT_ROOT),
    dag=dag_al,
)

t1 = BashOperator(
    task_id="time_series_creation_task",
    bash_command="bash bash/statsapi_time_series_generation_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_al,
)

t2 = BashOperator(
    task_id="report_task",
    bash_command="bash bash/statsapi_reporting_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_al,
)


t3 = BashOperator(
    task_id="rename_task",
    bash_command="bash bash/statsapi_name_change_al_bash.sh ",
    cwd=str(PROJECT_ROOT),
    dag=dag_al,
)

t0 >> t1 >> t2 >> t3
