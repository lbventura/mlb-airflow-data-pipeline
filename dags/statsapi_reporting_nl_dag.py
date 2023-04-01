from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"start_date": datetime(2023, 4, 7)}

dag_nl = DAG(
    "mlb-airflow-data-pipeline-reporting-nl-dag",
    default_args=default_args,
    schedule_interval="30 17 * * 1",
    catchup=False,
)

t0 = BashOperator(
    task_id="set_league_name_task",
    bash_command="echo 'national_league' > /root/mlb-airflow-data-pipeline/league_name_choice.txt",
    dag=dag_nl,
)

t1 = BashOperator(
    task_id="time_series_creation_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_time_series_generation_bash.txt",
    dag=dag_nl,
)

t2 = BashOperator(
    task_id="report_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_reporting_bash.txt",
    dag=dag_nl,
)

t3 = BashOperator(
    task_id="rename_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_name_change_nl_bash.txt",
    dag=dag_nl,
)

t0 >> t1 >> t2 >> t3
