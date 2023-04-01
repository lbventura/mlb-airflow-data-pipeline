from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"start_date": datetime(2023, 4, 7)}

dag_al = DAG(
    "mlb-airflow-data-pipeline-reporting-al-dag",
    default_args=default_args,
    schedule_interval="0 17 * * 1",
    catchup=False,
)

t0 = BashOperator(
    task_id="set_league_name_task",
    bash_command="echo 'american_league' > /root/mlb-airflow-data-pipeline/league_name_choice.txt",
    dag=dag_al,
)

t1 = BashOperator(
    task_id="time_series_creation_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_time_series_generation_bash.txt",
    dag=dag_al,
)

t2 = BashOperator(
    task_id="report_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_reporting_bash.txt",
    dag=dag_al,
)


t3 = BashOperator(
    task_id="rename_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_name_change_al_bash.txt",
    dag=dag_al,
)

t0 >> t1 >> t2 >> t3
