from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"start_date": datetime(2023, 4, 1)}

# national league dag
dag_nl = DAG(
    "mlb-airflow-data-pipeline-nl-dag",
    default_args=default_args,
    schedule="30 08 * * *",
    catchup=False,
)

t0 = BashOperator(
    task_id="setting_league_name_task",
    bash_command="echo 'national_league' > /root/mlb-airflow-data-pipeline/league_name_choice.txt",
    dag=dag_nl,
)


t1 = BashOperator(
    task_id="extraction_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_extraction_bash.txt",
    dag=dag_nl,
)

t2 = BashOperator(
    task_id="treatment_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_treatment_bash.txt",
    dag=dag_nl,
)

t3 = BashOperator(
    task_id="analysis_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_analysis_bash.txt",
    dag=dag_nl,
)

t0 >> t1 >> t2 >> t3
