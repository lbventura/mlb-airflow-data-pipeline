from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"start_date": datetime(2023, 4, 1)}

# american league dag
dag_al = DAG(
    "mlb-airflow-data-pipeline-al-dag",
    default_args=default_args,
    schedule_interval="0 09 * * *",
    catchup=False,
)

t0 = BashOperator(
    task_id="setting_league_name_task",
    bash_command="echo 'american_league' > /root/mlb-airflow-data-pipeline/league_name_choice.txt",
    dag=dag_al,
)


t1 = BashOperator(
    task_id="extraction_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_extraction_bash.txt",
    dag=dag_al,
)

t2 = BashOperator(
    task_id="treatment_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_treatment_bash.txt",
    dag=dag_al,
)

t3 = BashOperator(
    task_id="analysis_task",
    bash_command="bash /root/mlb-airflow-data-pipeline/bash/statsapi_analysis_bash.txt",
    dag=dag_al,
)

t0 >> t1 >> t2 >> t3
