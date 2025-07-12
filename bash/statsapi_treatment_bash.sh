#!/bin/bash
cd /root/mlb-airflow-data-pipeline
micromamba run -p mlb-airflow-env python mlb_airflow_data_pipeline/statsapi_treatment_script.py
