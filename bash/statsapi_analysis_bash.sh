#!/bin/bash
cd "$(dirname "$0")/.."
micromamba run -n mlb-airflow-env python mlb_airflow_data_pipeline/statsapi_analysis_script.py
