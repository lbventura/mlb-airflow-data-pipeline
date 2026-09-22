#!/bin/bash
cd "$(dirname "$0")/.."
mv -f mlb_airflow_data_pipeline/statsapi_reporting_notebook.html mlb_airflow_data_pipeline/report/$(date +"%Y_%m_%d")_statsapi_reporting_notebook_al.html
