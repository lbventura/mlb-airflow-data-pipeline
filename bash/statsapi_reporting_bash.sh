#!/bin/bash
cd /root/mlb-airflow-data-pipeline
micromamba run -n mlb-airflow-env jupyter nbconvert --execute mlb_airflow_data_pipeline/statsapi_reporting_notebook.ipynb --TemplateExporter.exclude_input=True --to html
