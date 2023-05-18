A data pipeline which allows for automatic, fast and reliable extraction, manipulation and representation of data from the Major League Baseball (MLB) Statistics API (statsapi). This involves:

1. Extracting raw data from statsapi and pre-process it;
2. Augmenting the current set of statistics;
3. Summarizing results in an automatically-generated HTML report with a relevant description, tables and charts.

To achieve this, the following tools are be used, with Python as the programming language:

1. The package [`statsapi`](https://github.com/toddrob99/MLB-StatsAPI), which writes HTTP requests to the MLB API. This simplifies data collection and allows us to focus on extracting the data from these requests and treating it;
2. Pandas for creating data tables and generating new features;
3. Matplotlib for representing the results;

Together with Airflow for orchestration.

The working repository is composed of the following folders:

1. `bash`, containing the bash commands called by the Airflow BashOperators in orchestration;
2. `dags`, containing the Airflow DAGs which schedule the daily runs of the American and National League data pipelines and the weekly runs of the reporting pipelines. This folder should be placed in the `Airflow` folder after Airflow is installed;
3. `mlb_airflow_data_pipeline`, containing the scripts which perform the extraction, manipulation and representation of data. See README.md there for details;
