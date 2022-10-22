A data pipeline which allows for automatic, fast and reliable extraction, manipulation and representation of data from the MLB Statistics API (statsapi). This involves:

    1. Extracting raw data from statsapi and pre-process it;
    2. Augmenting the current set of statistics;
    3. Summarizing results in an automatically-generated HTML report with a relevant description, tables and charts.

To achieve this, the following tools will be used, with Python as the programming language:
    
    1. The package [`statsapi`](https://github.com/toddrob99/MLB-StatsAPI), which writes HTTP requests to the MLB API. This simplifies data collection and allows us to focus on extracting the data from these requests and treating it;
    2. Pandas for creating data tables and generating new features;
    3. Matplotlib for representing the results;

Together with Airflow for orchestration, which is crucial to run the data pipelines automatically.

The working repository is composed of the following folders:

    1. `bash`, containing the bash commands called by the Airflow BashOperators in orchestration;
    2. `dags`, containing the Airflow DAGs which schedule the daily runs of the American and National League data pipelines and the weekly runs of the reporting pipelines;
    3. `data`, containing the data files extracted from August 28th 2022 until the end of the regular MLB season, October 5th 2022. There are several files past this date, but these include playoff data;
    4. `output`, containing the .png files generated by `statsapi_analysis_script.py` and `statsapi_time_series_creation_analysis_script.py`;
    5. `report`, containing the HTML files which report on American and National Leagues team and player data;

And of the following files:

    1. `statsapi_extraction_script.py`, which contains the interactions with the MLB statsapi through the functions:
        * `get_league_division_standings`;
        * `get_league_team_rosters_player_names`;
        * `get_player_stats_dataframe_per_team`;
        * `get_player_stats_per_league`;
    and stores the player and team statistics in `.csv` files.
    2. `statsapi_treatment_script.py`, which reads the full player stats data file created in `statsapi_extraction_script.py` and generates a batter-specific stats data file with extra features;
    3. `statsapi_analysis_script.py`, which reads the treated data from `statsapi_treatment_script.py` and creates several scatter plots to be used in the report;
    4. `statsapi_time_series_creation_analysis_script.py`. This reads the all the batter data saved in `data` and generates time-series charts for several features;
    5. `statsapi_reporting_notebook.ipynb`, which creates an automated HTML report, stored in `report`;
    6. `statsapi_parameters_script.py`, which contains the relevant parameters for the execution of the data pipeline.