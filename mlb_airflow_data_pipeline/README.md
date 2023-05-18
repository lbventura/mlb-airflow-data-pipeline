The file structure is:

1. `statsapi_extraction_script.py`, which contains the interactions with the MLB statsapi through the functions:
    * `get_league_division_standings`;
    * `get_league_team_rosters_player_names`;
    * `get_player_stats_dataframe_per_team`;
    * `get_player_stats_per_league`, and stores the player and team statistics in `.csv` files.
2. `statsapi_treatment_script.py`, which reads the full player stats data file created in `statsapi_extraction_script.py` and generates a batter-specific stats data file with extra features;
3. `statsapi_analysis_script.py`, which reads the treated data from `statsapi_treatment_script.py` and creates several scatter plots to be used in the report;
4. `statsapi_time_series_creation_analysis_script.py`. This reads the all the batter data saved in `data` and generates time-series charts for several features;
5. `statsapi_reporting_notebook.ipynb`, which creates an automated HTML report, stored in `report`;
6. `statsapi_parameters_script.py`, which contains the relevant parameters for the execution of the data pipeline.
7. `statsapi_feature_utils.py` creates the extra features;
