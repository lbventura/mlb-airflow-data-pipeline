# Machine Moneyball - Extracting insights from Major League Baseball's data - Part 3

This is the third part of a series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report automatically [1](https://github.com/lbventura/mlb-airflow-data-pipeline). The previous sections were about

- Part 1: Introduction
- Part 2: A very (very!) short introduction to baseball

Following these, we have a good understanding of what the project goals are and of the game dynamics that we will attempt to quantify. In the next 3 parts, we will be examining how the first section of the data pipeline - extracting, treating and representing data - is built by going into each task in detail. Afterwards, we will delve into the second part of the data pipeline: generating an automated report.

Today the discussion will focus on extracting data and it will be rather technical 🔧. If the details bore you, the next session on treating data will be more conceptual and data-sciency ⚗️.

The first part of the pipeline can be represented by a directed acyclic graph (DAG) represented in Airflow [2](https://airflow.apache.org/) which is composed of `bash` operators:

1. `setting_league_name_task`  - Just writes a league name (either American League or National League) to a text file. This input then defines for which league the code in the repository runs;
2. `extraction_task` - For a given league, it extracts team and player information. It is important to note that the information is contemporaneous. If a team records a win - or a player scores a homerun - from one day to the other, we would pick up that information;
3. `treatment_task` - Reads the data collected in the second operator and treats it;
4. `analysis_task` - Reads the treated data and generates images representing pairs of important statistics which provide insights about the game;

The operators in points 2 through 4 are activating a virtual environment, running a Python script and deactivating the virtual environment after the script has finished. Let us now look with more detail into the second task.

## Extracting data - `extraction_task`

After activating the virtual environment, the `statsapi_extraction_script.py` [3](https://github.com/lbventura/mlb-airflow-data-pipeline/blob/main/statsapi_extraction_script.py) file is executed, performing the following steps:

1. Team statistics are obtained using the function get_league_division_standings, which does the API call statsapi.standings_data. Iterates through the different leagues (American and National) and through the different divisions (North, South, East, West) and obtains the team rankings, number of wins, losses and so forth.


2. Player statistics are obtained by looking up each team (with the call `statsapi.lookup_team`) and extract its rosters (`statsapi.roster`), through the function `get_player_stats_dataframe_per_team`. Since each player has an unique ID (findable through `statsapi.lookup_player`), we can then use this to extract his game statistics (through `statsapi.player_stats`). By combining all the players of all the teams, we obtain player data for the whole league. See details [here](https://github.com/lbventura/mlb-airflow-data-pipeline/blob/052bbf3fbdeafc606fc463e6c4752bfead7d922f/statsapi_extraction_script.py#L96) [4]

3. Logging registers whether the request failed to extract information for a given team or a given player. This is quite useful when figuring out what went wrong directly in Airflow.


4. Because the number of teams (30) and players (around 500) is small, the team and player data are stored in the `.csv` format. There are two advantages of doing so: first, a user which only interested in the data can easily check the data with programs readily available in any computer. Second, Pandas' `read_csv` function infers data types quite well, allowing us to be a bit lazy about enforcing these explicitly. See, however, below.

After these steps, the virtual environment is deactivated. If you skimmed through the repo, you might have seen another file, `statsapi_parameters_script.py` [5](https://github.com/lbventura/mlb-airflow-data-pipeline/blob/main/statsapi_parameters_script.py), which does not perform any operations. It simply stores all the necessary information for executing the pipelines

    1. Day of execution;
    2. Mapping between the two leagues (American and National) and their `statsapi` ID;
    3. The league for which we are extracting the information;
    4. The name and location of the files that store the team and player data;
    5. The name and location of the .png images generated in the analysis step;

This becomes quite useful when used in combination with Airflow orchestration.

## Shortcomings

Reading through the above, you might have identified two (or more! 😅) shortcomings of the current setup, to be addressed in further iterations.

The first one is accessing player data by looking at each team roster. This leads to incomplete statistics when players change teams throughout a season. While this would not affect the statistics of superstar players which change teams infrequently, it can affect the statistics of players with considerable potential performance which have not found the right team yet.

The second is enforcing data types explicitly. In the image above, "Output example for player statistics", all of the stats except for batting average were integer values, which should be guaranteed while manipulating the data rather than relying on Pandas.
In the next section, we will look at the third step of the pipeline, treatment_tasks, where we are handling the information collected by the operator described today. See you then!
