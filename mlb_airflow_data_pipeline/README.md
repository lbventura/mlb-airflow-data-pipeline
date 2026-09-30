The file structure is:

1. `statsapi_extraction_script.py`, which contains the interactions with the MLB statsapi through the functions:
    * `set_league_division_standings`;
    * `set_league_team_roster_players`;
    * `get_player_stats_dataframe_per_team`;
    * `get_player_stats_per_league`, and stores complete league/date snapshots in SQLite through `run_extraction`.
2. `statsapi_treatment_script.py`, which reads one league/date from SQLite and generates batter, pitcher, and defender CSV files with extra features;
3. `statsapi_analysis_script.py`, which reads the treated data from `statsapi_treatment_script.py` and creates several scatter plots to be used in the report;
4. `statsapi_time_series_creation_analysis_script.py`. This reads the all the batter data saved in `data` and generates time-series charts for several features;
5. `statsapi_reporting_notebook.ipynb`, which creates an automated HTML report, stored in `report`;
6. `statsapi_parameters_script.py`, which contains the relevant parameters for the execution of the data pipeline.
7. `statsapi_feature_utils.py` creates the extra features;

Extraction replaces both tables' rows for one league and date in a single transaction.
Failed teams or database writes leave the previous snapshot intact. Standings use
`(league_name, date, team_id)` as their key; player rows also include `player_id`.
Every key field is non-null. Repeated incoming keys keep the last extracted row.

Before upgrading an existing database, extraction creates a SQLite backup beside it
with the suffix `.pre-issue-62.bak`; an existing backup is never overwritten.
The first successful migration copies each original table into `legacy_league_standings`
or `legacy_player_stats`. Existing archives are preserved and receive the source rows.
The active tables are rebuilt with non-null primary keys. Fully keyed historical
rows survive, identical copies collapse, and rows with missing keys or conflicting
values remain only in the archives. Later saves leave archives unchanged and do not
rescan history. Migration and snapshot replacement roll back together on failure.

Treatment reads only active tables. Before recovering archived history, work on a
database copy and verify the league, date, team, and player IDs from trustworthy run
records. Conflicting values require evidence of which snapshot they belong to;
SQLite row order is not evidence of extraction time. Restore a verified complete
league/date snapshot through `save_extraction_run`, with player IDs in the DataFrame
index, after preserving the backup. If the metadata cannot be recovered, keep the
rows archived. Fetching current statistics and assigning an old date does not
reconstruct a historical snapshot.
