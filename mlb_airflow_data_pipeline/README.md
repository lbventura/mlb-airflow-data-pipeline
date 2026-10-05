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

Player statistics retain their source group in `hitting_*` and `pitching_*` columns,
and their fielding position in `fielding_<position>_*` columns. Extraction makes one
structured `person` request per roster player. It preserves the split's team metadata
to select combined totals for traded players; a position played for only one team
uses that team's split. Ambiguous splits fail extraction instead of overwriting data.
Season requests use `SEASON_YEAR`; `team_id` describes roster membership, not the scope
of the player's totals.

Treatment reads each role's own statistics. Defender output has separate rows for
`C`, `1B`, `2B`, `3B`, `SS`, `LF`, `CF`, and `RF`; source `P` and `DH` splits remain
stored but are excluded from that output. Undefined numeric rates become missing
values, and roles without eligible players produce empty CSVs with headers.

Existing keyed databases receive grouped columns through the normal transactional
writer. Old unprefixed snapshots remain readable, but their overwritten statistics
cannot be reconstructed from the stored data. No historical values are copied into
the new grouped columns, and no separate fielding table is introduced.

To inspect the grouped schema and fielding data after extraction, open
[`statsapi_database_inspection.ipynb`](statsapi_database_inspection.ipynb)
in Jupyter or VS Code and select the pipeline's Python environment. Run the cells
in order to see schema tables, each league's latest saved snapshot, populated
fielding counts, and sample player rows. Edit the schema prefix, league, position,
and sample limit to investigate specific data.

Zero counts as populated; NULL and blank values do not. Positions a player did not
play are normally empty, as are grouped columns in old snapshots. Source `P` and
`DH` observations are also shown when present. Edit `database_path` to inspect a
different database; the default respects `MLB_AIRFLOW_DATA_DIR`.

Before upgrading an existing table's primary key, extraction creates a SQLite backup beside it
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
