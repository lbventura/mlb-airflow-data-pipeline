# Machine Moneyball - Extracting insights from Major League Baseball's data - Part 3

This is the second part of a multi-part series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report [1](https://github.com/lbventura/mlb-airflow-data-pipeline). The previous can be read in

- Part 1: Introduction
- Part 2: A very (very!) short introduction to baseball

Following these, we have a good understanding of what the project goals are and what is the game that we are trying to describe. Let us now look at how the first part of the data pipeline is built, as represented by a directed acyclic graph (DAG) represented in Airflow

[DAG image or a diagram]

which is composed of `bash` operators:

1. `setting_league_name_task` - This simply writes a league name (either "American League" or "National League") to a text file, which is itself used to define for which league the code runs;
2. `extraction_task` - For a given league, it extracts team and player information;
3. `treatment_tasks` - Reads the data collected in the second operator and treats it;
4. `analysis_task` - Reads the treated data and generates images representing pairs of important statistics which provide insights about the game;

The operators in points 2 through 4 are actually activating a virtual environment, running a Python script and deactivating the virtual environment after the script has finished.

## Extracting information - `extraction_task`

In more detail, 
    1. Team statistics are obtained using `get_league_division_standings`, which does the API call `statsapi.standings_data`. Iterates through the different leagues (american and national) and through the different divisions (north, south, east, west) and obtains the team rankings, number of wins, losses ...;

    2. Player statistics are obtained by looking up each team (with the call `statsapi.lookup_team`) and extract its rosters (`statsapi.roster`), `get_player_stats_dataframe_per_team`. Since each player has an unique ID (findable through `statsapi.lookup_player`), we can then use this to extract his game statistics (through `statsapi.player_stats`). By combining all the players of all the teams, we obtain player data for the whole league;

    3. Logging registers whether the request failed to extract information for a given team or a given player.

Because the number of teams (30) and players (around 500) is small, the data is stored in .csv format. 

But before running any processes, it is best to set up a file `statsapi_parameters_script.py` in which all the necessary information is stored:

    1. Day of execution;
    2. Mapping between the two leagues (American and National) and their `statsapi` ID;
    3. The league for which we are extracting the information;
    4. The name and location of the files that store the team and player data;
    5. The name and location of the .png images generated in the analysis step;

This file comes in handy when executing the pipeline automatically in Airflow.


## Treating information - `treatment_tasks`

Because we are later interested in doing statistical analysis of players' performance, we have to focus on players which have played enough times (i.e., a minimum number of games) for there to be an statistically significant assessment of their performance. For example, if I play a total 3 baseball games, scoring 1 home-run in each game, I might be very good or very lucky. However, if I play 150 games and score 1 homerun per game, luck might be playing a role, but it is skill that is driving the results. [This leads to an interesting discussion of randomness in player performance, see more details here]  

Unfortunately, the MLB API has some data consistency issues, one example is in the number of games each player participated in. The image below shows players which had less games on the 6th of October (gamesPlayed) than on the 28th of August (gamesPlayedOld):

[Games Played Inconsistency example]

It is easy to check that the column gamesPlayed is incorrect. Julio Rodriguez, the star center-fielder of the Seattle Mariners, ended his regular season with 129 games. It is very important to be aware of these shortcomings and attempt to minimize their impact. Rather than double-checking the data and correcting it [This would have been a safer approach, but requires a second, correct, information source. We would then just use this second source instead], we checked other stats are proportional to the number of games and filter the universe of players accordingly.

For batters (i.e, attacking players), we use plateAppearances - the number of times you were in this situation -

[Player in the batter box]

For which the number of inconsistent players is much lower than before

[Plate Appearances Consistency example]

After we filtering the data for players which have a minimum number of plate appearances [We used 100 - as a rule of thumb, ...], we are left with a universe of about 150 players [The actual number increases steadily across time as some players get above the threshold]. For these, we generate a new set of statistical features. The actual statistics are beyond the scope of this article, but there are two main ideas:

1. Compute features normalized by the number of plate appearances. This corrects a phenomenon called stat accumulation, i.e, a good player can have larger total number of home-runs, runs-batted-in, ... than a great player simply because it has more plate appearances.
2. Compute features normalized by the league average. This allows a direct comparison of a player with its peers. For a league average normalized at 100, a player with a normalized rbi (on-base-percentage) of 200 has 2 times as many runs batted in as the league average [It is debatable whether the league average or league median should be used here, but we avoid this discussion here.]

Again, this data is stored in a .csv file for further manipulation.