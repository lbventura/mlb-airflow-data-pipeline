# Machine Moneyball - Extracting insights from Major League Baseball's data

On the occasion of the Baseball World Series, I have written a summary of a project I have developed for the past two months - a data pipeline using the Major League Baseball API. With it, I was able to extract team and player statistics and generate an automated report. The lessons learned here will serve as a starting point for further developments - both in the data engineering and science directions - during the 2023 baseball season.

## Part 0: Introduction

1. What is the motivation for this work?

A couple of months ago, my brother Gonçalo excitedly introduced me to America's past-time, baseball. At first, I failed to understand the reason for such enthusiam, but politely watched a couple of games and highlight videos on MLB's Youtube page [1](https://www.youtube.com/c/MLB) with him. Together with the excellent content from Foolish Baseball [2](https://www.youtube.com/c/FoolishBaseball), after two weeks I was hooked on the sport! One very interesting aspect of baseball is that advanced player-individual statistics are a part of the sport for decades and their usage has been behind considerable success in building teams: remember Brad Pitt in Moneyball, where he played Billy Beane, the general manager of the Oakland Athletics? This is possible because these statistics give an accurate representation of how players execute their actions on the field, therefore allowing for a good quantitative assessment of their performance.

This is not the case, for example, in football (i.e, soccer), where standard match statistics are mostly team-focused (possessions statistics, distance ran) or final outcome oriented (goals and assists or important saves). These statistics cause, at least at the eyes of the layperson, an overvaluation of the importance of final-third players, while important defensive actions - as a clearing header away from goal - or a build-up pass during the construction phase that slices through the opponent's pressing - remain unnoticed. See, however, [3](https://www.tandfonline.com/doi/full/10.1080/24733938.2021.1944660) for more information on advanced football statistics.

My motivation is three-fold, as I: 

1. Am interested in developing a fundamental understanding of the game of baseball;
2. Believe that team and player data can provide those insights, as well as the opportunity for apply modelization techniques;
3. Want to have fun while developing something new! (This is actually number 1);


2. What is the first concrete goal of this project?

Regardless of how lofty one's research ideas are, one first needs to develop an infrastructure that allows for scientific development. The first goal is therefore to:

Create a data pipeline which allows for automatic, fast and reliable extraction, manipulation and representation of data from the MLB statistics API (statsapi). This involves:

    1. Extract raw data from statsapi and pre-process it;
    2. Augment the current set of statistics;
    3. Summarize results in an automatically-generated PDF with a relevant description, tables and charts.

To achieve this, the following tools will be used, with Python as the programming language:
    
    1. The excellent package `statsapi` [4](https://github.com/toddrob99/MLB-StatsAPI), which writes the HTTP requests to the MLB API. This simplifies data collection and allows us to focus on extracting the data from these requests and treating it. See, however, possible improvements in the upcoming publications;
    2. Pandas for creating data tables and generating new features;
    3. Matplotlib for representing the results;

Together with Airflow for orchestration, which is crucial to run the data pipelines automatically.

This setup is running quite stable for the past months, so I thought it might be interesting to share some lessons from this exercise in a x-part series going through the following:

    1. Introduction;
    2. Extraction (of information) - Process 1: Daily frequency;
    3. Treatment - Process 1: Daily frequency;
    4. Analysis - Process 1: Daily frequency;
    5. Report - Process 2: Weekly frequency;
    6. Orchestration of Processes 1 and 2;

By the end of this journey, we will have a data pipeline which we can reliably use to produce player and team analysis. If you are not very interested in the game of baseball and just want to see the pipeline, check the GitHub repository here [5](link to GitHub repo).

3. Ok, that sounds great... But how is baseball played?

Paraphrasing Wikipedia [5](https://en.wikipedia.org/wiki/Baseball), there are two teams of nine players each, taking turns batting (similar to attacking in football) and fielding (e.g., defending). The game is in play when a player on the fielding team, called the pitcher, throws a ball that a player on the batting team tries to hit with a bat. 

The objective of the attacking team is to hit the ball into the field of play, away from the other team's players, allowing its players to run the bases, having them advance counter-clockwise around four bases to score what are called "runs". The objective of the defending team is to prevent batters from becoming runners, and to prevent runners' advance around the bases.

Confusing? Let us look at an example:

[GIF]

[Description of what is happening]


## Part 1: Setting up the infrastructure (statsapi_parameters_script.py)

Before running any processes, it is best to set up a file in which all the necessary information is stored:

    1. Day of execution;
    2. The league for which we are extracting the information;
    3. The name and location of the files that store the team and player information;
    4. The name and location of the figures generated in the analysis step;

This file will come in handy when executing the pipeline automatically in Airflow.

## Part 2: Extracting information (statsapi_extraction_script.py)

[Show the DAG]

To keep this discussion simple, see [6](https://www.ibm.com/cloud/learn/api) for a good description of what an API is. For our project, we want to extract team and player statitics from the MLB API. This involves writing an HTTP request to the API, from which data is obtained in the `json` format.  

1. Team statistics are obtained using `get_league_division_standings`. Iterates through the different leagues (american and national) and through the different divisions (north, south, east, west) and obtains the team rankings, number of wins, losses ...;

2. Player statistics are obtained by looking up each team and extract its rosters, `get_player_stats_dataframe_per_team`. Since each player has an unique ID, we can then use this to extract his game statistics. By combining all the players of all the teams, we obtain player data for the whole league;

3. Logging registers whether the request failed to extract information for a given team or a given player.

[A diagram of MLB -> AL / NL -> Divisions]

Because the number of teams and players is small, the data is stored in .csv format. 


## Part 3: Treating information (statsapi_treatment_script.py)

Because we are later interested in doing statistical analysis of players' performance, we have to focus on players which have played enough times (i.e., a minimum number of games) for there to be an statistically significant assessment of their performance. For example, if I play a total 3 baseball games, scoring 1 home-run in each game, I might be very good or very lucky. However, if I play 150 games and score 1 home'run per game, luck might be playing a role, but it is skill that is driving the results. [This leads to an interesting discussion of randomness in player performance, see more details here]  

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

## Part 4: Analyzing information

After the two latter steps, displaying information to server as an analysis tool is a relatively straightforward process. One must understand, however, which information to show.

From this [7](https://www.mlb.com/glossary/standard-stats):

*Batting average* (AVG): The ratio between the number of player hits and the number of atBats. Does not take into account the number of times the batter is walked (intentionally or otherwise) or hit by a pitch.

*On-base percentage* (OBP): The ratio of the number of times a batter reaches base and number of plate appearances. This is a key stat because the goal of the attacker is to avoid getting an out. "Reaching base" here includes hits, walks and hits by pitches.

*Slugging percentage* (SLG): The ratio of total number of bases a player records per at-bat. The formula is:
    (1B + 2*2B + 3*3B + 4*HR)/AB 
where AB represents atBats, #B the number of bases covered in a hit, and HR homeruns.
This is a relevant stat because it attributes more weight to more valuable runs (triples and home runs).

*On-base plus slugging* (OPS): Sum of on-base percentage and slugging percentage.

*Batting average on Balls in Play (BABIP)*: Measures the player's batting average exclusively on balls into the field of play, removing outcomes not affected by the opposing defense (that is, homeruns and strikeouts). The formula is:
    (H - HR)/(AB - K - HR + SF)
where H represent runs, K strikeouts and SF sacrifice flies.

*Runs batted in (RBI)*: Measures the number of runs that resulted from a player's plate appearance, except if there is an error or ground into double play. Although RBIs are mostly generated from run-scoring hits, a player can also receive an RBI if he walks (or is hit by a pitch) with the bases loaded.

Do an explanation of the information shown in the report. Mention that the results of this process are .png figures which are then imported in the reporting step. [We should probably use a HDF rather than .png]


## Part 5: Reporting information

So far, we have focused on the daily extraction, treatment and analysis of data. However, because player statistics have a time scale larger than of a couple of days (that is, there are only meaningful changes to a player stats after he has played several games, in the course of several days), the automated report which summarizes the aforementioned results is generated weekly rather than daily. 

It is composed of three parts:

1. Time-series evolution of the league standings;
2. Time-series evolution of player statistics - homeRuns and on-base-percentage;
3. A week to date tabular analysis of player statistics - who are the best and worse players and who has had the largest improvement compared to last week;
4. Selection of the charts generated in Part 4 - focus on ... , ... , ... , ...;

Technically, the following steps are done:

1. A Jupyter notebook imports the previously generated images and computes the tables with the best and worse performers. Using a notebook is particularly useful because it combines output cells with text markdown cells, making it very easy to write an automated report.

2. This Jupyter notebook is automatically run using a bash command, which also converts the notebook to an HTML file. [Although we could have chosen a PDF file]



## Part 6: Orchestrating the processes

Process 1 - Daily data extraction

This is composed of the following steps:

    1. Data extraction;
    2. Data treatment;
    3. Data analysis;

Which we have to run for the two baseball leagues inside MLB: the American and National leagues. To this effect, we create the following directed acyclic graph in Airflow:

[Figure representing the orchestration DAG]

where each named box represents a bash operator performing a particular function:

    1. Write the name of the league for which the DAG is being executed to a text file;
    2. Trigger the extraction task. Following the first step, the file described in Part 1 updates the parameter `LEAGUE_NAME` at execution time, guaranteeing that the pipeline is ran for the correct league;
    3. Trigger the treatment task;
    4. Trigger the analysis task;

But what are these bash operators doing exactly? Let us look at a particular example:

[Figure representing a particular operator code in Airflow]

1. Activate a conda virtual environment: source /root/anaconda3/bin/activate mlb-airflow
2. Run a Python script: python /root/mlb-airflow/statsapi_extraction_script.py
3. Deactivate the conda virtual environment: source /root/anaconda3/bin/deactivate mlb-airflow

Process 2 - Weekly data reporting





# Things to do

    1. Write a DAG for team standings (Not necessary, we can extract it from the current process);
    2. Write notifications for the DAG (Done);
    3. Change the schedule of the DAG (Done);
    4. See if you write the same DAG running for different parameters or two different DAGs for each league; (Done)
    5. See if you can add an operator to the DAG which creates a report; (Done)
    6. See if you can add an operator to the DAG which sends a report every week;
    7. Change the code in the notebook so that it runs every week instead; (Done)
    8. Review code.
    9. Add a comment to the text on how the Airflow logs are interesting!

    Ideas for what to do next:
    1. Think of a better way of extracting the player stats (if we can make it team independent, or check how many teams a player had in a season and check for all of them)
    2. Extend the player analysis to pitching and defending
    3. Understand if there is a problem with the NL report 


    Execution of the two dags with 15 minute intervals
    The input parameter has to be fed to the first bash command (of an operator we would have to include in the dag,
    where the parameters are set). The others operators would read from this one.

    Use Jupyter notebook to build the final report (and then transform it to HTML and/or PDF)
    jupyter nbconvert --execute test_file.ipynb --to pdf
    (this executes the jupyter notebook and then transforms it to PDF
    we could do the same think later for HTML)

    How do we want to generate the reports automatically?
    * Create two DAGs, one using the AL name and the other the NL name (maybe I can use only one DAG,
    if I am able to use the LEAGUE_NAME as stored in the file)
    * These can run after the set of DAGs for AL and NL, once a week
    * There should be two operators in the DAG
        1. bash script running the Python script which creates the time series plots;
        2. bash script which updates the notebook and generates an HTML from it (check if one can change the name of the file to avoid overwrites)


    08-10-2022

    Part 4

    Do an explanation of the information shown in the report. Mention that the results of this process are .png figures which are then imported in the reporting step. [We should probably use a HDF rather than .png]

