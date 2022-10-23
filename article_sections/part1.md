# Machine Moneyball - Extracting insights from Major League Baseball's data

On the occasion of the Baseball World Series, which start on the 28th of October, I have written a summary of a project I have developed for the past two months: a data pipeline using the Major League Baseball (MLB) API. With it, I was able to extract team and player statistics and generate an automated report. The lessons learned here will serve as a starting point for further developments - both in the data engineering and science directions - during the 2023 baseball season.

## Part 1: Introduction

1. What is the motivation for this work?

A couple of months ago, my brother Gonçalo excitedly introduced me to America's past-time, baseball. At first, I failed to understand the reason for such enthusiasm, but politely watched a couple of games and highlight videos on MLB's Youtube page [1](https://www.youtube.com/c/MLB) with him. Together with the excellent content from Foolish Baseball [2](https://www.youtube.com/c/FoolishBaseball), after two weeks I was hooked on the sport! One particularly interesting aspect of baseball is the long-standing advanced player-individual statistics, whose usage has been behind considerable success in building competitive teams: remember Brad Pitt in Moneyball, where he played Billy Beane, the general manager of the Oakland Athletics? This is possible because these statistics give an accurate representation of how players execute their actions on the field, therefore allowing for a good quantitative assessment of their performance.

This is not the case, for example, in football (i.e, soccer), where standard match statistics are mostly team-focused (possessions statistics, distance ran) or final outcome oriented (goals and assists or important saves). These statistics cause, at least at the eyes of the layperson, an overvaluation of the importance of final-third players, while important defensive actions - as a clearing header away from goal - or a build-up pass during the construction phase that slices through the opponent's pressing - remain unnoticed. See, however, [3](https://www.tandfonline.com/doi/full/10.1080/24733938.2021.1944660) for more information on advanced football statistics.

The motivation is two-fold: 

1. Grow a fundamental understanding of the game of baseball by extracting information from team and player data;
2. Develop something new and (hopefully) exciting!


2. What is the first concrete goal of this project?

Regardless of how lofty one's research ideas are, one first needs to develop an infrastructure that allows for scientific investigation. The first goal is therefore to:

Create a data pipeline which allows for automatic, fast and reliable extraction, manipulation and representation of data from the MLB Statistics API (statsapi). This involves:

1. Extract raw data from statsapi and pre-process it;
2. Augment the current set of statistics;
3. Summarize results in an automatically-generated report with a relevant description, tables and charts.

To achieve this, the following tools will be used, with Python as the programming language:
    
1. The excellent package `statsapi` [4](https://github.com/toddrob99/MLB-StatsAPI), which writes HTTP requests to the MLB API. This simplifies data collection and allows us to focus on extracting the data from these requests and treating it. See, however, possible improvements in the upcoming publications;
2. Pandas for creating data tables and generating new features;
3. Matplotlib for representing the results;

Together with Airflow for orchestration, which is crucial to run the data pipelines automatically.

Because this setup is running quite stably for the past months, I thought it might be interesting to share some lessons from this exercise in a four-part series going through the following points:

1. Introduction;
2. Data extraction;
3. Data treatment;
4. Data analysis;
5. Report;
6. Orchestration of Processes 1 and 2;

Steps 2 through 4 are part of a automated process that runs daily, while step 5 creates a report from the previous process on a weekly basis. By the end of this journey, we will have a data pipeline which we can reliably use to produce player and team analysis. If you are not very interested in the game of baseball and just want to check the pipeline, check the GitHub repository here [5](link to GitHub repo).

3. Ok, that sounds great... But how is baseball played?

Paraphrasing Wikipedia [5](https://en.wikipedia.org/wiki/Baseball), there are two teams of nine players each, taking turns batting (similar to attacking in football) and fielding (e.g., defending). The game is in play when a player on the fielding team, called the pitcher, throws a baseball that a player on the batting team tries to hit with a bat. 

The objective of the attacking team is to hit the ball into the field of play, away from the defending team's players, allowing the attacking players to run the bases, having them advance counter-clockwise around four bases to score what are called "runs". The objective of the defending team is to prevent batters from becoming runners, and to prevent runners' advance around the bases.

Confusing? Let us look at a diagram of a baseball field layout with the field positions of the defending team players and then an example

[Baseball field layout](https://commons.wikimedia.org/wiki/File:Baseball_positions.svg)

The pitchers stands in the center of the field. The first, second and third basemen, together with the catcher form a diamond around the pitcher by standing on the bases themselves.

[Single hit GIF]

The pitcher threw a ball at the batter. This succeeded in hitting the ball past the opposing defenders, thereby allowing him to run to first base and the player at third base to run to the fourth base and score in a run. The player which, before the pitcher threw the ball, was at first base tried to run to second base but was tagged out of the game.

To understand the structure of the repository, it is also important to note that Major League Baseball is actually composed of two different leagues with 15 teams each: the American League, where the famous New York Yankees play, and the National League, whose most well-known team is likely the LA Dodgers.

[A diagram of MLB -> AL / NL -> Divisions]

In the next editions, we will examine in more detail how exactly the pipeline is structured.