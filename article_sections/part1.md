# Machine Moneyball - Extracting insights from Major League Baseball's data

On the occasion of the baseball World Series, which starts tomorrow, October 28th, I have written a summary of a project I have been developing for the past two months: a data pipeline using the Major League Baseball API. With it, I was able to extract team and player statistics and publish a weekly automated report. The lessons learned here will serve as a starting point for further developments - both in the data engineering and science directions - during the 2023 baseball season.

## Background and motivation

A couple of months ago, my brother Gonçalo excitedly introduced me to America's past-time, baseball. At first, I failed to understand the reason for such enthusiasm, but politely watched a couple of games and highlight videos on MLB's Youtube page [1](https://www.youtube.com/c/MLB) with him. Together with the excellent content from Foolish Baseball [2](https://www.youtube.com/c/FoolishBaseball), after two weeks I was hooked on the sport! One particularly interesting aspect of baseball is the existence of long-standing advanced player statistics, whose usage has been behind considerable success in building competitive teams: remember Brad Pitt in Moneyball, where he played Billy Beane, the general manager of the Oakland Athletics? This is possible because these statistics give an accurate representation of how players execute their actions on the field, therefore allowing for a good quantitative assessment of their performance.

This is not the case, for example, in soccer (i.e, football), where standard match statistics are mostly team-focused (possessions statistics, distance ran) or final outcome oriented (goals and assists or important saves). These statistics cause, at least in the eyes of the layperson, an overvaluation of the importance of final-third players, while important defensive actions - as a clearing header away from goal - or important offensive actions - as a build-up pass during the construction phase that slices through the opponent's pressing - remain unnoticed. See, however, [3](https://www.tandfonline.com/doi/full/10.1080/24733938.2021.1944660) for more information on advanced football statistics.

The motivation is two-fold: 

* Develop a fundamental understanding of the game of baseball through a quantitative (and, when possible, scientific) approach;
* Create the tools for extracting information from team and player data seamlessly.


## What is the first concrete goal of this project?

Regardless of how lofty one's research ideas are, one first needs to develop an infrastructure that allows for scientific investigation. The first goal is therefore to:

Create a data pipeline which allows for automatic, fast and reliable extraction, manipulation and representation of data from the MLB Statistics API (statsapi). 

This involves:

1. Extract raw data from statsapi and pre-process it;
2. Augment the current set of features;
3. Summarize results in an automatically-generated report with a relevant description, tables and charts.

To achieve this, the following tools will be used, with Python as the programming language:
    
1. The excellent package `statsapi` [4](https://github.com/toddrob99/MLB-StatsAPI), which writes HTTP requests to the MLB API. This simplifies data collection and allows us to focus on extracting the data from these requests and treating it;
2. Pandas for creating data tables and generating new features;
3. Matplotlib for representing the results;

For orchestration, Airflow will be used to run the data pipelines automatically.

Because this setup is running quite stably for the past months, I thought it might be interesting to share some lessons from this exercise in a multi-part series going through the following points:

1. A very (very!) short introduction to baseball;
2. Data extraction;
3. Data treatment;
4. Data analysis;
5. Reporting;
6. Orchestration of Processes 1 and 2;

Points 2 through 4 are part of a automated process that runs daily, while point 5 creates a report from the previous process on a weekly basis. By the end of this journey, we will have a data pipeline which we can reliably use to produce player and team analysis. If you are not very interested in the game of baseball and just want to check the pipeline, you can check the GitHub repository here. But stick around, as you might just learn something interesting!
