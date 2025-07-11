# Machine Moneyball - Extracting insights from Major League Baseball's data - Part 4

This is the fourth part of a series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report automatically [1](https://github.com/lbventura/mlb-airflow-data-pipeline). The previous sections were about:

- Part 1: Introduction
- Part 2: A very (very!) short introduction to baseball
- Part 3: Extracting data

Today we are shall focus on handling the raw data extracted in the previous step and make it ready for analysis. As a reminder of where we stand in the workflow, we are here:

[Airflow_Extracting_DAG (but with the operator marked)]

## Treating data - `treatment_task`

Because we are later interested in doing statistical analysis of players' performance, we have to focus on players who have played enough times (i.e., a minimum number of games) for there to be a statistically significant assessment of their performance. For example, if I were to play a total of 3 baseball games, scoring 1 homerun in each game, I could either be very good or very lucky. However, if I play 150 games (approximately a full MLB season) and score 1 homerun per game, luck might be playing a role, but most probably skill is driving the results [2].

Unfortunately, the MLB API has some data consistency issues, one example is in the number of games each player participated in, the `gamesPlayed` statistic. The image below shows players which had less games on the 6th of October (`gamesPlayed`) than on the 28th of August (`gamesPlayedOld`):

[Games Played Inconsistency example]

One explanation could be that these players have changed teams (see `Shortcomings` paragraph of the last post), but it is not the case. Jo Adell and Julio Rodriguez played the whole season in the Los Angeles Angels and Seattle Mariners, respectively.

By manually checking the data, it is easy to see that the `gamesPlayed` column is incorrect: Julio Rodriguez, the star center-fielder of the Seattle Mariners, ended his regular season with 129 games. It is very important to be aware of these data quality shortcomings and attempt to minimize their impact. There are two possible approaches to this:

1. Double-checking the data with a different source and correcting it. This is the safest approach but requires a second, correct, information source, at which point we would just use this second source.

2. Instead, check other stats which are proportional to the number of games and filter the universe of players accordingly.

Number 2 was selected for feasibility issues. For batters (i.e, attacking players), we use `plateAppearances` - the number of times a batter is in this situation:

[Player in the batter box]

(Barry Bonds likely crushing one of Randy Johnson's fastballs into the San Francisco Bay)

For which the number of inconsistent players is much lower than before

[Plate Appearances Consistency example]

(Kevin Plawecki changed from the Boston Red Sox to the Texas Rangers during the 2022 season)

After filtering the data for players which have a minimum number of plate appearances - 100, as a rule of thumb, was the used threshold [3] - we are left with a universe of about 150 players. For these, we generate a new set of statistical features. The actual statistics are beyond the scope of this article (see, however, the excellent MLB page on Statistics [4](https://www.mlb.com/glossary/standard-stats)), but there are two main ideas:

1. Compute features normalized by the number of plate appearances. This corrects a phenomenon called stats accumulation, i.e, a good player can have larger total number of home-runs, runs-batted-in (RBI), ... than a great player simply because it has played more, as represented by more plate appearances.

2. Compute features normalized by the league average. This allows a direct comparison of a player with its peers. For a league average normalized at 100, a player with a normalized OBP (on-base-percentage) of 200 has an on-base-percentage twice as large as the league average.

In the next part, we will use the treated data created here to generate point statistics based on points 1. and 2. above and with these design some interesting charts. See you then!


## Notes

[2][I picked a very large number deliberately here to make this sequence of events extremely unlikely. The longest streak of at least one homerun per game in MLB history is 8 games by Ken Griffey, Jr., from July 20 to July 28 1993 [https://www.mlb.com/news/consecutive-home-run-games-c265322182]. Griffey had a solid 6.5% homerun per plate appearance that season, but that shirks in comparison to the record value of 15.34% by Barry Bonds during the 2001 season, at the peak of the steroid era. An estimate with the numbers above - with 4 plate appearances per game, very roughly the league average for batters - results in a homerun per plate appearance percentage of 25%(!), far above Bonds' already unrepeatable mark.]
[3][The chosen value worked because it was imposed when 128 games (out of a season of 162) had been played. For perspective, the player of the National League with most plate appearances at the start of data extraction, the Atlanta Braves' first baseman Matt Olson, had 563. This threshold of 100 removes about 20% of the players at the start of the data extraction. Had we started collecting data at a different point in the season, a different number would have been selected.]
