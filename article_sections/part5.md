# Machine Moneyball - Extracting insights from Major League Baseball's data - Part 5

This is the fifth part of a series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report automatically [1](https://github.com/lbventura/mlb-airflow-data-pipeline). The previous sections were about:

- Part 1: Introduction
- Part 2: A very (very!) short introduction to baseball
- Part 3: Extracting data
- Part 4: Treating data

Following a long pause for rest, relaxation and reflection, we continue with the analysis of the collected data with the goal of understanding different attacking (i.e, batting) players types in baseball. To achieve this, we are going to:

1. Work through the game dynamics and understand how a team can maximize its points (i.e, number of runs), based on a very simple model with two types of players;
2. Design variables which describe the two aforementioned player types.

## Analyzing data - analysis_task

Roughly speaking, a batting team can achieve its goal of generating runs on each inning by having its players attack in two different ways:

1. Be conservative on which baseballs one chooses to swing at and favor a large amount of contact with the ball rather than strong contact. This puts the ball in play more often, thereby allowing the batting player to reach the bases. In technical terms, the team wants to avoid outs, both strikeouts and groundouts [2](https://www.mlb.com/glossary/standard-stats/groundout);
2. Be aggressive and attack all the balls that the pitcher throws while simultaneously going for power in order to score homeruns, which automatically give the team at least one run.


If it sounds like there is a catch - wordplay intended - here, you are correct! While being aggressive is likely to lead to more hits and more homeruns - because you are trying to hit the ball more often and with more power - it will also lead players to outs more often (see a long explanation in note [3] and references therein).

In general, how the team behaves during the game is influenced by the coaching philosophy and the mixture of players of type 1 (contact hitters) and type 2 (power hitters) within a team. Let us create an idealized scenario to exemplify the two types of players described above [4].

## Moneyballing

Our team is only composed of two players, Kyle Schwarber and Luis Arraez:
Kyle Schwarber [5](https://www.mlb.com/player/kyle-schwarber-656941) was one of the best power hitters in the National League, collecting a record total of 46 homeruns during the regular season. However, he only had the 71st best on-base percentage of the league, with 32.3%;
Luis Arraez [6](https://www.mlb.com/player/luis-arraez-650333) was one of the best contact hitters in the American League, only being striked out 49 times (best in MLB) during the regular season while recording a 37.5% on-base percentage (obp). However, he only had the 227th best home run numbers in the league, totalling 8;

As the manager of our own MLB team, we would send our 3 clones of Luis Arraez first. Each of these has a good chance of getting on base: very roughly there is a probability of 5.3% (i.e, 0.053 = 0.375^3) that the bases will be loaded (one player in each base) after these clones had their chance at the plate. We then send Kyle Schwarber, the power hitter per excellence. If he scores an homerun (for which there is a probability of approximately 7% [7](https://www.baseball-reference.com/players/s/schwaky01.shtml)) with the bases loaded, that is 4 runs in one single hit, with a probability of 0.4%, assuming independent events (i.e, 0.053*0.07 ≈ 0.004).

Compare this to the strategy where we instead send 3 clones of the power hitter Schwarber first. Very roughly there is a probability of 3.4% (i.e, 0.034 = 0.323^3) that the bases will be loaded after their plate appearances. Not surprisingly, the polynomial dependence on the on-base percentage causes a massive difference of about 2% (= 0.053 - 0.034) between three Luiz Arraez or three Kyle Schwarber loading the bases. Then sending Luis Arraez to deliver a homerun (for which there is a probability of about 1.3% [8](https://www.baseball-reference.com/players/a/arraelu01.shtml)) also gives us 4 runs in one single hit, but with a probability of 0.04%, a factor of 10 difference from the previous scenario!

Although this is a very rough back-of-the-envelope estimate, it illustrates [9]:
1. the two different types of players (contact vs power);
2. the importance of selecting the right player for the game circumstances [10];

Given the simplified game model above, let us design a few pairs of variables that might reinforce our view or falsify it. Unless made explicit, the variables are normalized per plate appearance, whereas a variable named "Normalized_x" refers to a variable x which was normalized by the league average. Both were explained in the last post [11](https://www.linkedin.com/pulse/machine-moneyball-extracting-insights-from-major-4-ventura-phd).

## Variable pairs of interest

y-var: Normalized on-base plus slugging percentage (ops) (obp + slugging), x-var: strikeouts - In theory, these variables should be slightly negatively (i.e, inversely) correlated. Roughly speaking, aggressive batters, which hit for power often, are more likely to strike out, thereby leading to a lower on-base percentage. This chart is relevant to determine outliers. Excellent sluggers will appear above the rest of the players, displaying a high ops while maintaining a low number of strikeouts.

[american_league_2022-10-06_strikeOutsperplateAppearance_normalized_ops.png]

y-var: Base on Balls (i.e, walks), x-var: strikeouts - In theory, these variables should be slightly inversely correlated. Roughly speaking, aggressive batters which attack the ball thrown by the pitcher more often should walk fewer times because they are more likely to attack balls outside the strikezone, for which they could eventually earn a walk.

[american_league_2022-10-06_strikeOutsperplateAppearance_baseOnBallsperplateAppearance.png]

y-var: Normalized obp, x-var: Difference between strikeouts and walks - In theory, these variables should be inversely correlated, because to obtain a large on-base percentage (obp), the batter must avoid getting striked out. This chart is particularly useful to determine players which are extremely good at hitting for contact - those with an ops above league average and a negative difference between strikeouts and walks - i.e, the top left corner of the chart. These might not score a lot of homeruns, but they get on base very often and give their team the possibility to score runs. Luis Arraez is there, next to Vinnie Pasquantino and Alex Bregman.

[american_league_2022-10-06_difstrikeOutsbaseOnBallsperplateAppearance_normalized_obp.png]

y-var: homeruns, x-var: strikeouts - In theory, these variables should be positively correlated. Aggressive batters try to generate powerful contact with the ball more often. They will receive more homeRuns at the expense of more strikeouts. Elite players should be placed in the top half of the chart, towards the left.

[american_league_2022-10-06_strikeOutsperplateAppearance_homeRunsperplateAppearance.png]


## Notes

[3]: This is caused by something that we have not analyzed so far in this series. Remember that the goal of attacking baseball is to advance players along the bases. This can happen if the batting player hits the baseball without anyone in the defending team catching it or if the batting player gets walked. But before talking about walks, we need to understand the pitcher count: the goal of the pitcher is to throw balls inside a rectangle formed by the batter's body such that the batter cannot hit it. If the throws three such balls, the batting player is out. However, the pitcher does not have unlimited chances to achieve this goal. If he throws four balls outside the aforementioned rectangle without a reaction from the batting player, the latter essentially never "received" a playable ball and therefore gets to walk to first base. Because only about 41% of all the pitches are inside the strikezone (see [N1](https://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg=all&qual=0&type=5&season=2022&month=0&season1=1901&ind=0&team=0,ss&rost=0&age=&filter=&players=0&startdate=&enddate=) for the stats and [N2](https://library.fangraphs.com/offense/plate-discipline/) for an explainer), this is why it is extremely important for the batter to be patient, wait for the pitcher to make mistakes, and earn a walk.

[4]: To understand more about the intricacies of a batting order, please see [N3](https://www.baseball-reference.com/bullpen/Batting_order).

[9]: Note that we only comparing these two teams in the very particular situation where the first three players get on base and the fourth player in the batting order scores a homerun. Of course, there are many more way to score runs in baseball.

[10]: Before patting ourselves on the back, we would note that Kyle Schwarber was actually first batting throughout the whole season, see [N4](https://www.lineups.com/mlb/lineups/philadelphia-phillies). This is, however, most likely due to a lack of offensive depth in the Philadelphia Phillies team.
