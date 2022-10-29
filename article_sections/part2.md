# Machine Moneyball - Extracting insights from Major League Baseball's data

## A very short introduction to baseball

This is the second part of a multi-part series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report [1](https://github.com/lbventura/mlb-airflow-data-pipeline). See the first part here. 

If you are like me before starting this project, your knowledge of baseball might go as far as:

1. Knowing the name "Babe Ruth";
2. Having seen an homerun before;
3. Knowing the New York Yankees, since every second person has a baseball cap from them.

[NY Yankees symbol](https://www.si.com/.image/t_share/MTY4MTg2MjA4MDAzNzYxNDI1/christiano-ronaldojpg.jpg)
Caption: Cristiano Ronaldo with a New York Yankees cap. He is likely disappointed with the Yankees performance in this post-season.

Because we are going to focus a considerable amount of time looking at baseball statistics, it is worth investing some time understanding how the game itself is played. 

Paraphrasing Wikipedia [2](https://en.wikipedia.org/wiki/Baseball), there are two teams of nine players each, taking turns batting (similar to attacking in football) and fielding (e.g., defending). The game is in play when a player on the fielding team, called the pitcher, throws a baseball that a player on the batting team tries to hit with a bat. 

The objective of the attacking team is to hit the ball into the field of play, away from the defending team's players, allowing the attacking players to run the bases, having them advance counter-clockwise around four bases to score what are called "runs". The objective of the defending team is to prevent batters from becoming runners, and to prevent runners from advancing around the bases.

Confusing? Let us look at a couple of diagrams of a baseball field with the field positions of the defending team players

[Baseball field layout](https://commons.wikimedia.org/wiki/File:Baseball_positions.svg)

The pitchers stands in the center of the field. The first, second and third basemen, together with the catcher standing behind the homeplate, form a diamond around the pitcher by standing close to the bases themselves. Let us check a real-life example to have a better idea of just how large the field is

[Baseball field layout](https://sportsfanfocus.com/wp-content/uploads/2019/02/baseball-positions.jpg)

The distance between the pitcher and the batter, which stands at the home plate, is 60ft and 6in (about 18.5 m). [Comment on the average dimensions on the MLB baseball parks, with reference to https://i.stack.imgur.com/Obr1G.png ]. For more details, see here [3](https://www.mlb.com/glossary/rules/field-dimensions), Now that the player's positions are clearer, let us check one example of a play:

[Single hit GIF]


The pitcher threw a ball at the batter. The batter succeeded in hitting the ball past the opposing defenders, thereby allowing him to run to first base and the player at third base to run to the fourth base and score in a run. The player which, before the pitcher threw the ball, was at first base tried to run to second base but was tagged out of the game.

To understand the structure of the repository, it is also important to note that Major League Baseball is actually composed of two different leagues with 15 teams each: the American League, where the famous New York Yankees play, and the National League, whose most well-known team is probably the LA Dodgers.

[A diagram of MLB -> AL / NL -> Divisions]

In the next editions, we will examine in more detail how exactly the pipeline is structured.