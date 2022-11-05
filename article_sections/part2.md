# Machine Moneyball - Extracting insights from Major League Baseball's data - Part 2

This is the second part of a multi-part series on a data pipeline which uses the Major League Baseball API to extract team and player statistics and publish a weekly automated report [1](https://github.com/lbventura/mlb-airflow-data-pipeline). See the first part, which introduces this project, here.

If you are like me before starting this project, your knowledge of baseball might go as far as:
1. Knowing the name "Babe Ruth";
2. Having seen a homerun (i.e. hitting the ball out of the baseball park) before;
3. Recognizing the New York Yankees logo, since every second person has a baseball cap from them.

Because we are going to focus on baseball statistics later, it is worth investing some time understanding how the game itself is played.

## A very (very!) short introduction to baseball

Paraphrasing Wikipedia [2](https://en.wikipedia.org/wiki/Baseball), there are two teams of nine players each, taking turns batting (similar to attacking in soccer) and fielding (i.e. defending). The game is in play when a player of the fielding team, called the pitcher, throws a baseball that a player of the batting team, the batter, tries to hit with a bat.

The objective of the attacking team is to hit the ball into the field of play, away from the defending team's players, allowing the attacking players to run the bases, having them advance counter-clockwise around four bases to score what are called "runs". The objective of the defending team is to prevent batters from becoming runners, and to prevent runners from advancing around the bases.

The distance between the pitcher and the batter standing at the home plate is 60ft and 6in (about 18.5m). The boundaries of the field of play roughly form a quarter-circle, such that the distance from home plate to the edge of the wall at the center of the field is around 400ft (about 121m) for most baseball fields in the MLB. It is therefore not surprising that MLB batters only made a home run about 3.1% of time in 2022 [5](https://www.baseball-almanac.com/hitting/hihr8.shtml). For more details, see an interesting analysis of all the MLB fields here [6](https://i.stack.imgur.com/Obr1G.png) and the rule book for the field dimensions here [7](https://www.mlb.com/glossary/rules/field-dimensions).


Let us also briefly talk about the competition under analysis. Major League Baseball (MLB) is the highest professional baseball league in the United States, composed of two different leagues with 15 teams each: the American League (AL), where the famous New York Yankees play, and the National League (NL), whose most well-known team is the LA Dodgers.
Each league is split into three divisions each: East, Central and West:

* NL East: New York Mets, Atlanta Braves, Philadelphia Phillies, Miami Marlins and Washington Nationals;
* NL Central: Milwaukee Brewers, St. Louis Cardinals, Chicago Cubs, Pittsburgh Pirates and Cincinnati Reds;
* NL West: Los Angeles Dodgers, San Diego Padres, San Francisco Giants, Arizona Diamond-Backs and Colorado Rockies;

* AL East: New York Yankees, Tampa Bay Rays, Toronto Blue Jays, Boston Red Sox and Baltimore Orioles;
* AL Central: Minnesota Twins, Chicago White Sox, Cleveland Guardians, Detroit Tigers and Kansas City Royals;
* AL West: Houston Astros, Los Angeles Angels, Texas Rangers, Seattle Mariners and Oakland Athletics;

While we will be focusing mainly on the league level (American and National), the fact that clubs play more often with their division rivals will play a role on team statistics. This is something we will touch on in the future.
In the next post, we will jump right into the pipeline and understand what is necessary to extract information from MLB's API. See you then!