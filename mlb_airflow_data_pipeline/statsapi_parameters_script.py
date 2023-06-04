import os
from datetime import datetime

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

LEAGUE_MAPPING = {"american_league": 103, "national_league": 104}

LEAGUE_DIVISION_MAPPING = {103: [200, 201, 202], 104: [203, 204, 205]}

SOURCE_LOCATION = os.path.dirname(os.path.abspath(__file__))

# setting "national_league" as the default league name
LEAGUE_NAME = "national_league"
LEAGUE_NAME_LOCATION = f"{SOURCE_LOCATION}/league_name_choice.txt"

# if there is not an ongoing season, this parameter has to be set to the
# previous year.
SEASON_YEAR = 2023
# set this parameter to False to fetch player career stats
IS_SEASON_STATS = True

# read the league name set at runtime
with open(LEAGUE_NAME_LOCATION, "r") as text_file:
    LEAGUE_NAME = text_file.readline().strip()

DATA_FILE_LOCATION = f"{SOURCE_LOCATION}/data/"

OUTPUT_FILE_LOCATION = f"{SOURCE_LOCATION}/output/"

# player information fields
PLAYER_INFORMATION = ["playername", "team_id"]

# separate data into batting, pitching and defense
BATTING_STATS = [
    "gamesPlayed",
    "groundOuts",
    "airOuts",
    "runs",
    "doubles",
    "triples",
    "homeRuns",
    "strikeOuts",
    "baseOnBalls",
    "intentionalWalks",
    "hits",
    "hitByPitch",
    "avg",
    "atBats",
    "obp",
    "slg",
    "ops",
    "caughtStealing",
    "stolenBases",
    "groundIntoDoublePlay",
    "plateAppearances",
    "totalBases",
    "rbi",
    "leftOnBase",
    "sacBunts",
    "sacFlies",
    "groundOutsToAirouts",
    "catchersInterference",
]

PITCHING_STATS = [
    "gamesPlayed",
    "gamesStarted",
    "intentionalWalks",
    "numberOfPitches",
    "inningsPitched",
    "era",
    "wins",
    "losses",
    "saves",
    "saveOpportunities",
    "holds",
    "blownSaves",
    "earnedRuns",
    "whip",
    "battersFaced",
    "gamesPitched",
    "completeGames",
    "shutouts",
    "strikes",
    "strikePercentage",
    "hitBatsmen",
    "balks",
    "wildPitches",
    "pickoffs",
    "pitchesPerInning",
    "gamesFinished",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
    "inheritedRunners",
    "inheritedRunnersScored",
    "innings",
    "outs",
]

DEFENSIVE_STATS = [
    "gamesPlayed",
    "plateAppearances",
    "atBats",
    "assists",
    "catcherERA",
    "chances",
    "doublePlays",
    "errors",
    "fielding",
    "games",
    "passedBall",
    "putOuts",
    "rangeFactorPer9Inn",
    "rangeFactorPerGame",
    "throwingErrors",
    "triplePlays",
]

# list of expected output columns
def expected_output_columns() -> list[str]:
    return sorted(
        [
            "playername",
            "gamesPlayed",
            "gamesStarted",
            "caughtStealing",
            "stolenBases",
            "stolenBasePercentage",
            "assists",
            "putOuts",
            "errors",
            "chances",
            "fielding",
            "rangeFactorPerGame",
            "rangeFactorPer9Inn",
            "innings",
            "games",
            "passedBall",
            "doublePlays",
            "triplePlays",
            "catcherERA",
            "catchersInterference",
            "wildPitches",
            "throwingErrors",
            "pickoffs",
            "groundOuts",
            "airOuts",
            "runs",
            "doubles",
            "triples",
            "homeRuns",
            "strikeOuts",
            "baseOnBalls",
            "intentionalWalks",
            "hits",
            "hitByPitch",
            "avg",
            "atBats",
            "obp",
            "slg",
            "ops",
            "groundIntoDoublePlay",
            "numberOfPitches",
            "plateAppearances",
            "totalBases",
            "rbi",
            "leftOnBase",
            "sacBunts",
            "sacFlies",
            "babip",
            "groundOutsToAirouts",
            "atBatsPerHomeRun",
            "era",
            "inningsPitched",
            "wins",
            "losses",
            "saves",
            "saveOpportunities",
            "holds",
            "blownSaves",
            "earnedRuns",
            "whip",
            "battersFaced",
            "outs",
            "gamesPitched",
            "completeGames",
            "shutouts",
            "strikes",
            "strikePercentage",
            "hitBatsmen",
            "balks",
            "winPercentage",
            "pitchesPerInning",
            "gamesFinished",
            "strikeoutWalkRatio",
            "strikeoutsPer9Inn",
            "walksPer9Inn",
            "hitsPer9Inn",
            "runsScoredPer9",
            "homeRunsPer9",
            "inheritedRunners",
            "inheritedRunnersScored",
            "team_id",
        ]
    )
