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

# read the league name set at runtime, fallback to default if file doesn't exist
try:
    with open(LEAGUE_NAME_LOCATION, "r") as text_file:
        LEAGUE_NAME = text_file.readline().strip()
except FileNotFoundError:
    # Keep the default value if file doesn't exist (useful for testing)
    pass

DATA_FILE_LOCATION = (
    "/root/mlb-airflow-data-pipeline/mlb_airflow_data_pipeline/db_data/"
)

OUTPUT_FILE_LOCATION = (
    "/root/mlb-airflow-data-pipeline/mlb_airflow_data_pipeline/output/"
)

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
            "date",
        ]
    )


def american_league_team_id_name() -> dict:
    return {
        140: "Texas Rangers",
        117: "Houston Astros",
        136: "Seattle Mariners",
        108: "Los Angeles Angels",
        133: "Oakland Athletics",
        139: "Tampa Bay Rays",
        110: "Baltimore Orioles",
        147: "New York Yankees",
        141: "Toronto Blue Jays",
        111: "Boston Red Sox",
        142: "Minnesota Twins",
        116: "Detroit Tigers",
        114: "Cleveland Guardians",
        145: "Chicago White Sox",
        118: "Kansas City Royals",
    }


def national_league_team_id_name() -> dict:
    return {
        109: "Arizona Diamondbacks",
        119: "Los Angeles Dodgers",
        137: "San Francisco Giants",
        135: "San Diego Padres",
        115: "Colorado Rockies",
        144: "Atlanta Braves",
        146: "Miami Marlins",
        121: "New York Mets",
        143: "Philadelphia Phillies",
        120: "Washington Nationals",
        158: "Milwaukee Brewers",
        134: "Pittsburgh Pirates",
        113: "Cincinnati Reds",
        112: "Chicago Cubs",
        138: "St. Louis Cardinals",
    }
