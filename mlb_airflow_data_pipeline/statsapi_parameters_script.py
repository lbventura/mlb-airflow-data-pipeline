import os
from datetime import datetime
from pathlib import Path

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

LEAGUE_MAPPING = {"american_league": 103, "national_league": 104}

LEAGUE_DIVISION_MAPPING = {103: [200, 201, 202], 104: [203, 204, 205]}

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# setting "national_league" as the default league name
LEAGUE_NAME = "national_league"
LEAGUE_NAME_LOCATION = str(PROJECT_ROOT / "league_name_choice.txt")

# if there is not an ongoing season, this parameter has to be set to the
# previous year.
SEASON_YEAR = 2026
# set this parameter to False to fetch player career stats
IS_SEASON_STATS = True

# read the league name set at runtime, fallback to default if file doesn't exist
try:
    with open(LEAGUE_NAME_LOCATION, "r") as text_file:
        LEAGUE_NAME = text_file.readline().strip()
except FileNotFoundError:
    # Keep the default value if file doesn't exist (useful for testing)
    pass

DATA_FILE_LOCATION = os.path.join(
    os.environ.get(
        "MLB_AIRFLOW_DATA_DIR",
        str(PROJECT_ROOT / "mlb_airflow_data_pipeline" / "db_data"),
    ),
    "",
)

OUTPUT_FILE_LOCATION = os.path.join(
    PROJECT_ROOT, "mlb_airflow_data_pipeline", "output", ""
)

# player information fields
PLAYER_INFORMATION = ["playername", "team_id"]

DEFENDER_POSITIONS = ("C", "1B", "2B", "3B", "SS", "LF", "CF", "RF")
# Retain these source splits even though defender reports exclude P and DH.
FIELDING_POSITIONS = ("P", *DEFENDER_POSITIONS, "DH")

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

# `innings` comes from fielding; pitching uses `inningsPitched`.
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


# StatsAPI emits more fields than treatment currently uses. Keep these source
# names separate so extraction can validate its complete grouped schema.
BATTING_SOURCE_STATS = set(BATTING_STATS) | {
    "age",
    "atBatsPerHomeRun",
    "babip",
    "caughtStealingPercentage",
    "numberOfPitches",
    "stolenBasePercentage",
}

# In pitching, homeRuns/hits/baseOnBalls are allowed to opponents, and
# atBats/avg describe opponents. numberOfPitches counts pitches thrown;
# its batting counterpart counts pitches seen.
PITCHING_SOURCE_STATS = set(PITCHING_STATS) | {
    "age",
    "airOuts",
    "atBats",
    "avg",
    "baseOnBalls",
    "catchersInterference",
    "caughtStealing",
    "caughtStealingPercentage",
    "doubles",
    "groundIntoDoublePlay",
    "groundOuts",
    "groundOutsToAirouts",
    "hitByPitch",
    "hits",
    "homeRuns",
    "obp",
    "ops",
    "runs",
    "runsScoredPer9",
    "sacBunts",
    "sacFlies",
    "slg",
    "stolenBasePercentage",
    "stolenBases",
    "strikeOuts",
    "strikeoutWalkRatio",
    "totalBases",
    "triples",
    "winPercentage",
}

FIELDING_SOURCE_STATS = (set(DEFENSIVE_STATS) - {"atBats", "plateAppearances"}) | {
    "age",
    "catchersInterference",
    "caughtStealing",
    "caughtStealingPercentage",
    "gamesStarted",
    "innings",
    "pickoffs",
    "stolenBasePercentage",
    "stolenBases",
    "wildPitches",
}


def expected_output_columns() -> list[str]:
    """Supported grouped columns for the fixed source position vocabulary."""
    columns = {"playername", "team_id", "date"}
    columns.update(f"hitting_{stat}" for stat in BATTING_SOURCE_STATS)
    columns.update(f"pitching_{stat}" for stat in PITCHING_SOURCE_STATS)
    columns.update(
        f"fielding_{position}_{stat}"
        for position in FIELDING_POSITIONS
        for stat in FIELDING_SOURCE_STATS
    )
    return sorted(columns)


def american_league_team_id_name() -> dict:
    return {
        140: "Texas Rangers",
        117: "Houston Astros",
        136: "Seattle Mariners",
        108: "Los Angeles Angels",
        133: "Athletics",  # They are no longer the "Oakland Athletics" in the MLB API
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
