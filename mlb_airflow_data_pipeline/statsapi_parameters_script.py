from datetime import datetime
import os

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

DATA_FILE_LOCATION: str = f"{SOURCE_LOCATION}/data/"

OUTPUT_FILE_LOCATION = f"{SOURCE_LOCATION}/output/"
