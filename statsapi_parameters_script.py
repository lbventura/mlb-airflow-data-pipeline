from datetime import datetime

DATE_TIME_EXECUTION = datetime.today().strftime("%Y-%m-%d")

LEAGUE_MAPPING = {103: "american_league", 104: "national_league"}

LEAGUE_DIVISION_MAPPING = {103: [200, 201, 202], 104: [203, 204, 205]}

# setting "national_league" as the default league name
LEAGUE_NAME = "national_league"
LEAGUE_NAME_LOCATION = "/root/mlb-airflow-data-pipeline/league_name_choice.txt"

# if there is not an ongoing season, this parameter has to be set to the
# previous year. Otherwise it should be set to None
SEASON_YEAR = 2023
IS_SEASON_STATS = True

# read the league name set at runtime
with open(LEAGUE_NAME_LOCATION, "r") as text_file:
    LEAGUE_NAME = text_file.readline().strip()

DATA_FILE_LOCATION = "/root/mlb-airflow-data-pipeline/data/"

OUTPUT_FILE_LOCATION = "/root/mlb-airflow-data-pipeline/output/"

LEAGUE_STANDINGS_FILE_NAME = (
    f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}_league_standings_df.csv"
)

PLAYER_DATA_FILE_NAME = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}_full_player_stats_df.csv"

DATA_FILTER_THRESHOLD = 0.6

OUTPUT_DETAILS = f"{LEAGUE_NAME}_{DATE_TIME_EXECUTION}"

BATTER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_batter_stats_df.csv"
PITCHER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_pitcher_stats_df.csv"
DEFENDER_DATA_FILE_NAME = f"{OUTPUT_DETAILS}_defender_stats_df.csv"
