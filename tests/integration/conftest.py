from pathlib import Path

import pandas as pd
import pytest

from mlb_airflow_data_pipeline import statsapi_treatment_script as treatment
from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    PITCHING_STATS,
)

from .database_helpers import COUNTS, DATE, LEAGUE, extract


@pytest.fixture
def lookup_player_expected_result() -> list[dict]:
    return [
        {
            "id": 592450,
            "fullName": "Aaron Judge",
            "firstName": "Aaron",
            "lastName": "Judge",
            "primaryNumber": "99",
            "currentTeam": {"id": 147},
            "primaryPosition": {"code": "9", "abbreviation": "RF"},
            "useName": "Aaron",
            "boxscoreName": "Judge",
            "nickName": "Baj",
            "mlbDebutDate": "2016-08-13",
            "nameFirstLast": "Aaron Judge",
            "nameSlug": "aaron-judge-592450",
            "firstLastName": "Aaron Judge",
            "lastFirstName": "Judge, Aaron",
            "lastInitName": "Judge, A",
            "initLastName": "A Judge",
            "fullFMLName": "Aaron James Judge",
            "fullLFMName": "Judge, Aaron James",
        }
    ]


@pytest.fixture
def players() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "playername": "Same Name",
                "team_id": 147,
                **{f"hitting_{stat}": 1 for stat in BATTING_STATS},
                **{f"pitching_{stat}": 1 for stat in PITCHING_STATS},
                "hitting_atBats": 100 + n,
                "hitting_plateAppearances": 110 + n,
                "hitting_hits": 40 + n,
                "pitching_gamesPlayed": 6,
                "pitching_inningsPitched": "20.1",
                "pitching_outs": 61,
                "fielding_C_gamesPlayed": 2,
                "fielding_C_assists": 1 + n,
                "fielding_1B_gamesPlayed": 3,
                "fielding_1B_assists": 2 + n,
            }
            for n in (0, 1)
        ],
        index=[101, 102],
    )


@pytest.fixture
def database(tmp_path: Path, players: pd.DataFrame) -> Path:
    path = tmp_path / "mlb_data.db"
    extract(path, players)
    return path


@pytest.fixture
def published_database(database: Path) -> Path:
    assert treatment.treat_player_stats(str(database), LEAGUE, DATE) == COUNTS
    return database


@pytest.fixture
def report_database(database: Path, players: pd.DataFrame) -> Path:
    for league in (LEAGUE, "national_league"):
        extract(
            database,
            players if league == LEAGUE else players.assign(team_id=119),
            league,
        )
        treatment.treat_player_stats(str(database), league, DATE)
    extract(database, players, date="2026-10-01")
    treatment.treat_player_stats(str(database), LEAGUE, "2026-10-01")
    return database
