import pytest


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
            "primaryPosition": {"code": "8", "abbreviation": "CF"},
            "useName": "Aaron",
            "boxscoreName": "Judge",
            "nickName": "Baj",
            "mlbDebutDate": "2016-08-13",
            "nameFirstLast": "Aaron Judge",
            "firstLastName": "Aaron Judge",
            "lastFirstName": "Judge, Aaron",
            "lastInitName": "Judge, A",
            "initLastName": "A Judge",
            "fullFMLName": "Aaron James Judge",
            "fullLFMName": "Judge, Aaron James",
        }
    ]
