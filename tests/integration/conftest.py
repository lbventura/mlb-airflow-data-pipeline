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
