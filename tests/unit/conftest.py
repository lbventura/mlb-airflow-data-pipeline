import pytest


@pytest.fixture
def player_stats() -> list[str]:
    player_stats = [
        'Aaron "Baj" Judge, CF (2016-)',
        "",
        "Season Hitting",
        "gamesPlayed: 26",
        "groundOuts: 13",
        "airOuts: 22",
        "runs: 18",
        "doubles: 5",
        "triples: 0",
        "homeRuns: 6",
        "strikeOuts: 35",
        "baseOnBalls: 14",
        "intentionalWalks: 1",
        "hits: 24",
        "hitByPitch: 0",
        "avg: .261",
        "atBats: 92",
        "obp: .352",
        "slg: .511",
        "ops: .863",
        "caughtStealing: 1",
        "stolenBases: 2",
        "stolenBasePercentage: .667",
        "groundIntoDoublePlay: 0",
        "numberOfPitches: 456",
        "plateAppearances: 108",
        "totalBases: 47",
        "rbi: 14",
        "leftOnBase: 33",
        "sacBunts: 0",
        "sacFlies: 2",
        "babip: .340",
        "groundOutsToAirouts: 0.59",
        "catchersInterference: 0",
        "atBatsPerHomeRun: 15.33",
        "",
        "Season Fielding (CF)",
        "gamesPlayed: 17",
        "gamesStarted: 16",
        "assists: 0",
        "putOuts: 43",
        "errors: 0",
        "chances: 43",
        "fielding: 1.000",
        "rangeFactorPerGame: 2.53",
        "rangeFactorPer9Inn: 2.91",
        "innings: 133.0",
        "games: 17",
        "doublePlays: 0",
        "triplePlays: 0",
        "throwingErrors: 0",
        "",
        "Season Fielding (RF)",
        "gamesPlayed: 9",
        "gamesStarted: 6",
        "assists: 0",
        "putOuts: 10",
        "errors: 0",
        "chances: 10",
        "fielding: 1.000",
        "rangeFactorPerGame: 1.11",
        "rangeFactorPer9Inn: 1.76",
        "innings: 51.0",
        "games: 9",
        "doublePlays: 0",
        "triplePlays: 0",
        "throwingErrors: 0",
        "",
        "Season Fielding (DH)",
        "gamesPlayed: 4",
        "gamesStarted: 4",
        "assists: 0",
        "putOuts: 0",
        "errors: 0",
        "chances: 0",
        "fielding: .000",
        "rangeFactorPerGame: 0.00",
        "rangeFactorPer9Inn: -.--",
        "innings: 0.0",
        "games: 4",
        "doublePlays: 0",
        "triplePlays: 0",
        "throwingErrors: 0",
        "",
        "",
    ]
    return player_stats


@pytest.fixture
def player_stats_list() -> list[str]:
    player_stats_list = [
        "airOuts",
        "assists",
        "atBats",
        "atBatsPerHomeRun",
        "avg",
        "babip",
        "baseOnBalls",
        "catchersInterference",
        "caughtStealing",
        "chances",
        "doublePlays",
        "doubles",
        "errors",
        "fielding",
        "games",
        "gamesPlayed",
        "gamesStarted",
        "groundIntoDoublePlay",
        "groundOuts",
        "groundOutsToAirouts",
        "hitByPitch",
        "hits",
        "homeRuns",
        "innings",
        "intentionalWalks",
        "leftOnBase",
        "numberOfPitches",
        "obp",
        "ops",
        "plateAppearances",
        "putOuts",
        "rangeFactorPer9Inn",
        "rangeFactorPerGame",
        "rbi",
        "runs",
        "sacBunts",
        "sacFlies",
        "slg",
        "stolenBasePercentage",
        "stolenBases",
        "strikeOuts",
        "throwingErrors",
        "totalBases",
        "triplePlays",
        "triples",
    ]
    return player_stats_list
