"""SQLite schema for treated statistics; extraction tables are not defined here.

batter_stats and pitcher_stats have one row per league/date/team/player.
defender_stats adds position to that identity for grouped statistics.
Historical CSV rows may lack player IDs/positions and retain their provenance.
treated_snapshots records completed categories, including empty results.
csv_imports records verified historical files and is created only during import.

TABLE_SCHEMAS maps each table name to its columns, constraints, and indexes.
Shared statistic groups define metric columns; readers use these same declarations.
db_utils owns execution and transactions.
"""

from mlb_airflow_data_pipeline.statsapi_parameters_script import (
    BATTING_STATS,
    DEFENSIVE_STATS,
    PITCHING_STATS,
)

BATTER_PLATE_STATS = [
    "strikeOuts",
    "homeRuns",
    "hits",
    "rbi",
    "baseOnBalls",
    "totalBases",
    "difstrikeOutsbaseOnBalls",
]
BATTER_MEAN_STATS = [
    "avg",
    "babip",
    "obp",
    "ops",
    "hits",
    "rbi",
    "baseOnBalls",
    "totalBases",
    "difstrikeOutsbaseOnBalls",
]
PITCHER_INNINGS_STATS = ["intentionalWalks", "numberOfPitches", "strikes", "outs"]
PITCHER_MEAN_STATS = [
    "era",
    "whip",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
]
DEFENDER_MEAN_STATS = ["catcherERA", "assists", "errors", "doublePlays", "fielding"]
BASE_STATS = {
    "batter": BATTING_STATS,
    "pitcher": PITCHING_STATS,
    "defender": DEFENSIVE_STATS,
}
RATE_STATS = {
    "avg",
    "obp",
    "slg",
    "ops",
    "groundOutsToAirouts",
    "inningsPitched",
    "era",
    "whip",
    "strikePercentage",
    "pitchesPerInning",
    "strikeoutsPer9Inn",
    "walksPer9Inn",
    "hitsPer9Inn",
    "homeRunsPer9",
    "catcherERA",
    "fielding",
    "rangeFactorPer9Inn",
    "rangeFactorPerGame",
}
LAYOUTS = ("grouped", "legacy_unprefixed")
TREATED_TABLES = {
    "batter": "batter_stats",
    "pitcher": "pitcher_stats",
    "defender": "defender_stats",
}
CATEGORIES = tuple(TREATED_TABLES)

IDENTITY_COLUMNS = {
    "row_id": "INTEGER PRIMARY KEY",
    "league_name": "TEXT NOT NULL",
    "date": "TEXT NOT NULL",
    "team_id": "INTEGER NOT NULL",
    "player_id": "INTEGER",
    "playername": "TEXT NOT NULL",
    "position": "TEXT",
    "layout": "TEXT NOT NULL CHECK (layout IN ('grouped','legacy_unprefixed'))",
    "origin": "TEXT NOT NULL CHECK (origin IN ('treatment','csv'))",
    "source_checksum": "TEXT",
    "source_row": "INTEGER",
    "source_index": "TEXT",
    "source_values": "TEXT",
}


PLAYER_CONSTRAINTS = (
    "CHECK (origin = 'csv' OR player_id IS NOT NULL)",
    "CHECK (origin != 'csv' OR (source_checksum IS NOT NULL AND source_row IS NOT NULL))",
    "UNIQUE (league_name, date, source_checksum, source_row)",
)


def _stat_columns(stats: list[str]) -> dict[str, str]:
    return {stat: "REAL" if stat in RATE_STATS else "INTEGER" for stat in stats}


def _mean_columns(stats: list[str]) -> dict[str, str]:
    return {
        column: "REAL"
        for stat in stats
        for column in (
            f"{stat}_mean",
            f"{stat}_std",
            f"normalized_{stat}",
            f"{stat}_z_score",
        )
    }


def _player_indexes(table: str, *, include_position: bool = False) -> tuple[str, ...]:
    position = ", COALESCE(position, '')" if include_position else ""
    return (
        f"""CREATE UNIQUE INDEX IF NOT EXISTS {table}_player_key
            ON {table} (league_name, date, team_id, player_id{position})
            WHERE player_id IS NOT NULL""",
        f"""CREATE UNIQUE INDEX IF NOT EXISTS {table}_legacy_key
            ON {table} (league_name, date, team_id, playername{position})
            WHERE player_id IS NULL""",
        f"CREATE INDEX IF NOT EXISTS {table}_scope ON {table} (league_name, date)",
    )


TABLE_SCHEMAS: dict[str, dict] = {
    "batter_stats": {
        "columns": {
            **IDENTITY_COLUMNS,
            **_stat_columns(BATTING_STATS),
            "babip": "REAL",
            "difstrikeOutsbaseOnBalls": "INTEGER",
            **{f"{stat}perplateAppearance": "REAL" for stat in BATTER_PLATE_STATS},
            **_mean_columns(BATTER_MEAN_STATS),
        },
        "constraints": PLAYER_CONSTRAINTS,
        "indexes": _player_indexes("batter_stats"),
    },
    "pitcher_stats": {
        "columns": {
            **IDENTITY_COLUMNS,
            **_stat_columns(PITCHING_STATS),
            "innings": "REAL",  # Retained for historical CSV imports.
            **{f"{stat}inningsPitched": "REAL" for stat in PITCHER_INNINGS_STATS},
            **_mean_columns(PITCHER_MEAN_STATS),
        },
        "constraints": PLAYER_CONSTRAINTS,
        "indexes": _player_indexes("pitcher_stats"),
    },
    "defender_stats": {
        "columns": {
            **IDENTITY_COLUMNS,
            **_stat_columns(DEFENSIVE_STATS),
            **_mean_columns(DEFENDER_MEAN_STATS),
        },
        "constraints": (
            *PLAYER_CONSTRAINTS,
            "CHECK (layout != 'grouped' OR position IN ('C','1B','2B','3B','SS','LF','CF','RF'))",
            "CHECK (layout != 'grouped' OR position IS NOT NULL)",
        ),
        "indexes": _player_indexes("defender_stats", include_position=True),
    },
    "treated_snapshots": {
        "columns": {
            "league_name": "TEXT NOT NULL",
            "date": "TEXT NOT NULL",
            "category": "TEXT NOT NULL",
            "layout": "TEXT NOT NULL CHECK (layout IN ('grouped', 'legacy_unprefixed'))",
            "origin": "TEXT NOT NULL CHECK (origin IN ('treatment', 'csv'))",
            "row_count": "INTEGER NOT NULL CHECK (row_count >= 0)",
        },
        "constraints": ("PRIMARY KEY (league_name, date, category)",),
        "indexes": (),
    },
    "csv_imports": {
        "columns": {
            "filename": "TEXT PRIMARY KEY",
            "checksum": "TEXT NOT NULL",
            "league_name": "TEXT NOT NULL",
            "date": "TEXT NOT NULL",
            "category": "TEXT NOT NULL",
            "row_count": "INTEGER NOT NULL",
            "inventory_json": "TEXT NOT NULL",
        },
        "constraints": (),
        "indexes": (),
    },
}


def metric_types(category: str) -> dict[str, str]:
    """Return the metric columns declared in the role's table schema."""
    if category not in TREATED_TABLES:
        raise ValueError(f"Unsupported category: {category}")
    columns = TABLE_SCHEMAS[TREATED_TABLES[category]]["columns"]
    return {
        name: kind for name, kind in columns.items() if name not in IDENTITY_COLUMNS
    }


def table_statements(table_name: str) -> tuple[str, ...]:
    """Render a declared table and its indexes without changing a database."""
    schema = TABLE_SCHEMAS[table_name]
    definitions = [f'"{name}" {kind}' for name, kind in schema["columns"].items()]
    definitions.extend(schema["constraints"])
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(definitions)})",
        *schema["indexes"],
    )
