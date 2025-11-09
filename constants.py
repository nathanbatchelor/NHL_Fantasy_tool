"""
Central configuration file for the Fantasy Hockey Tool.
Contains all static variables, API endpoints, and scoring rules
to ensure consistency across all scripts.
"""

import os

# --- API & SEASON CONFIG ---
SEASON_ID = "20252026"
ALL_PlAYERS_URL = "https://api.nhle.com/stats/rest/en"
WEB_URL = "https://api-web.nhle.com/v1"

DATABASE_FILE = "data/nhl_stats.db"

# --- FANTASY LEAGUE CONFIG ---
FANTASY_TIMEZONE = "US/Eastern"  # Options: "US/Eastern", "US/Central", "US/Mountain", "US/Pacific", etc.

# --- FANTASY LEAGUE SCORING WEIGHTS ---
# (Matches your script's calculations)

SKATER_FPTS_WEIGHTS = {
    "goals": 2,
    "assists": 1,
    "ppPoints": 0.5,
    "shPoints": 0.5,
    "shots": 0.1,
    "blockedShots": 0.5,
    "hits": 0.1,
}

GOALIE_FPTS_WEIGHTS = {
    "wins": 4,
    "goalsAgainst": -2,
    "saves": 0.2,
    "shutouts": 3,
    "otLosses": 1,
}

# --- DATA FILEPATHS ---
DATA_DIR = "data"
# Cache for Phase 1 (Seed Script)
SKATER_SUMMARY_CACHE = os.path.join(DATA_DIR, f"skater_summary_{SEASON_ID}.json")
SKATER_REALTIME_CACHE = os.path.join(DATA_DIR, f"skater_realtime_{SEASON_ID}.json")
GOALIE_SUMMARY_CACHE = os.path.join(DATA_DIR, f"goalie_summary_{SEASON_ID}.json")

# Cache for Phase 2 (Schedule Script)
SCHEDULE_CACHE = os.path.join(DATA_DIR, f"nhl_schedule_{SEASON_ID}.json")
# *** NEW ***
# Cache for player_stats_fetcher (Seeder & Daily Update)
GAME_STATS_CACHE = os.path.join(DATA_DIR, f"game_stats_cache_{SEASON_ID}.json")


# Output CSVs
SKATER_STATS_CSV = os.path.join(DATA_DIR, f"skater_stats_{SEASON_ID}.csv")
GOALIE_STATS_CSV = os.path.join(DATA_DIR, f"goalie_stats_{SEASON_ID}.csv")
WEEKLY_SCHEDULE_CSV = os.path.join(DATA_DIR, "team_weekly_schedule.csv")


# --- OTHER STATIC DATA ---
NHL_TEAMS = [
    "ANA",
    "BOS",
    "BUF",
    "CGY",
    "CAR",
    "CHI",
    "COL",
    "DAL",
    "DET",
    "EDM",
    "FLA",
    "LAK",
    "MIN",
    "MTL",
    "NSH",
    "NJD",
    "NYI",
    "NYR",
    "OTT",
    "PHI",
    "PIT",
    "SJS",
    "STL",
    "TBL",
    "TOR",
    "UTA",
    "VAN",
    "VGK",
    "WSH",
    "WPG",
    "SEA",
]
