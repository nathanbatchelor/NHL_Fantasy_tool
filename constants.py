"""
Central configuration file for the Fantasy Hockey Tool.
Contains all static variables, API endpoints, and scoring rules
to ensure consistency across all scripts.
"""

import os

# --- API & SEASON CONFIG ---
SEASON_ID = "20252026"
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

# Cache for Phase 2 (Schedule Script)
SCHEDULE_CACHE = os.path.join(DATA_DIR, f"nhl_schedule_{SEASON_ID}.json")
# *** NEW ***
# Cache for player_stats_fetcher (Seeder & Daily Update)
GAME_STATS_CACHE = os.path.join(DATA_DIR, f"game_stats_cache_{SEASON_ID}.json")

# Output CSVs
WEEKLY_SCHEDULE_CSV = os.path.join(DATA_DIR, "team_weekly_schedule.csv")

AVOID_PLAYER_ESPN_IDS = [5238086]

TEAM_MAP = {
    "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "CGY": "Calgary Flames",
    "CHI": "Chicago Blackhawks",
    "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "CAR": "Carolina Hurricanes",
    "LAK": "Los Angeles Kings",
    "DAL": "Dallas Stars",
    "MTL": "Montréal Canadiens",
    "NJD": "New Jersey Devils",
    "NYI": "New York Islanders",
    "NYR": "New York Rangers",
    "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "COL": "Colorado Avalanche",
    "SJS": "San Jose Sharks",
    "STL": "St. Louis Blues",
    "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "VAN": "Vancouver Canucks",
    "WSH": "Washington Capitals",
    "ANA": "Anaheim Ducks",
    "FLA": "Florida Panthers",
    "NSH": "Nashville Predators",
    "WPG": "Winnipeg Jets",
    "CBJ": "Columbus Blue Jackets",
    "MIN": "Minnesota Wild",
    "VGK": "Vegas Golden Knights",
    "SEA": "Seattle Kraken",
    "UTA": "Utah Hockey Club",  # change to mammoth if having issues
}

# Reverse mapping: full team name -> abbreviation
TEAM_MAP_REVERSE = {name: abbr for abbr, name in TEAM_MAP.items()}


# --- OTHER STATIC DATA ---
NHL_TEAMS = [
    "ANA",
    "BOS",
    "BUF",
    "CGY",
    "CAR",
    "CHI",
    "COL",
    "CBJ",
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
    "SEA",
    "STL",
    "TBL",
    "TOR",
    "UTA",
    "VAN",
    "VGK",
    "WSH",
    "WPG",
]
