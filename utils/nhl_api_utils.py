"""
utils/nhl_api_utils.py
All functions for fetching data from the NHL API.
"""

import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pydantic import ValidationError

import constants
from models.api.schedule import GamesResponse

# Import other utils
from utils.utils import load_data_from_cache, save_data_to_cache
from utils.retry_utils import safe_get


# --- Player Stats Fetching ---


def fetch_stats_data(url: str) -> list:
    """
    Fetches data from a specific NHL stats/rest endpoint.
    (Used by seed_player_stats.py)
    """
    print(f"  Fetching from: {url}")
    try:
        resp = safe_get(url)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []


# --- Schedule Fetching ---


def fetch_team_schedule(team_abbv: str) -> dict:
    """Fetch schedule for a single team"""
    team_schedule_url = (
        f"{constants.WEB_URL}/club-schedule-season/{team_abbv}/{constants.SEASON_ID}"
    )
    try:
        resp = safe_get(team_schedule_url)
        resp.raise_for_status()
        data = resp.json()
        games_response = GamesResponse(**data)
        team_games = {}
        for game in games_response.games:
            if game.gameType == 2:  # Regular season only
                team_games[game.id] = {
                    "date": game.startTimeUTC,
                    "home_team": game.homeTeam.commonName.default,
                    "away_team": game.awayTeam.commonName.default,
                    "home_abbrev": game.homeTeam.abbrev,
                    "away_abbrev": game.awayTeam.abbrev,
                }
        return team_games
    except (ValidationError, requests.RequestException) as e:
        print(f"  Warning: Error fetching {team_abbv}: {e}")
        return {}


def get_all_unique_games() -> dict:
    """Get all unique regular season games using concurrent requests"""
    unique_games = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_team = {
            executor.submit(fetch_team_schedule, team): team
            for team in constants.NHL_TEAMS
        }
        for future in as_completed(future_to_team):
            team_games = future.result()
            unique_games.update(team_games)
    return unique_games


def get_schedule(force_refresh: bool = False) -> dict:
    """
    Get schedule - from cache or API.
    This is the main reusable function for other scripts.
    """
    if not force_refresh:
        cached = load_data_from_cache(constants.SCHEDULE_CACHE)
        if cached:
            return cached

    print("Fetching fresh schedule from NHL API...")
    start_time = time.time()
    unique_games = get_all_unique_games()
    end_time = time.time()
    print(
        f"Successfully Fetched {len(unique_games)} games in {end_time - start_time:.2f}s"
    )
    save_data_to_cache(unique_games, constants.SCHEDULE_CACHE)
    return unique_games


# --- All other functions (like get_player_game_log_data, fetch_and_parse_game_data)
# --- have been removed as they are now handled by utils/player_stats_fetcher.py
