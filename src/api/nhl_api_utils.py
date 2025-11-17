"""
src/api/nhl_api_utils.py
All functions for fetching data from the NHL API.
"""

import httpx  # <-- NEW
import asyncio  # <-- NEW
import time
from pydantic import ValidationError

import src.core.constants as constants
from .models import GamesResponse

# Import other utils
from src.utils.cache_utils import load_data_from_cache, save_data_to_cache

# --- Concurrency Settings ---
CONCURRENCY_LIMIT = 50  # Max concurrent requests

# --- Schedule Fetching ---


async def fetch_team_schedule(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, team_abbv: str
) -> dict:
    """Fetch schedule for a single team"""
    team_schedule_url = (
        f"{constants.WEB_URL}/club-schedule-season/{team_abbv}/{constants.SEASON_ID}"
    )
    async with semaphore:
        try:
            resp = await client.get(team_schedule_url, timeout=10.0)
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
        except (ValidationError, httpx.RequestError, Exception) as e:
            print(f"  Warning: Error fetching {team_abbv}: {e}")
            return {}


async def get_all_unique_games(client: httpx.AsyncClient) -> dict:
    """Get all unique regular season games using concurrent requests"""
    unique_games = {}
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = []

    for team in constants.NHL_TEAMS:
        tasks.append(fetch_team_schedule(client, semaphore, team))

    results = await asyncio.gather(*tasks)
    for team_games in results:
        unique_games.update(team_games)
    return unique_games


async def get_schedule(force_refresh: bool = False) -> dict:
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

    async with httpx.AsyncClient() as client:
        unique_games = await get_all_unique_games(client)

    end_time = time.time()
    print(
        f"Successfully Fetched {len(unique_games)} games in {end_time - start_time:.2f}s"
    )
    save_data_to_cache(unique_games, constants.SCHEDULE_CACHE)
    return unique_games
