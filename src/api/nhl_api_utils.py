"""
src/api/nhl_api_utils.py
All functions for fetching data from the NHL API.
"""

from typing import Dict
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import httpx
import asyncio
import time
from pydantic import ValidationError

import src.core.constants as constants
from .models import GamesResponse

# Import other utils
from src.utils.cache_utils import load_data_from_cache, save_data_to_cache

# --- Schedule Fetching ---


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.HTTPError),
)
async def fetch_team_schedule(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, team_abbv: str
) -> dict:
    """Fetch schedule for a single team"""
    team_schedule_url = (
        f"{constants.WEB_URL}/club-schedule-season/{team_abbv}/{constants.SEASON_ID}"
    )
    async with semaphore:
        try:
            resp = await client.get(team_schedule_url, timeout=constants.API_TIMEOUT)
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
    semaphore = asyncio.Semaphore(constants.CONCURRENCY_LIMIT)
    tasks = []

    for team in constants.NHL_TEAMS:
        tasks.append(fetch_team_schedule(client, semaphore, team))

    results = await asyncio.gather(*tasks)
    for team_games in results:
        unique_games.update(team_games)
    return unique_games


async def get_schedule(force_refresh: bool = False) -> Dict[str, dict]:
    """
    Get schedule - from cache or API.

    Returns:
        Dict mapping game_id (as string) to game data dict containing:
        - date: str (UTC timestamp)
        - home_team: str
        - away_team: str
        - home_abbrev: str
        - away_abbrev: str
    """
    if not force_refresh:
        cached = load_data_from_cache(constants.SCHEDULE_CACHE)
        if cached and isinstance(cached, dict):
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