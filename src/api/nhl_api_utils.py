import httpx
import asyncio
import time
from typing import Any, Dict, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from pydantic import ValidationError

import src.core.constants as constants
from .models import GamesResponse
from src.utils.cache_utils import load_data_from_cache, save_data_to_cache


# --- Schedule Fetching ---


async def get_schedule_for_season(
    season_id: str, force_refresh: bool = False
) -> Dict[str, Any]:
    """
    Fetches the NHL schedule for a specific season.
    """
    # We reuse the team schedule fetching logic, but for ALL teams for the target season.
    # This is more robust than the single /schedule/{season_id} endpoint which
    # might not return what we expect or might 404 for older seasons.

    print(f"  Fetching full schedule for season {season_id}...")

    async with httpx.AsyncClient() as client:
        # We fetch every team's schedule for that season and combine them
        unique_games = await get_all_unique_games(client, season_id=season_id)

    return unique_games


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.HTTPError),
)
async def fetch_team_schedule(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    team_abbv: str,
    season_id: Optional[str] = None,
) -> dict:
    """
    Fetch schedule for a single team.

    Args:
        client: The async HTTP client
        semaphore: Concurrency limiter
        team_abbv: The team abbreviation (e.g. 'TOR')
        season_id: Optional season ID (e.g. '20242025'). Defaults to current season.
    """
    # Use the provided season_id, or fall back to the constant
    target_season = season_id if season_id else constants.SEASON_ID

    team_schedule_url = (
        f"{constants.WEB_URL}/club-schedule-season/{team_abbv}/{target_season}"
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
                        "game_id": str(game.id),  # Ensure string ID for consistency
                        "date": game.startTimeUTC,
                        "home_team": game.homeTeam.commonName.default,
                        "away_team": game.awayTeam.commonName.default,
                        "home_abbrev": game.homeTeam.abbrev,
                        "away_abbrev": game.awayTeam.abbrev,
                        "id": game.id,
                    }
            return team_games
        except (ValidationError, httpx.RequestError, Exception) as e:
            # Only print warning if it's not just a "season not found" for a team that didn't exist
            # But for now, printing is safer.
            print(
                f"  Warning: Error fetching {team_abbv} for season {target_season}: {e}"
            )
            return {}


async def get_all_unique_games(
    client: httpx.AsyncClient, season_id: Optional[str] = None
) -> dict:
    """
    Get all unique regular season games using concurrent requests.
    Accepts an optional season_id.
    """
    unique_games = {}
    semaphore = asyncio.Semaphore(constants.CONCURRENCY_LIMIT)
    tasks = []

    for team in constants.NHL_TEAMS:
        # Pass the season_id down to the fetcher
        tasks.append(fetch_team_schedule(client, semaphore, team, season_id=season_id))

    results = await asyncio.gather(*tasks)
    for team_games in results:
        unique_games.update(team_games)
    return unique_games


async def get_schedule(force_refresh: bool = False) -> Dict[str, dict]:
    """
    Get schedule for the CURRENT season - from cache or API.
    """
    if not force_refresh:
        cached = load_data_from_cache(constants.SCHEDULE_CACHE)
        if cached and isinstance(cached, dict):
            return cached

    print("Fetching fresh schedule from NHL API...")
    start_time = time.time()

    async with httpx.AsyncClient() as client:
        # Default behavior uses constants.SEASON_ID
        unique_games = await get_all_unique_games(client)

    end_time = time.time()
    print(
        f"Successfully Fetched {len(unique_games)} games in {end_time - start_time:.2f}s"
    )
    save_data_to_cache(unique_games, constants.SCHEDULE_CACHE)
    return unique_games
