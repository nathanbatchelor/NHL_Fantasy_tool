import asyncio
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from typing import Optional, Tuple

import src.core.constants as constants
from src.api.models import (
    PlayerGameLogResponse,
    GameBoxscoreResponse,
    PlayerStatsFromBoxscore,
    GoalieStatsFromBoxscore,
    FinalPlayerGameStats,
)


def toi_to_seconds(toi_str: str) -> int:
    try:
        minutes, seconds = map(int, toi_str.split(":"))
        return (minutes * 60) + seconds
    except:
        return 0


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.HTTPError),
)
async def fetch_player_log(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, player_id: int
) -> Optional[Tuple[int, PlayerGameLogResponse]]:
    """Fetches one player's entire season game log."""
    url = f"{constants.WEB_URL}/player/{player_id}/game-log/{constants.SEASON_ID}/2"
    async with semaphore:
        try:
            res = await client.get(url, timeout=constants.API_TIMEOUT)
            res.raise_for_status()
            data = res.json()
            if not data:
                return None
            log_response = PlayerGameLogResponse(**data)
            return (player_id, log_response)
        except httpx.RequestError as e:
            print(f"Error (Player Log {player_id}): {e}")
            return None
        except Exception as e:
            print(f"Error (Player Log {player_id} parsing): {e}")
            return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.HTTPError),
)
async def fetch_game_boxscore(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, game_id: int
) -> Optional[GameBoxscoreResponse]:
    """Fetches the full boxscore for a single game."""
    url = f"{constants.WEB_URL}/gamecenter/{game_id}/boxscore"

    async with semaphore:
        try:
            res = await client.get(url, timeout=constants.API_TIMEOUT)
            res.raise_for_status()
            data = res.json()
            if not data:
                return None

            boxscore_response = GameBoxscoreResponse(**data)
            return boxscore_response

        except httpx.RequestError as e:
            print(f"Error (Boxscore {game_id}): {e}")
            return None
        except Exception as e:
            print(f"Error (Boxscore {game_id} parsing): {e}")
            return None


def merge_skater_stats(entry: FinalPlayerGameStats, stats: PlayerStatsFromBoxscore):
    """Helper function to update the cache entry with skater stats."""
    entry.name = stats.name.get("default", "N/A")
    entry.position = stats.position
    entry.goals = stats.goals
    entry.assists = stats.assists
    entry.sog = stats.sog
    entry.blockedShots = stats.blockedShots
    entry.hits = stats.hits
    entry.sweaterNumber = stats.sweaterNumber


def merge_goalie_stats(entry: FinalPlayerGameStats, stats: GoalieStatsFromBoxscore):
    """Helper function to update the cache entry with goalie stats."""
    entry.name = stats.name.get("default", "N/A")
    entry.position = stats.position
    entry.saves = stats.saves
    entry.savePctg = stats.savePctg
    entry.goalsAgainst = stats.goalsAgainst
    entry.decision = stats.decision
    entry.sweaterNumber = stats.sweaterNumber


def calculate_fantasy_points_skater(stats: FinalPlayerGameStats) -> float:
    """Calculate fantasy points for a skater."""
    weights = constants.SKATER_FPTS_WEIGHTS
    fpts = (
        stats.goals * weights["goals"]
        + stats.assists * weights["assists"]
        + stats.powerPlayPoints * weights["ppPoints"]
        + stats.shorthandedPoints * weights["shPoints"]
        + stats.sog * weights["shots"]
        + stats.blockedShots * weights["blockedShots"]
        + stats.hits * weights["hits"]
    )
    return round(fpts, 2)


def calculate_fantasy_points_goalie(stats: FinalPlayerGameStats) -> float:
    """Calculate fantasy points for a goalie."""
    weights = constants.GOALIE_FPTS_WEIGHTS
    wins = 1 if stats.decision == constants.WIN_DECISION else 0
    ot_losses = 1 if stats.decision == constants.OT_LOSS_DECISION else 0
    shutouts = (
        1
        if (stats.goalsAgainst == 0 and stats.saves is not None and stats.saves > 0)
        else 0
    )
    fpts = (
        wins * weights["wins"]
        + (stats.goalsAgainst or 0) * weights["goalsAgainst"]
        + (stats.saves or 0) * weights["saves"]
        + shutouts * weights["shutouts"]
        + ot_losses * weights["otLosses"]
    )
    return round(fpts, 2)


def get_opponent_abbrev(boxscore: GameBoxscoreResponse, team_abbrev: str) -> str:
    """Determine opponent abbreviation based on player's team."""
    if team_abbrev == boxscore.homeTeam.abbrev:
        return boxscore.awayTeam.abbrev
    else:
        return boxscore.homeTeam.abbrev
