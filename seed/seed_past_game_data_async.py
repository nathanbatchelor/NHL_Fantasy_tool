import constants
import pandas as pd
import numpy as np
import sys
from pathlib import Path
import time
import asyncio
import httpx
from models.api.stats import (
    PlayerGameLogResponse,
    GameBoxscoreResponse,
    PlayerStatsFromBoxscore,  # <--- Import new models
    GoalieStatsFromBoxscore,  # <--- Import new models
    FinalPlayerGameStats,  # <--- Import our new combined model
)
from collections import defaultdict
from typing import List, Union  # <--- Added for type hints

# --- Concurrency Settings ---
CONCURRENCY_LIMIT = 50

# --- Global Cache ---
# This will now store our new FinalPlayerGameStats model
# game_cache[game_id][player_id] = FinalPlayerGameStats(...)
game_cache = defaultdict(dict)


# --- Pydantic Models ---
# (You should move all your models to a separate file like `models/api/stats.py`)
# I'm defining the new combined model here for clarity.

from pydantic import BaseModel
from typing import Optional

# (Your existing models: TeamInfoAPI, PlayerStatsFromBoxscore, GoalieStatsFromBoxscore, etc.)
# (... PlayerGameLogEntry, PlayerGameLogResponse, GameBoxscoreResponse ...)
# (I am assuming these are all in your models.api.stats file)


class FinalPlayerGameStats(BaseModel):
    """
    Our new model to combine stats from the PlayerLog (Phase 1)
    and the Boxscore (Phase 2).
    """

    # Common keys
    playerId: int
    gameId: int
    teamAbbrev: str
    gameDate: str

    # From PlayerGameLogEntry (Phase 1)
    powerPlayPoints: int = 0
    shorthandedPoints: int = 0
    toi: str = "00:00"
    shifts: int = 0
    pim: int = 0

    # From PlayerStatsFromBoxscore (Phase 2)
    name: str = "N/A"
    position: str = "N/A"
    goals: int = 0
    assists: int = 0
    sog: int = 0
    blockedShots: int = 0
    hits: int = 0

    # From GoalieStatsFromBoxscore (Phase 2)
    saves: Optional[int] = None
    savePct: Optional[float] = None
    goalsAgainst: Optional[int] = None
    decision: Optional[str] = None


# --- Async Fetch Functions ---


async def fetch_player_log(client, semaphore, player_id):
    """
    PHASE 1: Fetches one player's log to find all the games they played.
    """
    url = f"{constants.WEB_URL}/player/{player_id}/game-log/{constants.SEASON_ID}/2"
    async with semaphore:
        try:
            res = await client.get(url, timeout=10.0)
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


async def fetch_game_boxscore(client, semaphore, game_id):  # <--- NEW FUNCTION
    """
    PHASE 2: Fetches the full boxscore for a single game.
    """
    # !!! YOU MAY NEED TO UPDATE THIS URL !!!
    url = f"{constants.WEB_URL}/gamecenter/{game_id}/boxscore"

    async with semaphore:
        try:
            res = await client.get(url, timeout=10.0)
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
            # This can happen if a game is postponed and has no data
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


def merge_goalie_stats(entry: FinalPlayerGameStats, stats: GoalieStatsFromBoxscore):
    """Helper function to update the cache entry with goalie stats."""
    entry.name = stats.name.get("default", "N/A")
    entry.position = stats.position
    entry.saves = stats.saves
    entry.savePct = stats.savePct
    entry.goalsAgainst = stats.goalsAgainst
    entry.decision = stats.decision


async def main():
    start_time = time.perf_counter()

    csv_path = Path(constants.SKATER_STATS_CSV)
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return

    skater_stats = pd.read_csv(constants.SKATER_STATS_CSV)
    player_ids = skater_stats["playerId"].dropna().unique().tolist()

    print(f"Found {len(player_ids)} unique player ids.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with httpx.AsyncClient() as client:

        # --- PHASE 1: Get Player Logs ---
        print(f"--- Starting Phase 1: Fetching {len(player_ids)} player game logs...")
        player_tasks = []
        for player_id in player_ids:
            task = fetch_player_log(client, semaphore, int(player_id))
            player_tasks.append(task)

        player_log_results = await asyncio.gather(*player_tasks)

        # --- Populate Cache (Phase 1 Data) ---
        print("Phase 1 complete. Populating initial cache...")
        for result in player_log_results:
            if result is None:
                continue

            player_id, log_response = result
            for game in log_response.gameLog:
                # Create the new, combined stats object
                final_stats = FinalPlayerGameStats(
                    playerId=player_id,
                    gameId=game.gameId,
                    teamAbbrev=game.teamAbbrev,
                    gameDate=game.gameDate,
                    powerPlayPoints=game.powerPlayPoints,
                    shorthandedPoints=game.shorthandedPoints,
                    toi=game.toi,
                    shifts=game.shifts,
                    pim=game.pim,
                )
                game_cache[game.gameId][player_id] = final_stats

        phase1_time = time.perf_counter()
        print(
            f"Found {len(game_cache)} unique games. (Phase 1 took {phase1_time - start_time:.2f}s)"
        )

        # --- PHASE 2: Get Boxscores ---
        game_ids_to_fetch = list(game_cache.keys())
        print(
            f"--- Starting Phase 2: Fetching {len(game_ids_to_fetch)} game boxscores..."
        )

        boxscore_tasks = []
        for game_id in game_ids_to_fetch:
            task = fetch_game_boxscore(client, semaphore, game_id)
            boxscore_tasks.append(task)

        boxscore_results = await asyncio.gather(*boxscore_tasks)

        # --- PHASE 3: Merge Boxscore Data ---
        print("Phase 2 complete. Merging boxscore data into cache...")
        merge_count = 0
        for boxscore in boxscore_results:
            if boxscore is None:
                continue

            game_id = boxscore.id

            # Combine all skater/goalie lists
            all_players: List[
                Union[PlayerStatsFromBoxscore, GoalieStatsFromBoxscore]
            ] = []
            all_players.extend(boxscore.playerByGameStats.awayTeam.forwards)
            all_players.extend(boxscore.playerByGameStats.awayTeam.defense)
            all_players.extend(boxscore.playerByGameStats.awayTeam.goalies)
            all_players.extend(boxscore.playerByGameStats.homeTeam.forwards)
            all_players.extend(boxscore.playerByGameStats.homeTeam.defense)
            all_players.extend(boxscore.playerByGameStats.homeTeam.goalies)

            for player_stats in all_players:
                player_id = player_stats.playerId

                # Check if this player is one we care about (from Phase 1)
                if player_id in game_cache[game_id]:
                    # Get the entry we made in Phase 1
                    entry_to_update = game_cache[game_id][player_id]

                    # Update it with Phase 2 data
                    if isinstance(player_stats, PlayerStatsFromBoxscore):
                        merge_skater_stats(entry_to_update, player_stats)
                        merge_count += 1
                    elif isinstance(player_stats, GoalieStatsFromBoxscore):
                        merge_goalie_stats(entry_to_update, player_stats)
                        merge_count += 1

    # --- Done ---
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\n--- All Phases Complete! ---")
    print(f"Total games in cache: {len(game_cache)}")
    print(f"Total player-game entries merged: {merge_count}")
    print(f"Total execution time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    # You must have your models defined or imported before this runs
    asyncio.run(main())
