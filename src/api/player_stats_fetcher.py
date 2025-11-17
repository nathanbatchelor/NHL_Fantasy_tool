"""
utils/player_stats_fetcher.py

This is the new shared core module for fetching, processing,
and caching all player game stats.

Both the seeder and the daily updater use this module.
"""

import src.core.constants as constants
import time
import asyncio
import httpx
from sqlmodel import Session
from src.database.database import engine
from src.database.models import PlayerGameStats, GoalieGameStats

# --- NEW IMPORT ---
from src.database.utils import bulk_merge_data
from src.api.models import (  # Corrected import path
    PlayerGameLogResponse,
    GameBoxscoreResponse,
    PlayerStatsFromBoxscore,
    GoalieStatsFromBoxscore,
    FinalPlayerGameStats,
)
from collections import defaultdict
from typing import List, Union, cast
from datetime import datetime
from src.utils.utils import load_data_from_cache, save_data_to_cache


# --- HELPERS ---
def toi_to_seconds(toi_str: str) -> int:
    try:
        minutes, seconds = map(int, toi_str.split(":"))
        return (minutes * 60) + seconds
    except:
        return 0


# --- Concurrency Settings ---
CONCURRENCY_LIMIT = 50  # Max concurrent requests

# --- Async Fetch Functions ---


async def fetch_player_log(client, semaphore, player_id):
    """
    Fetches one player's *entire* season game log.
    This is efficient as it's one call per player, and we
    can find all games they played in.
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


async def fetch_game_boxscore(client, semaphore, game_id):
    """
    Fetches the full boxscore for a single game.
    """
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
            print(f"Error (Boxscore {game_id} parsing): {e}")
            return None


# --- Stat Processing Helpers ---


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

    # Determine decision outcomes
    wins = 1 if stats.decision == "W" else 0
    ot_losses = 1 if stats.decision == "O" else 0
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


# --- Database Writer (MODIFIED) ---


def write_stats_to_db(game_cache: dict, boxscore_map: dict):
    """
    PHASE 5: Write all collected stats to the database.
    Uses a bulk merge for efficiency.
    """
    print("\n--- Phase 5: Writing to database...")

    skater_records_to_merge = []
    goalie_records_to_merge = []

    for game_id, players in game_cache.items():
        # Get the boxscore to determine opponent
        boxscore = boxscore_map.get(game_id)
        if not boxscore:
            print(
                f"Warning: No boxscore found for game {game_id}. Skipping DB write for this game."
            )
            continue

        for player_id, stats in players.items():
            opponent_abbrev = get_opponent_abbrev(boxscore, stats.teamAbbrev)

            team_name = constants.TEAM_MAP.get(stats.teamAbbrev)
            opponent_name = constants.TEAM_MAP.get(opponent_abbrev)

            # Determine if this is a skater or goalie
            if stats.position in ["G"]:
                # Goalie
                goalie_record = GoalieGameStats(
                    game_id=stats.gameId,
                    player_id=stats.playerId,
                    season=constants.SEASON_ID,
                    game_date=stats.gameDate,
                    team_abbrev=stats.teamAbbrev,
                    team_name=team_name,
                    opponent_abbrev=opponent_abbrev,
                    opponent_name=opponent_name,
                    player_name=stats.name,
                    jersey_number=stats.sweaterNumber,
                    position="Goalie",  # Use the simplified position
                    saves=stats.saves or 0,
                    save_pct=stats.savePctg or 0.0,
                    goals_against=stats.goalsAgainst or 0,
                    decision=stats.decision,
                    wins=1 if stats.decision == "W" else 0,
                    shutouts=(
                        1
                        if (stats.goalsAgainst == 0 and stats.saves and stats.saves > 0)
                        else 0
                    ),
                    ot_losses=1 if stats.decision == "O" else 0,
                    total_fpts=calculate_fantasy_points_goalie(stats),
                )
                goalie_records_to_merge.append(goalie_record)
            else:
                # Skater
                shooting_pct = None
                if stats.sog > 0:
                    shooting_pct = stats.goals / stats.sog

                position_name = constants.NHL_TO_ESPN_POSITION_MAP.get(
                    stats.position, stats.position
                )

                skater_record = PlayerGameStats(
                    game_id=stats.gameId,
                    player_id=stats.playerId,
                    season=constants.SEASON_ID,
                    game_date=stats.gameDate,
                    team_abbrev=stats.teamAbbrev,
                    team_name=team_name,
                    opponent_abbrev=opponent_abbrev,
                    opponent_name=opponent_name,
                    player_name=stats.name,
                    jersey_number=stats.sweaterNumber,
                    position=position_name,
                    goals=stats.goals,
                    assists=stats.assists,
                    pp_points=float(stats.powerPlayPoints),
                    sh_points=float(stats.shorthandedPoints),
                    shots=stats.sog,
                    shooting_pct=shooting_pct,
                    blocked_shots=stats.blockedShots,
                    hits=stats.hits,
                    total_fpts=calculate_fantasy_points_skater(stats),
                    toi_seconds=toi_to_seconds(stats.toi),
                    shifts=stats.shifts,
                )
                skater_records_to_merge.append(skater_record)

    # --- NEW: Use bulk_merge_data ---
    with Session(engine) as session:
        bulk_merge_data(session, skater_records_to_merge)
        bulk_merge_data(session, goalie_records_to_merge)

        # Commit all changes at once
        print(
            f"Committing {len(skater_records_to_merge)} skater and {len(goalie_records_to_merge)} goalie records..."
        )
        session.commit()

    print(f"✅ Database write complete!")
    print(f"  - Skaters: {len(skater_records_to_merge)} records")
    print(f"  - Goalies: {len(goalie_records_to_merge)} records")


# --- Main Orchestrator ---


async def process_games(game_ids_to_process: List[int], use_cache: bool):
    """
    Main orchestrator to fetch, process, and cache game stats.

    Args:
        game_ids_to_process: A list of game IDs to process.
        use_cache: If True, will load 'final' games from cache
                   and skip fetching them.
    """
    start_time = time.perf_counter()
    print(f"Processing {len(game_ids_to_process)} games. Cache enabled: {use_cache}")

    # In-memory cache for this run
    # game_cache_internal: { game_id: { player_id: FinalPlayerGameStats } }
    game_cache_internal = defaultdict(dict)
    # boxscore_map: { game_id: GameBoxscoreResponse }
    boxscore_map = {}

    # Load the on-disk cache
    game_stats_cache_on_disk = load_data_from_cache(constants.GAME_STATS_CACHE) or {}

    game_ids_to_fetch_fresh = []
    game_ids_from_cache = []

    if use_cache:
        for game_id in game_ids_to_process:
            cached_game = game_stats_cache_on_disk.get(str(game_id))
            # Load from cache *only* if it exists and is marked final
            if cached_game and cached_game.get("status") == "final":
                game_ids_from_cache.append(game_id)
            else:
                game_ids_to_fetch_fresh.append(game_id)
    else:
        # Not using cache, so all games are "fresh"
        game_ids_to_fetch_fresh = list(game_ids_to_process)

    print(f"  - Loading {len(game_ids_from_cache)} games from cache.")
    print(f"  - Fetching {len(game_ids_to_fetch_fresh)} new/updated games from API.")

    # --- Load Cached Games ---
    for game_id in game_ids_from_cache:
        cached_data = game_stats_cache_on_disk[str(game_id)]
        try:
            # Re-construct the Pydantic models from the cached dicts
            boxscore_map[game_id] = GameBoxscoreResponse(**cached_data["boxscore_raw"])
            for player_id_str, player_stats_dict in cached_data["players"].items():
                game_cache_internal[game_id][int(player_id_str)] = FinalPlayerGameStats(
                    **player_stats_dict
                )
        except Exception as e:
            # If cache is corrupted, just re-fetch the game
            print(
                f"Warning: Cache for game {game_id} corrupted, re-fetching. Error: {e}"
            )
            game_ids_to_fetch_fresh.append(game_id)

    # --- Fetch Fresh Games ---
    if game_ids_to_fetch_fresh:
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        async with httpx.AsyncClient() as client:
            # --- PHASE 1: Fetch boxscores ---
            # We fetch boxscores first to find out which players played
            print(
                f"\n--- Phase 1: Fetching {len(game_ids_to_fetch_fresh)} boxscores..."
            )
            boxscore_tasks = [
                fetch_game_boxscore(client, semaphore, game_id)
                for game_id in game_ids_to_fetch_fresh
            ]
            boxscore_results = await asyncio.gather(*boxscore_tasks)

            player_ids_to_fetch_log = set()
            for boxscore in boxscore_results:
                if boxscore is None:
                    continue

                # Add to our in-memory map
                boxscore_map[boxscore.id] = boxscore

                # Get all players from the game
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
                    player_ids_to_fetch_log.add(player_stats.playerId)

            phase1_time = time.perf_counter()
            print(
                f"Phase 1 complete. Found {len(player_ids_to_fetch_log)} unique players. ({phase1_time - start_time:.2f}s)"
            )

            # --- PHASE 2: Fetch player logs ---
            # Now fetch the logs for all players we found
            print(
                f"\n--- Phase 2: Fetching logs for {len(player_ids_to_fetch_log)} players..."
            )
            player_log_tasks = [
                fetch_player_log(client, semaphore, player_id)
                for player_id in player_ids_to_fetch_log
            ]
            player_log_results = await asyncio.gather(*player_log_tasks)

            # Populate the in-memory cache with partial data from logs
            for result in player_log_results:
                if result is None:
                    continue

                player_id, log_response = result
                for game in log_response.gameLog:
                    # Only add data for the games we are processing
                    if game.gameId in game_ids_to_fetch_fresh:
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
                        game_cache_internal[game.gameId][player_id] = final_stats

            phase2_time = time.perf_counter()
            print(
                f"Phase 2 complete. Populated internal cache. ({phase2_time - phase1_time:.2f}s)"
            )

            # --- PHASE 3: Merge boxscore data ---
            # Now, merge the boxscore data into the partially-filled cache
            print("\n--- Phase 3: Merging boxscore data...")
            merge_count = 0
            for game_id in game_ids_to_fetch_fresh:
                boxscore = boxscore_map.get(game_id)
                if not boxscore:
                    # print(f"Warning: No boxscore data for {game_id}, skipping merge.")
                    continue

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
                    # Check if this player is in our cache (i.e., we got log data)
                    if player_id in game_cache_internal[game_id]:
                        entry_to_update = game_cache_internal[game_id][player_id]

                        if isinstance(player_stats, PlayerStatsFromBoxscore):
                            merge_skater_stats(entry_to_update, player_stats)
                            merge_count += 1
                        elif isinstance(player_stats, GoalieStatsFromBoxscore):
                            merge_goalie_stats(entry_to_update, player_stats)
                            merge_count += 1

            phase3_time = time.perf_counter()
            print(
                f"Phase 3 complete. Merged {merge_count} records. ({phase3_time - phase2_time:.2f}s)"
            )

    # --- PHASE 4: Update Cache on Disk ---
    # (Outside the async block)
    print("\n--- Phase 4: Updating on-disk cache...")
    updated_count = 0
    for game_id in game_ids_to_fetch_fresh:
        boxscore = boxscore_map.get(game_id)
        if not boxscore:
            continue

        # We assume any game we processed is 'final'.
        # A more complex check could use `boxscore.gameState` if it were available
        # in the boxscore model, but for daily updates this is safe.
        status = "final"

        # Convert Pydantic models to dicts for JSON serialization
        players_dict = {
            pid: stats.model_dump()
            for pid, stats in game_cache_internal[game_id].items()
        }

        game_stats_cache_on_disk[str(game_id)] = {
            "cached_at": datetime.utcnow().isoformat() + "Z",
            "status": status,
            "boxscore_raw": boxscore.model_dump(),
            "players": players_dict,
        }
        updated_count += 1

    if updated_count > 0:
        save_data_to_cache(game_stats_cache_on_disk, constants.GAME_STATS_CACHE)
        print(f"  ✅ Saved {updated_count} games to on-disk cache.")
    else:
        print("  - On-disk cache is already up-to-date.")

    # --- PHASE 5: Write all data (cached + fresh) to DB ---
    # This ensures the DB is fully in sync, even with cached games
    write_stats_to_db(game_cache_internal, boxscore_map)
