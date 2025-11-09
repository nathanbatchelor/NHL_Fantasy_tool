import constants
import pandas as pd
import numpy as np
import sys
from pathlib import Path
import time
import asyncio
import httpx
from sqlmodel import Session, select
from database import engine, init_db
from models.database import PlayerGameStats, GoalieGameStats
from models.api.stats import (
    PlayerGameLogResponse,
    GameBoxscoreResponse,
    PlayerStatsFromBoxscore,
    GoalieStatsFromBoxscore,
    FinalPlayerGameStats,
)
from collections import defaultdict
from typing import List, Union

# --- Concurrency Settings ---
CONCURRENCY_LIMIT = 50

# --- Global Cache ---
# Temporary cache to hold combined stats before writing to DB
game_cache = defaultdict(dict)


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


async def fetch_game_boxscore(client, semaphore, game_id):
    """
    PHASE 2: Fetches the full boxscore for a single game.
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


def write_stats_to_db(game_cache: dict, boxscore_map: dict):
    """
    PHASE 3: Write all collected stats to the database.
    Uses merge to handle upserts (insert or update).
    """
    print("\n--- Phase 3: Writing to database...")

    skater_count = 0
    goalie_count = 0

    with Session(engine) as session:
        for game_id, players in game_cache.items():
            # Get the boxscore to determine opponent
            boxscore = boxscore_map.get(game_id)
            if not boxscore:
                continue

            for player_id, stats in players.items():
                opponent_abbrev = get_opponent_abbrev(boxscore, stats.teamAbbrev)

                # Determine if this is a skater or goalie
                if stats.position in ["G"]:
                    # Goalie - use merge for upsert
                    goalie_record = GoalieGameStats(
                        game_id=stats.gameId,
                        player_id=stats.playerId,
                        game_date=stats.gameDate,
                        team_abbrev=stats.teamAbbrev,
                        opponent_abbrev=opponent_abbrev,
                        player_name=stats.name,
                        saves=stats.saves or 0,
                        save_pct=stats.savePct or 0.0,
                        goals_against=stats.goalsAgainst or 0,
                        decision=stats.decision,
                        wins=1 if stats.decision == "W" else 0,
                        shutouts=(
                            1
                            if (
                                stats.goalsAgainst == 0
                                and stats.saves
                                and stats.saves > 0
                            )
                            else 0
                        ),
                        ot_losses=1 if stats.decision == "O" else 0,
                        total_fpts=calculate_fantasy_points_goalie(stats),
                    )
                    session.merge(goalie_record)
                    goalie_count += 1
                else:
                    # Skater - use merge for upsert
                    shooting_pct = None
                    if stats.sog > 0:
                        shooting_pct = round((stats.goals / stats.sog) * 100, 2)

                    skater_record = PlayerGameStats(
                        game_id=stats.gameId,
                        player_id=stats.playerId,
                        game_date=stats.gameDate,
                        team_abbrev=stats.teamAbbrev,
                        opponent_abbrev=opponent_abbrev,
                        player_name=stats.name,
                        goals=stats.goals,
                        assists=stats.assists,
                        pp_points=float(stats.powerPlayPoints),
                        sh_points=float(stats.shorthandedPoints),
                        shots=stats.sog,
                        shooting_pct=shooting_pct,
                        blocked_shots=stats.blockedShots,
                        hits=stats.hits,
                        total_fpts=calculate_fantasy_points_skater(stats),
                    )
                    session.merge(skater_record)
                    skater_count += 1

        # Commit all changes at once
        print(f"Committing changes to database...")
        session.commit()

    print(f"✓ Database write complete!")
    print(f"  - Skaters: {skater_count} records")
    print(f"  - Goalies: {goalie_count} records")


async def main():
    """
    Fetches complete player game stats for the current NHL season.
    Phase 1: Get game logs for all players
    Phase 2: Get boxscores for all games
    Phase 3: Write combined data to database
    """
    start_time = time.perf_counter()

    # Initialize database
    init_db()

    # Load both skater and goalie IDs
    skater_csv_path = Path(constants.SKATER_STATS_CSV)
    goalie_csv_path = Path(constants.GOALIE_STATS_CSV)

    player_ids = []

    if skater_csv_path.exists():
        skater_stats = pd.read_csv(constants.SKATER_STATS_CSV)
        skater_ids = skater_stats["playerId"].dropna().unique().tolist()
        player_ids.extend(skater_ids)
        print(f"Found {len(skater_ids)} unique skater ids.")
    else:
        print(f"Warning: Skater CSV not found: {skater_csv_path}")

    if goalie_csv_path.exists():
        goalie_stats = pd.read_csv(constants.GOALIE_STATS_CSV)
        goalie_ids = goalie_stats["playerId"].dropna().unique().tolist()
        player_ids.extend(goalie_ids)
        print(f"Found {len(goalie_ids)} unique goalie ids.")
    else:
        print(f"Warning: Goalie CSV not found: {goalie_csv_path}")

    if not player_ids:
        print("Error: No player IDs found. Exiting.")
        return

    print(f"Total unique player ids: {len(player_ids)}")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    boxscore_map = {}  # Store boxscores for Phase 3

    async with httpx.AsyncClient() as client:

        # --- PHASE 1: Get Player Logs ---
        print(f"\n--- Phase 1: Fetching {len(player_ids)} player game logs...")
        player_tasks = []
        for player_id in player_ids:
            task = fetch_player_log(client, semaphore, int(player_id))
            player_tasks.append(task)

        player_log_results = await asyncio.gather(*player_tasks)

        # Populate Cache (Phase 1 Data)
        print("Phase 1 complete. Populating initial cache...")
        for result in player_log_results:
            if result is None:
                continue

            player_id, log_response = result
            for game in log_response.gameLog:
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
            f"Found {len(game_cache)} unique games. (Phase 1: {phase1_time - start_time:.2f}s)"
        )

        # --- PHASE 2: Get Boxscores ---
        game_ids_to_fetch = list(game_cache.keys())
        print(f"\n--- Phase 2: Fetching {len(game_ids_to_fetch)} game boxscores...")

        boxscore_tasks = []
        for game_id in game_ids_to_fetch:
            task = fetch_game_boxscore(client, semaphore, game_id)
            boxscore_tasks.append(task)

        boxscore_results = await asyncio.gather(*boxscore_tasks)

        # Merge Boxscore Data
        print("Phase 2 complete. Merging boxscore data into cache...")
        merge_count = 0
        for boxscore in boxscore_results:
            if boxscore is None:
                continue

            game_id = boxscore.id
            boxscore_map[game_id] = boxscore  # Save for Phase 3

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
                    entry_to_update = game_cache[game_id][player_id]

                    if isinstance(player_stats, PlayerStatsFromBoxscore):
                        merge_skater_stats(entry_to_update, player_stats)
                        merge_count += 1
                    elif isinstance(player_stats, GoalieStatsFromBoxscore):
                        merge_goalie_stats(entry_to_update, player_stats)
                        merge_count += 1

        phase2_time = time.perf_counter()
        print(
            f"Merged {merge_count} player-game entries. (Phase 2: {phase2_time - phase1_time:.2f}s)"
        )

    # --- PHASE 3: Write to Database ---
    write_stats_to_db(game_cache, boxscore_map)

    # --- Done ---
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\n" + "=" * 50)
    print("✓ ALL PHASES COMPLETE!")
    print("=" * 50)
    print(f"Total games processed: {len(game_cache)}")
    print(f"Total execution time: {elapsed:.2f} seconds")
    print(
        f"Database location: {constants.DATABASE_FILE if hasattr(constants, 'DATABASE_FILE') else 'data/nhl_stats.db'}"
    )


if __name__ == "__main__":
    asyncio.run(main())
