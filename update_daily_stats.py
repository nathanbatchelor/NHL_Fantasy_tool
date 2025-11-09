"""
update_daily_stats.py
Fetches and updates player stats for yesterday's games only.
Run this script daily (via cron) to keep the database current.
"""

import constants
import pytz
import time
import asyncio
import httpx
from datetime import datetime, timedelta
from sqlmodel import Session
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
from utils.nhl_api_utils import get_schedule
from utils.date_utils import get_schedule_by_date

# --- Concurrency Settings ---
CONCURRENCY_LIMIT = 20  # Lower than seeder since we have fewer games


# --- Helper Functions (same as seeder) ---


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


# --- Async Fetch Functions ---


async def fetch_player_log_for_game(client, semaphore, player_id, game_id):
    """
    Fetch a player's game log to get PP/SH points for a specific game.
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

            # Find the specific game in the log
            for game in log_response.gameLog:
                if game.gameId == game_id:
                    return (player_id, game)

            return None
        except Exception as e:
            print(f"Error (Player Log {player_id}): {e}")
            return None


async def fetch_game_boxscore(client, semaphore, game_id):
    """
    Fetch the full boxscore for a single game.
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

        except Exception as e:
            print(f"Error (Boxscore {game_id}): {e}")
            return None


def write_stats_to_db(game_cache: dict, boxscore_map: dict):
    """
    Write all collected stats to the database using merge (upsert).
    """
    print("\n--- Writing to database...")

    skater_count = 0
    goalie_count = 0

    with Session(engine) as session:
        for game_id, players in game_cache.items():
            boxscore = boxscore_map.get(game_id)
            if not boxscore:
                continue

            for player_id, stats in players.items():
                opponent_abbrev = get_opponent_abbrev(boxscore, stats.teamAbbrev)

                if stats.position in ["G"]:
                    # Goalie
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
                    # Skater
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

        print(f"Committing changes to database...")
        session.commit()

    print(f"✓ Database write complete!")
    print(f"  - Skaters: {skater_count} records")
    print(f"  - Goalies: {goalie_count} records")


async def main():
    """
    Fetches and updates player stats for yesterday's games only.
    """
    start_time = time.perf_counter()

    # Initialize database
    init_db()

    # --- Calculate yesterday's date ---
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    yesterday = datetime.now(tz) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print(f"=" * 60)
    print(f"UPDATING STATS FOR: {yesterday_str}")
    print(f"=" * 60)

    # --- Get yesterday's games from schedule ---
    schedule_by_id = get_schedule()
    schedule_by_date = get_schedule_by_date(schedule_by_id)

    yesterdays_games = schedule_by_date.get(yesterday_str, [])

    if not yesterdays_games:
        print(f"No games found for {yesterday_str}. Exiting.")
        return

    print(f"Found {len(yesterdays_games)} game(s) to process:")
    for game in yesterdays_games:
        print(
            f"  - Game {game['game_id']}: {game['away_abbrev']} @ {game['home_abbrev']}"
        )

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    game_cache = defaultdict(dict)  # {game_id: {player_id: FinalPlayerGameStats}}
    boxscore_map = {}

    async with httpx.AsyncClient() as client:

        # --- PHASE 1: Fetch boxscores for yesterday's games ---
        print(f"\n--- Phase 1: Fetching {len(yesterdays_games)} boxscores...")

        boxscore_tasks = []
        for game in yesterdays_games:
            task = fetch_game_boxscore(client, semaphore, game["game_id"])
            boxscore_tasks.append(task)

        boxscore_results = await asyncio.gather(*boxscore_tasks)

        # Process boxscores and collect all player IDs
        all_player_ids = set()
        for boxscore in boxscore_results:
            if boxscore is None:
                continue

            game_id = boxscore.id
            boxscore_map[game_id] = boxscore

            # Collect all players from this game
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
                all_player_ids.add((player_stats.playerId, game_id))

        phase1_time = time.perf_counter()
        print(
            f"Phase 1 complete. Found {len(all_player_ids)} player-game records. ({phase1_time - start_time:.2f}s)"
        )

        # --- PHASE 2: Fetch player logs for all players ---
        print(
            f"\n--- Phase 2: Fetching player logs for {len(all_player_ids)} players..."
        )

        player_log_tasks = []
        for player_id, game_id in all_player_ids:
            task = fetch_player_log_for_game(client, semaphore, player_id, game_id)
            player_log_tasks.append(task)

        player_log_results = await asyncio.gather(*player_log_tasks)

        # Build the game cache with PP/SH data
        for result in player_log_results:
            if result is None:
                continue

            player_id, game_entry = result
            game_id = game_entry.gameId

            # Create initial cache entry with log data
            game_cache[game_id][player_id] = FinalPlayerGameStats(
                playerId=player_id,
                gameId=game_id,
                teamAbbrev=game_entry.teamAbbrev,
                gameDate=game_entry.gameDate,
                powerPlayPoints=game_entry.powerPlayPoints,
                shorthandedPoints=game_entry.shorthandedPoints,
                toi=game_entry.toi,
                shifts=game_entry.shifts,
                pim=game_entry.pim,
            )

        phase2_time = time.perf_counter()
        print(f"Phase 2 complete. Populated cache. ({phase2_time - phase1_time:.2f}s)")

        # --- PHASE 3: Merge boxscore data into cache ---
        print("\n--- Phase 3: Merging boxscore data...")

        merge_count = 0
        for boxscore in boxscore_results:
            if boxscore is None:
                continue

            game_id = boxscore.id

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

                if player_id in game_cache[game_id]:
                    entry_to_update = game_cache[game_id][player_id]

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

    # --- PHASE 4: Write to Database ---
    write_stats_to_db(game_cache, boxscore_map)

    # --- Done ---
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\n" + "=" * 60)
    print("✓ DAILY UPDATE COMPLETE!")
    print("=" * 60)
    print(f"Date updated: {yesterday_str}")
    print(f"Games processed: {len(yesterdays_games)}")
    print(f"Total execution time: {elapsed:.2f} seconds")
    print(f"Database location: {constants.DATABASE_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
