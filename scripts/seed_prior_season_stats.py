"""
seed_prior_season_stats.py

One-time script to fetch stats from the PRIOR season (2024-2025)
and populate the 'reputation' columns in the ProPlayers table.

This data is crucial for the ML model to have a baseline for players
before the current season starts.
"""

import asyncio
import time
from sqlmodel import Session, select, text
from src.database.database import engine, init_db
from src.database.models import ProPlayers
from src.api.player_stats_fetcher import PlayerStatsProcessor
from src.api.nhl_api_utils import get_schedule_for_season

# Hardcoded prior season ID
PRIOR_SEASON_ID = "20242025"


def calculate_prior_season_averages(session: Session):
    """
    Scans the game stats tables for the PRIOR season and updates
    the ProPlayers table with averages.
    """
    print(f"\nCalculating averages for season {PRIOR_SEASON_ID}...")

    skater_count = 0
    goalie_count = 0

    # --- Optimized Approach ---
    # Calculate all averages in one go using SQL

    # Skater Averages
    sql_skaters = text(
        f"""
        SELECT 
            player_id, 
            COUNT(game_id) as games_played, 
            AVG(total_fpts) as avg_fpts
        FROM player_game_stats 
        WHERE season = '{PRIOR_SEASON_ID}'
        GROUP BY player_id
    """
    )
    results_skaters = session.exec(sql_skaters).all()

    for row in results_skaters:
        player = session.get(ProPlayers, row.player_id)
        if player:
            player.prior_season_games_played = row.games_played
            player.prior_season_avg_fpts = row.avg_fpts
            session.add(player)
            skater_count += 1

    # Goalie Averages
    sql_goalies = text(
        f"""
        SELECT 
            player_id, 
            COUNT(game_id) as games_played, 
            AVG(total_fpts) as avg_fpts
        FROM goalie_game_stats 
        WHERE season = '{PRIOR_SEASON_ID}'
        GROUP BY player_id
    """
    )
    results_goalies = session.exec(sql_goalies).all()

    for row in results_goalies:
        player = session.get(ProPlayers, row.player_id)
        if player:
            player.prior_season_games_played = row.games_played
            player.prior_season_avg_fpts = row.avg_fpts
            session.add(player)
            goalie_count += 1

    session.commit()
    print(
        f"  - Updated {skater_count} skaters and {goalie_count} goalies with prior season stats."
    )


async def main():
    start_time = time.perf_counter()
    init_db()

    print("=" * 60)
    print(f"SEEDING PRIOR SEASON STATS ({PRIOR_SEASON_ID})")
    print("=" * 60)

    # 1. Fetch the schedule for the prior season
    print("\nStep 1: Fetching Prior Season Schedule...")
    # We use the imported helper to get the schedule specifically for last season
    schedule = await get_schedule_for_season(PRIOR_SEASON_ID)

    if not schedule:
        print("Error: Could not fetch prior season schedule.")
        return

    all_game_ids = [int(g["game_id"]) for g in schedule.values()]
    print(f"Found {len(all_game_ids)} games for {PRIOR_SEASON_ID}.")

    # 2. Fetch Game Data (using PlayerStatsProcessor)
    # We temporarily repurpose the processor.
    # IMPORTANT: We do NOT want to run incremental updates or cache logic
    # that might mess up our current season data. We just want to populate
    # the game_stats tables.

    print("\nStep 2: Fetching Game Stats...")
    # We create a processor but we need to be careful about the season ID.
    # The processor uses constants.SEASON_ID globally.
    # This is a limitation of the current design.

    # HACK: Temporarily patch the constant for this script execution.
    # This is safe because we are in a standalone script process.
    import src.core.constants as constants

    original_season = constants.SEASON_ID
    constants.SEASON_ID = PRIOR_SEASON_ID

    # Also need to ensure we don't overwrite the current season's cache file.
    # The processor uses constants.GAME_STATS_CACHE. We should change that too.
    constants.GAME_STATS_CACHE = f"data/game_stats_cache_{PRIOR_SEASON_ID}.json"

    try:
        # Process in chunks to avoid memory issues/timeouts
        chunk_size = 100
        total_batches = (len(all_game_ids) // chunk_size) + 1

        for i in range(0, len(all_game_ids), chunk_size):
            chunk = all_game_ids[i : i + chunk_size]
            print(f"  Processing batch {i//chunk_size + 1}/{total_batches}...")

            # --- OPTIMIZATION: New processor instance per batch ---
            # This ensures we only hold 100 games in memory at a time
            # and don't re-write the previous 100 games to DB.
            processor = PlayerStatsProcessor(
                use_cache=True, perform_incremental_update=False
            )
            await processor.process_games(chunk)

    finally:
        # Restore constants just in case (though script exit will reset them)
        constants.SEASON_ID = original_season

    # 3. Calculate Averages
    print("\nStep 3: Calculating and Saving Averages...")
    with Session(engine) as session:
        calculate_prior_season_averages(session)

    end_time = time.perf_counter()
    print(f"\n✅ Done! Total time: {end_time - start_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
