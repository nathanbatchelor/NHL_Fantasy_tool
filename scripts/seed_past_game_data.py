"""
seed_past_game_data_async.py (Refactored)

This script seeds the database with stats for *all* games
from the current season that have already been played.

This script now uses the PlayerStatsProcessor class.
"""

import asyncio
import time
from src.core.constants import FANTASY_TIMEZONE, DATABASE_FILE, SEASON_ID
import pytz
from datetime import datetime
from src.database.database import init_db

# --- 1. IMPORT THE CLASS, NOT THE FUNCTION ---
from src.api.player_stats_fetcher import (
    PlayerStatsProcessor,
)
from src.api.nhl_api_utils import get_schedule  # Import the schedule fetcher

# --- 2. IMPORT THE REBUILD FUNCTION ---
from scripts.seed_pro_players import populate_pro_players


async def main():
    """
    Fetches the full season schedule, filters for *past* games,
    and processes them, disabling the cache to force a fresh pull.
    """
    start_time = time.perf_counter()

    # Initialize database
    init_db()

    print("--- Seeding all past game data ---")

    # --- Get Today's Date ---
    # We use this to filter out future games
    tz = pytz.timezone(FANTASY_TIMEZONE)
    today_local_date = datetime.now(tz).date()
    print(f"Today's date ({FANTASY_TIMEZONE}): {today_local_date}")

    # --- Get all game IDs for the season ---
    print("Fetching full season schedule to get all game IDs...")
    # We force_refresh=True here to ensure we get *all* games
    schedule_by_id = await get_schedule(force_refresh=True)
    if not schedule_by_id:
        print("Error: No schedule data returned. Exiting.")
        return

    # --- Filter for *past* games only ---

    # --- MODIFICATION: Store date with game_id to allow sorting ---
    past_games_to_sort = []
    future_game_count = 0
    total_game_count = len(schedule_by_id)

    for game_id_str, game_data in schedule_by_id.items():
        try:
            # game_data["date"] is a UTC string like "2025-11-09T00:00:00Z"
            game_utc_time = datetime.fromisoformat(
                game_data["date"].replace("Z", "+00:00")
            )
            # Convert game time to the fantasy timezone to get its "local" date
            game_local_date = game_utc_time.astimezone(tz).date()

            if game_local_date < today_local_date:
                # Store as a tuple (date, game_id)
                past_games_to_sort.append((game_local_date, int(game_id_str)))
            else:
                future_game_count += 1
        except Exception as e:
            print(
                f"Warning: Could not parse date for game {game_id_str}. Skipping. Error: {e}"
            )

    # --- NEW: Sort the past games by date (chronologically) ---
    past_games_to_sort.sort(key=lambda x: x[0])

    # Now create the final list of IDs in the correct order
    past_game_ids = [game_id for game_date, game_id in past_games_to_sort]
    # --- END MODIFICATION ---

    print(f"Found {total_game_count} total games for season {SEASON_ID}.")
    print(f"  - {len(past_game_ids)} games are in the past (will be processed).")
    print(
        f"  - {future_game_count} games are today or in the future (will be skipped)."
    )

    # Call the core processor with the *filtered and sorted* list
    # use_cache=False to force re-fetch and re-build the cache
    if past_game_ids:
        # --- 3. PASS perform_incremental_update=False ---
        # Create an instance with cache disabled and incremental updates OFF
        processor = PlayerStatsProcessor(
            use_cache=False, perform_incremental_update=False
        )
        # Call the process_games method on the instance
        await processor.process_games(game_ids_to_process=past_game_ids)
    else:
        print("No past games found to process. Exiting.")

    # --- 4. RUN THE FULL REBUILD ---
    # Now that all stats are in the DB, run the full rebuild
    # to get correct, non-duplicated season totals.
    print("\n" + "=" * 50)
    print("Running full ProPlayers table rebuild...")
    populate_pro_players()
    print("ProPlayers table rebuild complete.")

    # --- Done ---
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\n" + "=" * 50)
    print("✅ SEEDING COMPLETE!")
    print("=" * 50)
    print(f"Total games processed: {len(past_game_ids)}")
    print(f"Total execution time: {elapsed:.2f} seconds")
    print(
        f"Database location: {DATABASE_FILE if DATABASE_FILE else 'data/nhl_stats.db'}"
    )


if __name__ == "__main__":
    asyncio.run(main())
