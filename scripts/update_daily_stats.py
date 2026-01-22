"""
update_daily_stats.py (Refactored)

Fetches and updates player stats for yesterday's games only.
Run this script daily (via cron) to keep the database current.

This script now uses the PlayerStatsProcessor class.
"""

import src.core.constants as constants
import pytz
import time
import asyncio
from datetime import datetime, timedelta
from src.database.database import init_db

# --- 1. IMPORT THE CLASS, NOT THE FUNCTION ---
from src.api.player_stats_fetcher import PlayerStatsProcessor
from src.api.nhl_api_utils import get_schedule
from src.utils.date_utils import get_schedule_by_date


async def main():
    """
    Finds games played yesterday and processes them,
    using the cache to skip games already processed.
    """
    start_time = time.perf_counter()

    # Initialize database
    init_db()

    # --- Calculate yesterday's date ---
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    yesterday = datetime.now(tz) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"UPDATING STATS FOR: {yesterday_str}")
    print("=" * 60)

    # --- Get yesterday's games from schedule ---
    schedule_by_id = await get_schedule()
    schedule_by_date = get_schedule_by_date(schedule_by_id)

    yesterdays_games = schedule_by_date.get(yesterday_str, [])

    if not yesterdays_games:
        print(f"No games found for {yesterday_str}. Exiting.")
        return

    print(f"Found {len(yesterdays_games)} game(s) to process:")
    yesterdays_game_ids = []
    for game in yesterdays_games:
        print(
            f"  - Game {game['game_id']}: {game['away_abbrev']} @ {game['home_abbrev']}"
        )
        yesterdays_game_ids.append(int(game["game_id"]))

    # --- 2. USE THE CLASS ---
    # use_cache=True to skip games already marked 'final'
    if yesterdays_game_ids:
        # Create an instance with cache enabled
        processor = PlayerStatsProcessor(use_cache=True)
        # Call the process_games method on the instance
        await processor.process_games(game_ids_to_process=yesterdays_game_ids)

    # --- Done ---
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print("\n" + "=" * 60)
    print("✅ DAILY UPDATE COMPLETE!")
    print("=" * 60)
    print(f"Date updated: {yesterday_str}")
    print(f"Games processed: {len(yesterdays_games)}")
    print(f"Total execution time: {elapsed:.2f} seconds")
    print(f"Database location: {constants.DATABASE_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
