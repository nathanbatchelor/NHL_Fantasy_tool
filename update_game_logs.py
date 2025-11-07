"""
update_game_logs.py - The "Daily" Script

This script should be run once per day (e.g., every morning).
It fetches all games from YESTERDAY, processes them concurrently,
and adds them to the player_game_stats and goalie_game_stats tables.

Usage:
  python update_game_logs.py
  (To force a re-run, ignoring cache and existing DB entries):
  python update_game_logs.py --force
"""

import pytz
import sys  # <-- IMPORT SYS
from datetime import datetime, timedelta
import time

# SQLModel imports for modern querying
from sqlmodel import Session, select
from sqlalchemy import func

# --- NEW IMPORTS ---
from concurrent.futures import ThreadPoolExecutor, as_completed
from models.database import PlayerGameStats, GoalieGameStats

# -------------------

from database import engine
import constants

# Import all the necessary helpers from your new utils/ files
from utils.date_utils import get_schedule_by_date

# --- MODIFIED IMPORTS ---
# We now use the thread-safe 'fetch_and_parse_game_data'
# We no longer use the sequential 'process_game'
from utils.nhl_api_utils import (
    get_schedule,
    fetch_and_parse_game_data,  # <-- Use this
    save_player_log_cache,
)

# ------------------------

# This is the safe number you found
OPTIMAL_WORKERS = 20


# <-- MODIFIED FUNCTION SIGNATURE
def update_yesterdays_games(force: bool = False):
    """
    Main function. Fetches only games from yesterday using threads.
    """
    print("=" * 60)
    print("NHL DAILY GAME LOG UPDATER (THREADED)")
    print("=" * 60)

    # <-- ADDED FORCE CHECK
    if force:
        print("   *** FORCE RUN ENABLED ***")
        print("   Will re-fetch all games, ignoring cache and existing DB data.")

    # 1. Get "yesterday" in the fantasy timezone
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    yesterday = datetime.now(tz) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print(f"\n1. Target Date: {yesterday_str}")

    # 2. Load schedule (from utils)
    print("\n2. Loading season schedule...")
    # <-- MODIFIED CALL TO USE force
    schedule_by_id = get_schedule(force_refresh=force)  # From nhl_api_utils
    if not schedule_by_id:
        print("Error: Could not load schedule.")
        return
    print(f"   ✓ Loaded {len(schedule_by_id)} games from schedule")

    # 3. Re-index by date (from utils)
    schedule_by_date = get_schedule_by_date(schedule_by_id)  # From date_utils

    # 4. Find games from yesterday
    print(f"\n3. Finding games for {yesterday_str}...")
    games_to_fetch = schedule_by_date.get(yesterday_str)

    if not games_to_fetch:
        print("   ✓ No games were played yesterday. All done!")
        return

    num_games = len(games_to_fetch)
    print(f"   ✓ Found {num_games} games to process")

    # 5. Process games (CONCURRENTLY)
    print(f"\n4. Submitting all games to thread pool ({OPTIMAL_WORKERS} workers)...")
    print("-" * 60)

    # Master lists to hold all returned objects
    all_skaters_to_add: list[PlayerGameStats] = []
    all_goalies_to_add: list[GoalieGameStats] = []

    futures_to_game = {}
    games_processed_count = 0
    total_skaters = 0
    total_goalies = 0

    start_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=OPTIMAL_WORKERS) as executor:
        # Submit all jobs
        for game in games_to_fetch:
            future = executor.submit(fetch_and_parse_game_data, game)  #
            futures_to_game[future] = game

        print(f"   ✓ All {num_games} games submitted. Waiting for results...")

        # Process results as they complete
        for i, future in enumerate(as_completed(futures_to_game), 1):
            game = futures_to_game[future]
            game_id = game["game_id"]

            try:
                skater_list, goalie_list = future.result()

                if skater_list or goalie_list:
                    all_skaters_to_add.extend(skater_list)
                    all_goalies_to_add.extend(goalie_list)
                    games_processed_count += 1
                    total_skaters += len(skater_list)
                    total_goalies += len(goalie_list)

                print(
                    f"   ... Progress: {i}/{num_games} jobs complete (Game {game_id})"
                )

            except Exception as e:
                print(f"   ✗ CRITICAL ERROR processing game {game_id}: {e}")

    end_fetch_time = time.time()
    print(f"   ✓ API requests complete in {end_fetch_time - start_fetch_time:.2f}s")

    # 6. Check for existing games AND save to DB
    print("\n5. Checking database and saving new data...")

    with Session(engine) as session:

        # <-- MODIFIED LOGIC FOR FORCE CHECK
        final_skaters_to_add = all_skaters_to_add
        final_goalies_to_add = all_goalies_to_add

        if not force:
            # Standard run: Only add games that are not already in the DB
            existing_game_ids = set(
                session.exec(select(PlayerGameStats.game_id).distinct()).all()
            )
            print(f"   ✓ Found {len(existing_game_ids)} games already in DB")

            # Filter out players/goalies from games we already have
            final_skaters_to_add = [
                s for s in all_skaters_to_add if s.game_id not in existing_game_ids
            ]
            final_goalies_to_add = [
                g for g in all_goalies_to_add if g.game_id not in existing_game_ids
            ]
        else:
            # Force run: We will merge all fetched games
            print(
                "   ! FORCE RUN: Will merge all fetched games, overwriting existing entries."
            )
        # <-- END MODIFIED LOGIC

        new_games_processed = len(set(s.game_id for s in final_skaters_to_add))

        if not final_skaters_to_add and not final_goalies_to_add:
            if not force:
                print(
                    "   ✓ All games processed were already in the database. All done!"
                )
            else:
                print("   ✓ No games found to process.")
            # Still save the cache in case new player logs were fetched
            save_player_log_cache()
            return

        print(
            f"   ✓ Adding/Merging {len(final_skaters_to_add)} new skaters and {len(final_goalies_to_add)} new goalies."
        )

        # Use merge() - this is safe for both new and forced-overwrite
        for skater in final_skaters_to_add:
            session.merge(skater)
        for goalie in final_goalies_to_add:
            session.merge(goalie)

        print("   Committing transaction...")
        start_commit = time.time()
        session.commit()
        end_commit = time.time()
        print(f"   ✓ Commit complete in {end_commit - start_commit:.2f}s")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"New games processed/merged: {new_games_processed}")
    print(f"Total skaters saved/merged: {len(final_skaters_to_add)}")
    print(f"Total goalies saved/merged: {len(final_goalies_to_add)}")
    save_player_log_cache()
    print(f"\n✓ Daily update complete!")


if __name__ == "__main__":
    # NOTE: We DO NOT run init_db() here.
    # This script assumes the database has already been created
    # by seed/seed_past_game_data.py

    # <-- MODIFIED MAIN EXECUTION BLOCK
    # Check for --force argument
    force_run = "--force" in sys.argv

    start_time = time.time()
    update_yesterdays_games(force=force_run)  # <-- PASS THE FLAG
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
